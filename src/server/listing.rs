//! ListObjectsV2 merged across all upstreams in key order.
//!
//! Each upstream is read through a cursor. A cursor can only be advanced by a request the client
//! signed over that upstream's continuation token, or, before its first page, by the client's
//! initial request, which carries no token. Every page therefore hands out the token of a cursor
//! that still has keys, and the client's next request advances all cursors waiting on it. The merge
//! stops early when such a cursor runs dry, as its next key could precede everything buffered.
//! Only once every upstream is exhausted, the remaining buffered entries get tokens minted by
//! parkhaus.
//!
//! Upstream tokens only encode a position, so different listings can receive the same one. Tokens
//! are therefore looked up together with the listing's parameters, and each listing handing out a
//! token can be resumed with it once. Listings agreeing on both are in the same state at equal
//! tokens, so it does not matter which of them a client resumes.

use crate::config::UpstreamId;
use crate::error::TierError;
use crate::server::endpoints;
use crate::server::state::AppState;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::Request;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Method};
use axum::response::{IntoResponse, Response};
use futures_util::future::try_join_all;
use jiff::{SignedDuration, Timestamp};
use percent_encoding::percent_decode_str;
use quick_xml::Reader;
use quick_xml::escape::{escape, unescape};
use quick_xml::events::Event;
use reqwest::StatusCode;
use rootcause::prelude::ResultExt;
use rootcause::{Report, bail, report};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use url::form_urlencoded;

/// Idle time after which a paused listing is forgotten. Every page fetched refreshes it.
const LISTING_TTL: SignedDuration = SignedDuration::from_hours(1);
const MAX_PAGE_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_KEYS: usize = 1000;

/// The request parameters determining a listing's content, as the client sent them.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
struct ListParams {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    start_after: Option<String>,
    max_keys: Option<String>,
    encoding_type: Option<String>,
    fetch_owner: Option<String>,
    optional_attributes: Option<String>,
}

impl ListParams {
    fn new(bucket: &str, req: &Request) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: query_param(req, "prefix"),
            delimiter: query_param(req, "delimiter"),
            start_after: query_param(req, "start-after"),
            max_keys: query_param(req, "max-keys"),
            encoding_type: query_param(req, "encoding-type"),
            fetch_owner: query_param(req, "fetch-owner"),
            optional_attributes: req
                .headers()
                .get("x-amz-optional-object-attributes")
                .and_then(|it| it.to_str().ok())
                .map(str::to_string),
        }
    }

    fn max_keys(&self) -> usize {
        self.max_keys
            .as_deref()
            .and_then(|it| it.parse().ok())
            .unwrap_or(DEFAULT_MAX_KEYS)
            .clamp(1, DEFAULT_MAX_KEYS)
    }

    fn url_encoded(&self) -> bool {
        self.encoding_type.as_deref() == Some("url")
    }
}

#[derive(Clone, Default)]
pub struct ListingStore {
    /// The listings that handed out a continuation token and were not resumed with it yet.
    paused: Arc<Mutex<HashMap<TokenKey, Vec<Paused>>>>,
}

impl ListingStore {
    fn claim(&self, key: &TokenKey) -> Option<Listing> {
        let mut paused = self.lock_pruned();
        let listings = paused.get_mut(key)?;
        let claimed = listings.pop()?;
        if listings.is_empty() {
            paused.remove(key);
        }
        Some(claimed.listing)
    }

    fn register(&self, token: String, listing: Listing) {
        let key = TokenKey {
            params: listing.params.clone(),
            token,
        };
        self.lock_pruned().entry(key).or_default().push(Paused {
            since: Timestamp::now(),
            listing,
        });
    }

    fn lock_pruned(&self) -> MutexGuard<'_, HashMap<TokenKey, Vec<Paused>>> {
        let mut paused = self.paused.lock().unwrap();
        let now = Timestamp::now();
        paused.retain(|_, listings| {
            listings.retain(|it| now.duration_since(it.since) < LISTING_TTL);
            !listings.is_empty()
        });
        paused
    }
}

#[derive(PartialEq, Eq, Hash)]
struct TokenKey {
    params: ListParams,
    token: String,
}

struct Paused {
    since: Timestamp,
    listing: Listing,
}

struct Listing {
    params: ListParams,
    /// Elements of the first page describing the request (`Name`, `Prefix`, `MaxKeys`, ...).
    preamble: Vec<u8>,
    cursors: Vec<Cursor>,
    /// Entries present on several upstreams are only listed once.
    last_emitted: Option<String>,
}

impl Listing {
    fn new(params: ListParams, pages: Vec<(UpstreamId, UpstreamPage)>) -> Self {
        let mut preamble = None;
        let cursors = pages
            .into_iter()
            .map(|(upstream, page)| {
                preamble.get_or_insert(page.preamble);
                Cursor {
                    upstream,
                    buffered: page.entries.into(),
                    next_token: page.next_token,
                }
            })
            .collect();
        Self {
            params,
            preamble: preamble.unwrap_or_default(),
            cursors,
            last_emitted: None,
        }
    }

    /// Several cursors wait on the same token if their upstreams continue at the same key.
    fn waiting_on<'a>(&'a self, token: &'a str) -> impl Iterator<Item = &'a UpstreamId> {
        self.cursors
            .iter()
            .filter(move |it| it.next_token.as_deref() == Some(token))
            .map(|it| &it.upstream)
    }

    fn refill(&mut self, pages: Vec<(UpstreamId, UpstreamPage)>) {
        for (upstream, page) in pages {
            if let Some(cursor) = self.cursors.iter_mut().find(|it| it.upstream == upstream) {
                cursor.refill(page);
            }
        }
    }

    /// The next entries in key order and how the listing continues after them.
    fn next_page(&mut self) -> (Vec<Bytes>, Continuation) {
        let mut out = Vec::new();
        loop {
            // The next key of a dry cursor could precede everything that is buffered
            if let Some(token) = self
                .cursors
                .iter()
                .filter(|it| it.buffered.is_empty())
                .find_map(|it| it.next_token.clone())
            {
                return (out, Continuation::Upstream(token));
            }

            let Some(cursor) = self
                .cursors
                .iter_mut()
                .filter(|it| !it.buffered.is_empty())
                .min_by(|a, b| a.buffered[0].sort_key.cmp(&b.buffered[0].sort_key))
            else {
                return (out, Continuation::Done);
            };
            if out.len() == self.params.max_keys() {
                let next = self.upstream_token();
                return (
                    out,
                    next.map_or(Continuation::Buffered, Continuation::Upstream),
                );
            }

            let entry = cursor.buffered.pop_front().unwrap();
            if self
                .last_emitted
                .as_ref()
                .is_some_and(|last| entry.sort_key <= *last)
            {
                continue;
            }
            self.last_emitted = Some(entry.sort_key);
            out.push(entry.xml);
        }
    }

    /// The token of the cursor likely to run dry first, so it is refilled before it does.
    fn upstream_token(&self) -> Option<String> {
        self.cursors
            .iter()
            .filter(|it| it.next_token.is_some())
            .min_by_key(|it| it.buffered.len())?
            .next_token
            .clone()
    }
}

enum Continuation {
    Done,
    /// Resumed by a request carrying this upstream token.
    Upstream(String),
    /// Every upstream is exhausted, but entries are still buffered.
    Buffered,
}

struct Cursor {
    upstream: UpstreamId,
    buffered: VecDeque<Entry>,
    /// The token to fetch the upstream's next page with, `None` once it is exhausted.
    next_token: Option<String>,
}

impl Cursor {
    fn refill(&mut self, page: UpstreamPage) {
        self.buffered.extend(page.entries);
        self.next_token = page.next_token;
    }
}

/// A `Contents` or `CommonPrefixes` element, kept as the upstream sent it.
struct Entry {
    sort_key: String,
    xml: Bytes,
}

impl Entry {
    fn new(xml: Bytes, url_encoded: bool) -> Result<Self, Report> {
        let parsed: EntryKey =
            quick_xml::de::from_reader(&xml[..]).context("invalid ListObjectsV2 entry")?;
        let mut sort_key = parsed.key;
        if url_encoded {
            // S3 orders by the key itself, not its encoding
            sort_key = percent_decode_str(&sort_key.replace('+', " "))
                .decode_utf8_lossy()
                .into_owned();
        }
        Ok(Self { sort_key, xml })
    }
}

#[derive(Deserialize)]
struct EntryKey {
    #[serde(rename = "Key", alias = "Prefix")]
    key: String,
}

struct UpstreamPage {
    entries: Vec<Entry>,
    next_token: Option<String>,
    preamble: Vec<u8>,
}

impl UpstreamPage {
    fn parse(body: &Bytes, url_encoded: bool) -> Result<Self, Report> {
        let mut reader = Reader::from_reader(&body[..]);
        let mut page = Self {
            entries: Vec::new(),
            next_token: None,
            preamble: Vec::new(),
        };
        let mut is_truncated = false;
        let mut in_root = false;

        loop {
            let start = usize::try_from(reader.buffer_position())?;
            match reader.read_event().context("invalid ListObjectsV2 XML")? {
                Event::Start(_) if !in_root => in_root = true,
                Event::Start(element) => {
                    let end = element.to_end().into_owned();
                    let text = reader
                        .read_text(end.name())
                        .context("invalid ListObjectsV2 XML")?;
                    let text = unescape(&text)
                        .context("invalid ListObjectsV2 XML")?
                        .into_owned();
                    let xml = body.slice(start..usize::try_from(reader.buffer_position())?);
                    match element.local_name().as_ref() {
                        b"Contents" | b"CommonPrefixes" => {
                            page.entries.push(Entry::new(xml, url_encoded)?);
                        }
                        b"IsTruncated" => is_truncated = text == "true",
                        b"NextContinuationToken" => page.next_token = Some(text),
                        b"KeyCount" | b"ContinuationToken" => {}
                        _ => page.preamble.extend_from_slice(&xml),
                    }
                }
                Event::Empty(_) if in_root => {
                    let end = usize::try_from(reader.buffer_position())?;
                    page.preamble.extend_from_slice(&body[start..end]);
                }
                Event::Eof => break,
                _ => {}
            }
        }

        // Pages list all Contents before all CommonPrefixes, each sorted on its own
        page.entries.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        if !is_truncated {
            page.next_token = None;
        } else if page.next_token.is_none() {
            bail!("truncated ListObjectsV2 page without continuation token");
        }
        Ok(page)
    }
}

pub(super) fn is_list_objects_v2(req: &Request) -> bool {
    *req.method() == Method::GET && query_param(req, "list-type").is_some_and(|it| it == "2")
}

pub(super) async fn proxy_list_objects_v2(
    state: &AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, TierError> {
    let listings = &state.listings;
    let params = ListParams::new(bucket, &req);
    let url_encoded = params.url_encoded();
    let token = query_param(&req, "continuation-token");

    let mut listing = match &token {
        None => {
            let upstreams = state.config.upstreams_in_order();
            let names = upstreams.iter().map(|it| &it.name);
            match fetch_pages(state, bucket, names, req, url_encoded).await {
                Ok(pages) => Listing::new(params, pages),
                Err(response) => return Ok(response),
            }
        }
        Some(token) => {
            let key = TokenKey {
                params,
                token: token.clone(),
            };
            let mut listing = listings.claim(&key).ok_or_else(|| {
                report!("unknown, expired or already used continuation token")
                    .attach(StatusCode::BAD_REQUEST)
            })?;
            let waiting = listing.waiting_on(token);
            let pages = fetch_pages(state, bucket, waiting, req, url_encoded).await;
            match pages {
                Ok(pages) => listing.refill(pages),
                Err(response) => {
                    // The client may retry a failed refill
                    listings.register(key.token, listing);
                    return Ok(response);
                }
            }
            listing
        }
    };

    let (entries, continuation) = listing.next_page();
    let next_token = match continuation {
        Continuation::Done => None,
        Continuation::Upstream(token) => Some(token),
        Continuation::Buffered => Some(format!(
            "parkhaus-{}",
            hex::encode(rand::random::<[u8; 16]>())
        )),
    };
    let body = render(
        &listing.preamble,
        &entries,
        token.as_deref(),
        next_token.as_deref(),
    );
    if let Some(next_token) = next_token {
        listings.register(next_token, listing);
    }
    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(response)
}

/// Sends the request to each upstream. All pages are fetched before any is used. Errors are
/// returned as the response for the client, an upstream's error response as is.
async fn fetch_pages(
    state: &AppState,
    bucket: &str,
    upstreams: impl IntoIterator<Item = &UpstreamId>,
    req: Request,
    url_encoded: bool,
) -> Result<Vec<(UpstreamId, UpstreamPage)>, Response> {
    let (parts, _) = req.into_parts();
    try_join_all(upstreams.into_iter().map(|upstream| {
        let req = Request::from_parts(parts.clone(), Body::empty());
        async move {
            match fetch_page(state, bucket, upstream, req, url_encoded).await {
                Ok(Ok(page)) => Ok((upstream.clone(), page)),
                Ok(Err(response)) => Err(response),
                Err(error) => Err(error.into_response()),
            }
        }
    }))
    .await
}

async fn fetch_page(
    state: &AppState,
    bucket: &str,
    upstream: &UpstreamId,
    req: Request,
    url_encoded: bool,
) -> Result<Result<UpstreamPage, Response>, TierError> {
    let upstream = state
        .config
        .upstreams
        .get(upstream)
        .ok_or_else(|| report!("upstream {upstream} of listing no longer configured"))?;
    let mut url = upstream.format_url(bucket, None)?;
    url.url.set_query(req.uri().query());
    let response = endpoints::forward_request(state, upstream, url, req, async {}).await?;
    if !response.status().is_success() {
        return Ok(Err(response));
    }
    let body = to_bytes(response.into_body(), MAX_PAGE_SIZE)
        .await
        .context("failed to read ListObjectsV2 response")?;
    Ok(Ok(
        UpstreamPage::parse(&body, url_encoded).attach(format!("upstream: {}", upstream.name))?
    ))
}

fn query_param(req: &Request, name: &str) -> Option<String> {
    form_urlencoded::parse(req.uri().query()?.as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

fn render(
    preamble: &[u8],
    entries: &[Bytes],
    continuation_token: Option<&str>,
    next_token: Option<&str>,
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(preamble.len() + entries.iter().map(Bytes::len).sum::<usize>() + 512);
    out.extend_from_slice(
        b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
          <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    out.extend_from_slice(preamble);
    out.extend_from_slice(format!("<KeyCount>{}</KeyCount>", entries.len()).as_bytes());
    out.extend_from_slice(
        format!("<IsTruncated>{}</IsTruncated>", next_token.is_some()).as_bytes(),
    );
    if let Some(token) = continuation_token {
        out.extend_from_slice(
            format!("<ContinuationToken>{}</ContinuationToken>", escape(token)).as_bytes(),
        );
    }
    if let Some(token) = next_token {
        out.extend_from_slice(
            format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                escape(token)
            )
            .as_bytes(),
        );
    }
    for entry in entries {
        out.extend_from_slice(entry);
    }
    out.extend_from_slice(b"</ListBucketResult>");
    out
}

#[cfg(test)]
mod tests {
    use super::{
        Continuation, LISTING_TTL, ListParams, Listing, ListingStore, TokenKey, UpstreamPage,
    };
    use crate::config::UpstreamId;
    use axum::body::Bytes;
    use jiff::Timestamp;

    fn page(keys: &[&str], prefixes: &[&str], next_token: Option<&str>) -> UpstreamPage {
        let mut xml = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>b</Name><Prefix/><MaxKeys>2</MaxKeys>",
        );
        xml += &format!("<KeyCount>{}</KeyCount>", keys.len() + prefixes.len());
        xml += &format!("<IsTruncated>{}</IsTruncated>", next_token.is_some());
        if let Some(token) = next_token {
            xml += &format!("<NextContinuationToken>{token}</NextContinuationToken>");
        }
        for key in keys {
            xml += &format!("<Contents><Key>{key}</Key><Size>1</Size></Contents>");
        }
        for prefix in prefixes {
            xml += &format!("<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>");
        }
        xml += "</ListBucketResult>";
        UpstreamPage::parse(&Bytes::from(xml), false).unwrap()
    }

    fn params(bucket: &str, max_keys: usize) -> ListParams {
        ListParams {
            bucket: bucket.to_string(),
            max_keys: Some(max_keys.to_string()),
            ..ListParams::default()
        }
    }

    fn listing_in(bucket: &str, max_keys: usize, pages: Vec<UpstreamPage>) -> Listing {
        let pages = (0..)
            .map(|i| UpstreamId(format!("u{i}")))
            .zip(pages)
            .collect();
        Listing::new(params(bucket, max_keys), pages)
    }

    fn listing(max_keys: usize, pages: Vec<UpstreamPage>) -> Listing {
        listing_in("bucket", max_keys, pages)
    }

    fn keys(entries: &[Bytes]) -> Vec<String> {
        entries
            .iter()
            .map(|it| String::from_utf8(it.to_vec()).unwrap())
            .map(|it| it.split(['>', '<']).nth(4).unwrap().to_string())
            .collect()
    }

    fn token_key(bucket: &str, token: &str) -> TokenKey {
        TokenKey {
            params: params(bucket, 1),
            token: token.to_string(),
        }
    }

    #[test]
    fn claims_listings_once_and_preserves_colliding_tokens() {
        let listings = ListingStore::default();
        for (bucket, key) in [("bucket", "a"), ("bucket", "b"), ("other", "c")] {
            listings.register(
                "token".to_string(),
                listing_in(bucket, 1, vec![page(&[key], &[], None)]),
            );
        }

        for key in ["b", "a"] {
            let mut claimed = listings.claim(&token_key("bucket", "token")).unwrap();
            assert_eq!(keys(&claimed.next_page().0), [key]);
        }
        assert!(listings.claim(&token_key("bucket", "token")).is_none());
        let mut claimed = listings.claim(&token_key("other", "token")).unwrap();
        assert_eq!(keys(&claimed.next_page().0), ["c"]);
    }

    #[test]
    fn expires_idle_listings_without_dropping_active_ones() {
        let listings = ListingStore::default();
        for token in ["expired", "active"] {
            listings.register(token.to_string(), listing(1, Vec::new()));
        }
        listings
            .paused
            .lock()
            .unwrap()
            .get_mut(&token_key("bucket", "expired"))
            .unwrap()[0]
            .since = Timestamp::now() - LISTING_TTL;

        assert!(listings.claim(&token_key("bucket", "expired")).is_none());
        assert!(listings.claim(&token_key("bucket", "active")).is_some());
        assert!(listings.paused.lock().unwrap().is_empty());
    }

    #[test]
    fn parses_page() {
        let page = page(&["a&amp;b"], &["p/"], Some("tok"));
        assert_eq!(page.next_token.as_deref(), Some("tok"));
        assert_eq!(
            String::from_utf8(page.preamble).unwrap(),
            "<Name>b</Name><Prefix/><MaxKeys>2</MaxKeys>"
        );
        let sort_keys: Vec<_> = page.entries.iter().map(|it| it.sort_key.as_str()).collect();
        assert_eq!(sort_keys, ["a&b", "p/"]);
    }

    #[test]
    fn merges_in_key_order_and_stops_at_drained_upstreams() {
        let mut listing = listing(
            10,
            vec![
                page(&["a", "c"], &[], Some("hot-1")),
                page(&["b", "d"], &[], None),
            ],
        );

        let (entries, next) = listing.next_page();
        assert_eq!(keys(&entries), ["a", "b", "c"]);
        assert!(matches!(next, Continuation::Upstream(token) if token == "hot-1"));

        listing.cursors[0].refill(page(&["e"], &[], None));
        let (entries, next) = listing.next_page();
        assert_eq!(keys(&entries), ["d", "e"]);
        assert!(matches!(next, Continuation::Done));
    }

    #[test]
    fn hands_out_token_of_upstream_running_dry_first() {
        let mut listing = listing(
            2,
            vec![
                page(&["a", "b", "c"], &[], Some("hot-1")),
                page(&["d", "e"], &[], Some("cold-1")),
            ],
        );

        let (entries, next) = listing.next_page();
        assert_eq!(keys(&entries), ["a", "b"]);
        assert!(matches!(next, Continuation::Upstream(token) if token == "hot-1"));
    }

    #[test]
    fn refills_all_upstreams_waiting_on_a_token() {
        let listing = listing(
            1,
            vec![
                page(&["a"], &[], Some("same")),
                page(&["a"], &[], Some("same")),
                page(&["b"], &[], Some("other")),
            ],
        );
        let waiting: Vec<_> = listing.waiting_on("same").map(|it| it.0.as_str()).collect();
        assert_eq!(waiting, ["u0", "u1"]);
    }

    #[test]
    fn pages_by_max_keys_and_deduplicates() {
        let mut listing = listing(
            2,
            vec![
                page(&["a", "x"], &["p/"], None),
                page(&["a", "b"], &["p/"], None),
            ],
        );

        let (entries, next) = listing.next_page();
        assert_eq!(keys(&entries), ["a", "b"]);
        assert!(matches!(next, Continuation::Buffered));

        let (entries, next) = listing.next_page();
        assert_eq!(keys(&entries), ["p/", "x"]);
        assert!(matches!(next, Continuation::Done));
    }

    #[test]
    fn rejects_truncated_page_without_token() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>";
        assert!(UpstreamPage::parse(&Bytes::from_static(xml.as_bytes()), false).is_err());
    }
}
