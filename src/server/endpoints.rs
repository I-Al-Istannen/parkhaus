use crate::config::{Config, Upstream};
use crate::data::{S3Object, S3ObjectId, UpstreamId};
use crate::error::TierError;
use crate::s3::client::S3Client;
use crate::s3::types::ForwardObjectUrl;
use crate::server::metrics::{
    COUNTER_OBJECT_CREATIONS_TOTAL, COUNTER_OBJECT_DELETIONS_TOTAL,
    COUNTER_OBJECT_IMPORTED_ON_THE_FLY, COUNTER_UPSTREAM_FALLBACKS_TOTAL,
    COUNTER_UPSTREAM_FORWARDS_TOTAL,
};
use crate::server::state::{AppState, MutationLockGuard};
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use axum::response::Response;
use axum_prometheus::metrics::counter;
use percent_encoding::percent_decode_str;
use reqwest::StatusCode;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use std::collections::HashSet;
use tracing::{debug, warn};
use url::form_urlencoded;

const fn nop() {}

/// How a successful request affects the tracked state of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectOp {
    /// The object was created or replaced, its metadata must be fetched from the upstream.
    Write,
    Delete,
    Access,
    /// Multipart upload bookkeeping that does not (yet) change the object.
    MultipartStep,
}

impl ObjectOp {
    fn classify(method: &Method, query: Option<&str>) -> Self {
        let has_param = |name: &str| {
            query.is_some_and(|q| form_urlencoded::parse(q.as_bytes()).any(|(k, _)| k == name))
        };
        let upload_id = has_param("uploadId");

        match *method {
            Method::PUT if upload_id => Self::MultipartStep,
            Method::PUT => Self::Write,
            // CompleteMultipartUpload
            Method::POST if upload_id => Self::Write,
            Method::POST if has_param("uploads") => Self::MultipartStep,
            Method::DELETE if upload_id => Self::MultipartStep,
            Method::DELETE => Self::Delete,
            Method::GET if upload_id => Self::MultipartStep,
            _ => Self::Access,
        }
    }
}

pub async fn proxy_request(
    State(state): State<AppState>,
    OriginalUri(original_uri): OriginalUri,
    req: Request,
) -> Result<Response, TierError> {
    debug!(%original_uri, method=%req.method(), "received request for URL");

    let path = percent_decode_str(original_uri.path())
        .decode_utf8()
        .context("request path is not valid UTF-8")
        .attach(StatusCode::BAD_REQUEST)?;

    if path.chars().filter(|&it| it == '/').count() == 1 {
        // CreateBucket/DeleteBucket would need to be coordinated across all upstreams
        if req.uri().query().is_none_or(str::is_empty)
            && matches!(*req.method(), Method::PUT | Method::DELETE)
        {
            return Err(report!("creating and deleting buckets is not supported")
                .attach(StatusCode::NOT_IMPLEMENTED)
                .into());
        }

        // bucket-specific operation, nothing for us to track
        let upstream = state.config.hottest_upstream();
        let bucket = path.trim_start_matches('/');
        debug!(%original_uri, method = %req.method(), %bucket, "handling bucket-level request for URL");

        let mut upstream_url = upstream.format_url(bucket, None)?;
        upstream_url.url.set_query(req.uri().query());
        return forward_request(&state, upstream, upstream_url, req, nop).await;
    }

    debug!(foo=%original_uri, method=%req.method(), "handling request for URL");
    let Some((bucket, key)) = path.trim_start_matches('/').split_once('/') else {
        return Err(report!("url misses bucket: '{original_uri}'")
            .attach(StatusCode::BAD_REQUEST)
            .into());
    };

    let object_id = S3ObjectId {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };
    let op = ObjectOp::classify(req.method(), req.uri().query());
    let mutation_guard = state
        .migration_locks
        .wait_for_migration(req.method(), &object_id)
        .await;

    // CopyObject is executed by the upstream holding the source, the destination follows it.
    let copy_source = match op {
        ObjectOp::Write => copy_source(req.headers())?,
        _ => None,
    };
    let routing_id = copy_source.as_ref().unwrap_or(&object_id);

    let upstream = state
        .db
        .get_upstream(routing_id)
        .await
        .context("failed to get upstream for object")?
        .and_then(|it| {
            counter!(COUNTER_UPSTREAM_FORWARDS_TOTAL,
                "upstream" => it.0.clone(),
                "method" => req.method().to_string()
            )
            .increment(1);
            state.config.upstreams.get(&it)
        })
        // default to the coldest upstream
        .unwrap_or_else(|| {
            get_fallback_upstream(routing_id, &state.config, &req, op, copy_source.is_some())
        });

    let stale_upstream = match &copy_source {
        Some(_) => state
            .db
            .get_upstream(&object_id)
            .await
            .context("failed to get upstream for copy destination")?
            .filter(|it| *it != upstream.name),
        None => None,
    };

    let on_success = record_successful_request(
        op,
        object_id.clone(),
        state.clone(),
        upstream.name.clone(),
        stale_upstream,
        mutation_guard,
    );
    let mut target_url = upstream.format_url(&object_id.bucket, Some(&object_id.key))?;
    target_url.url.set_query(req.uri().query());
    forward_request(&state, upstream, target_url, req, on_success).await
}

fn get_fallback_upstream<'a>(
    object_id: &'_ S3ObjectId,
    config: &'a Config,
    request: &'_ Request,
    op: ObjectOp,
    is_copy: bool,
) -> &'a Upstream {
    let is_read = request.method() == Method::GET || request.method() == Method::HEAD;
    let is_delete = request.method() == Method::DELETE;
    let fallback = if (is_copy || is_read || is_delete) && op != ObjectOp::MultipartStep {
        config.coldest_upstream()
    } else {
        config.hottest_upstream()
    };

    counter!(COUNTER_UPSTREAM_FALLBACKS_TOTAL, "method" => request.method().to_string())
        .increment(1);
    debug!(
        ?object_id,
        %fallback.name,
        "object not found in database, defaulting upstream"
    );

    fallback
}

async fn forward_request(
    state: &AppState,
    upstream: &Upstream,
    target: ForwardObjectUrl,
    in_req: Request,
    on_success: impl FnOnce(),
) -> Result<Response, TierError> {
    let mut out_req = state
        .http
        .request(in_req.method().clone(), target.url.clone())
        .header(HeaderName::from_static("x-tiering-proxy"), "parkhaus");

    if let Some(host_header) = &target.host_header {
        out_req = out_req.header(reqwest::header::HOST, host_header);
    }

    let connection_headers = connection_header_names(in_req.headers());
    for (name, val) in in_req.headers() {
        if is_hop_by_hop_header(name) || connection_headers.contains(name) {
            continue;
        }
        out_req = out_req.header(name, val);
    }

    out_req = out_req.body(reqwest::Body::wrap_stream(
        in_req.into_body().into_data_stream(),
    ));

    debug!(
        upstream = %upstream.name,
        %target,
        "forwarding request to upstream"
    );

    let in_response = state
        .http
        .execute(
            out_req
                .build()
                .context("failed to build HTTP request")
                .attach(format!("url: {target}"))?,
        )
        .await
        .context("failed to send request")
        .attach(format!("url: {target}"))?;

    let in_resp_status = in_response.status();
    let in_resp_headers = in_response.headers().clone();
    let mut out_response = Response::new(Body::from_stream(in_response.bytes_stream()));
    *out_response.status_mut() = in_resp_status;
    for (name, value) in &in_resp_headers {
        if is_hop_by_hop_header(name) {
            continue;
        }
        out_response.headers_mut().append(name, value.clone());
    }

    // Add a header to indicate which upstream we forwarded to, for debugging purposes
    match HeaderValue::from_bytes(upstream.name.0.as_bytes()) {
        Ok(val) => {
            out_response.headers_mut().append("x-tiering-upstream", val);
        }
        Err(err) => {
            warn!(%err, "failed to set x-tiering-upstream header");
        }
    }

    if in_resp_status.is_success() {
        on_success();
    }

    Ok(out_response)
}

/// Parses `x-amz-copy-source` (`[/]bucket/key[?versionId=...]`, URL-encoded).
fn copy_source(headers: &HeaderMap) -> Result<Option<S3ObjectId>, TierError> {
    let Some(value) = headers.get("x-amz-copy-source") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .context("invalid x-amz-copy-source header")
        .attach(StatusCode::BAD_REQUEST)?;
    let value = value.split_once('?').map_or(value, |(path, _)| path);
    let decoded = percent_decode_str(value.trim_start_matches('/'))
        .decode_utf8()
        .context("x-amz-copy-source is not valid UTF-8")
        .attach(StatusCode::BAD_REQUEST)?;
    let Some((bucket, key)) = decoded.split_once('/') else {
        return Err(report!("x-amz-copy-source misses key: '{decoded}'")
            .attach(StatusCode::BAD_REQUEST)
            .into());
    };
    Ok(Some(S3ObjectId {
        bucket: bucket.to_string(),
        key: key.to_string(),
    }))
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    // https://datatracker.ietf.org/doc/html/rfc2068#section-13.5.1
    //   - Connection
    //   - Keep-Alive
    //   - Public
    //   - Proxy-Authenticate
    //   - Transfer-Encoding
    //   - Upgrade
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "public"
            | "proxy-authenticate"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn connection_header_names(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| HeaderName::from_bytes(value.as_bytes()).ok())
        .collect()
}

fn record_successful_request(
    op: ObjectOp,
    obj_id: S3ObjectId,
    state: AppState,
    upstream_name: UpstreamId,
    stale_upstream: Option<UpstreamId>,
    mutation_guard: MutationLockGuard,
) -> impl FnOnce() {
    move || {
        let obj_id_clone = obj_id.clone();
        let recording = async move {
            let _mutation_guard = mutation_guard;
            match op {
                ObjectOp::MultipartStep => return Ok::<_, Report>(()),
                ObjectOp::Delete => {
                    counter!(COUNTER_OBJECT_DELETIONS_TOTAL, "upstream" => upstream_name.0.clone())
                        .increment(1);
                    state
                        .db
                        .delete_object(&obj_id_clone)
                        .await
                        .context("failed to record deletion")
                        .attach(format!("object: {obj_id_clone:?}"))?;
                    return Ok(());
                }
                ObjectOp::Write => {
                    counter!(COUNTER_OBJECT_CREATIONS_TOTAL, "upstream" => upstream_name.0.clone())
                        .increment(1);
                    record_from_upstream(&state, &upstream_name, &obj_id_clone)
                        .await
                        .context("failed to record creation")
                        .attach(format!("object: {obj_id_clone:?}"))?;
                    if let Some(stale) = &stale_upstream {
                        delete_from_upstream(&state, stale, &obj_id_clone)
                            .await
                            .context("failed to delete overwritten object")
                            .attach(format!("object: {obj_id_clone:?}"))?;
                    }
                }
                ObjectOp::Access => {
                    if !state
                        .db
                        .has_object(&obj_id_clone)
                        .await
                        .context("failed to check object existence")?
                    {
                        record_from_upstream(&state, &upstream_name, &obj_id_clone)
                            .await
                            .context("failed to import object on the fly")
                            .attach(format!("object: {obj_id_clone:?}"))?;
                        counter!(COUNTER_OBJECT_IMPORTED_ON_THE_FLY, "upstream" => upstream_name.0.clone())
                            .increment(1);
                    }
                }
            }

            state
                .db
                .record_access(&obj_id_clone, &jiff::Zoned::now())
                .await
                .context("failed to record access")
                .attach(format!("object: {obj_id_clone:?}"))?;

            Ok(())
        };

        tokio::spawn(async move {
            if let Err(e) = recording.await {
                warn!(
                    %e,
                    object_id=?obj_id,
                    "failed to record object creation/deletion"
                );
            }
        });
    }
}

async fn delete_from_upstream(
    state: &AppState,
    upstream_name: &UpstreamId,
    obj_id: &S3ObjectId,
) -> Result<(), Report> {
    let upstream = state.config.upstreams.get(upstream_name).ok_or_else(|| {
        report!("upstream not found in config").attach(format!("upstream: '{upstream_name:?}'"))
    })?;
    S3Client::for_upstream(state.http.clone(), upstream)
        .delete_file(obj_id)
        .await
        .context("failed to DELETE file")
        .attach(format!("upstream: '{upstream_name:?}'"))?;
    Ok(())
}

async fn record_from_upstream(
    state: &AppState,
    upstream_name: &UpstreamId,
    obj_id: &S3ObjectId,
) -> Result<(), Report> {
    let upstream = state.config.upstreams.get(upstream_name).ok_or_else(|| {
        report!("upstream not found in config").attach(format!("upstream: '{upstream_name:?}'"))
    })?;
    let metadata = S3Client::for_upstream(state.http.clone(), upstream)
        .head_file(obj_id)
        .await
        .context("failed to HEAD file")
        .attach(format!("object: {obj_id:?}"))?;
    state
        .db
        .record_creation(&S3Object {
            id: obj_id.clone(),
            assigned_upstream: upstream_name.clone(),
            last_modified: metadata.last_modified,
            size: metadata.size,
        })
        .await
        .context("failed to record object")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ObjectOp, connection_header_names, copy_source, is_hop_by_hop_header};
    use crate::data::S3ObjectId;
    use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};

    #[test]
    fn parses_copy_source() {
        let parse = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert("x-amz-copy-source", HeaderValue::from_str(value).unwrap());
            copy_source(&headers).ok().flatten()
        };
        let id = |bucket: &str, key: &str| {
            Some(S3ObjectId {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })
        };

        assert_eq!(parse("bucket/a/b"), id("bucket", "a/b"));
        assert_eq!(parse("/bucket/a/b"), id("bucket", "a/b"));
        assert_eq!(
            parse("bucket/k%C3%A4y%20%3F?versionId=1"),
            id("bucket", "käy ?")
        );
        assert_eq!(parse("bucket"), None);
        assert_eq!(copy_source(&HeaderMap::new()).ok().flatten(), None);
    }

    #[test]
    fn classifies_object_operations() {
        let cases = [
            (Method::PUT, None, ObjectOp::Write),
            (Method::PUT, Some("tagging"), ObjectOp::Write),
            (
                Method::PUT,
                Some("partNumber=1&uploadId=abc"),
                ObjectOp::MultipartStep,
            ),
            (Method::POST, Some("uploads"), ObjectOp::MultipartStep),
            (Method::POST, Some("uploads="), ObjectOp::MultipartStep),
            (Method::POST, Some("uploadId=abc"), ObjectOp::Write),
            (Method::DELETE, None, ObjectOp::Delete),
            (
                Method::DELETE,
                Some("uploadId=abc"),
                ObjectOp::MultipartStep,
            ),
            (Method::GET, None, ObjectOp::Access),
            (
                Method::GET,
                Some("uploadId=abc&max-parts=10"),
                ObjectOp::MultipartStep,
            ),
            (Method::GET, Some("x-id=uploadIdent"), ObjectOp::Access),
            (Method::HEAD, None, ObjectOp::Access),
        ];
        for (method, query, expected) in cases {
            assert_eq!(
                ObjectOp::classify(&method, query),
                expected,
                "{method} ?{query:?}"
            );
        }
    }

    #[test]
    fn strips_headers_named_by_connection() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "connection",
            HeaderValue::from_static("keep-alive, x-foo, x-bar"),
        );

        let names = connection_header_names(&headers);

        assert!(names.contains(&HeaderName::from_static("keep-alive")));
        assert!(names.contains(&HeaderName::from_static("x-foo")));
        assert!(names.contains(&HeaderName::from_static("x-bar")));
    }

    #[test]
    fn ignores_invalid_connection_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", HeaderValue::from_static("x-ok, bad token"));

        let names = connection_header_names(&headers);

        assert!(names.contains(&HeaderName::from_static("x-ok")));
        assert_eq!(names.len(), 1);
    }

    #[test]
    fn fixed_hop_by_hop_headers_remain_filtered() {
        assert!(is_hop_by_hop_header(&HeaderName::from_static("connection")));
        assert!(is_hop_by_hop_header(&HeaderName::from_static("upgrade")));
        assert!(!is_hop_by_hop_header(&HeaderName::from_static(
            "x-not-hop-by-hop"
        )));
    }
}
