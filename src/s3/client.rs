use super::crypto::{SignRequest, SigningConfig, StreamingSignRequest};
use crate::config::Upstream;
use crate::data::S3ObjectId;
use crate::s3::types::{AddressingStyle, ForwardObjectUrl};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::{TryStreamExt, stream};
use jiff::Timestamp;
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Client, Method, Response, StatusCode};
use rootcause::prelude::ResultExt;
use rootcause::{Report, bail, report};
use serde::Deserialize;
use sha2::Digest;
use std::io::Write;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;
use tracing::Span;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

/// Client for talking to upstreams: never follows redirects, so signed requests and their
/// credentials stay with the configured endpoint.
pub fn http_client() -> Result<Client, Report> {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .context("failed to build HTTP client")
        .map_err(Report::into_dynamic)
}

/// The status an upstream answered a failed request with.
#[derive(Debug)]
pub struct UpstreamStatus(pub StatusCode);

impl std::fmt::Display for UpstreamStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "status: {}", self.0)
    }
}

/// Whether the error says the upstream is unreachable or overloaded, rather than that the request
/// itself failed.
pub fn is_upstream_unavailable(report: &Report) -> bool {
    report.iter_reports().any(|node| {
        node.downcast_current_context::<reqwest::Error>()
            .is_some_and(|e| e.is_connect() || e.is_timeout())
            || node.attachments().iter().any(|it| {
                it.downcast_inner::<UpstreamStatus>().is_some_and(|it| {
                    it.0.is_server_error() || it.0 == StatusCode::TOO_MANY_REQUESTS
                })
            })
    })
}

/// Headers describing an object, which a copy must keep (next to `x-amz-meta-*`).
const OBJECT_HEADERS: [&str; 6] = [
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-type",
    "expires",
];

pub struct Download<R> {
    pub size: Option<u64>,
    /// Content headers and user metadata of the object.
    pub headers: Vec<(String, String)>,
    pub data: R,
}

pub struct S3Client {
    client: Client,
    signing: SigningConfig,
    endpoint: Url,
    addressing_style: AddressingStyle,
}

#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: Timestamp,
}

impl S3Client {
    pub fn new(
        client: Client,
        endpoint: Url,
        region: &str,
        key_id: &str,
        secret: &str,
        addressing_style: AddressingStyle,
    ) -> Self {
        Self {
            client,
            signing: SigningConfig::new(key_id, secret, region),
            endpoint,
            addressing_style,
        }
    }

    pub fn for_upstream(client: Client, upstream: &Upstream) -> Self {
        Self::new(
            client,
            upstream.base_url.clone(),
            &upstream.region,
            &upstream.s3_access_key,
            &upstream.s3_secret.0,
            upstream.addressing_style.clone(),
        )
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, Report> {
        let mut buckets = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut target = ForwardObjectUrl::no_host(self.endpoint.clone());
            if let Some(token) = &continuation_token {
                target
                    .url
                    .query_pairs_mut()
                    .append_pair("continuation-token", token);
            }
            let body = self
                .signed_get(&target, &[])
                .await
                .context("failed to list buckets")?
                .text()
                .await
                .context("failed to read ListBuckets response body")?;
            let result: ListAllMyBucketsResult =
                quick_xml::de::from_str(&body).context("failed to parse ListBuckets XML")?;
            buckets.extend(
                result
                    .buckets
                    .bucket
                    .into_iter()
                    .map(|b| BucketInfo { name: b.name }),
            );

            match result.continuation_token {
                Some(token) if !token.is_empty() => continuation_token = Some(token),
                _ => return Ok(buckets),
            }
        }
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<(), Report> {
        let target = self
            .addressing_style
            .format_bucket_url(&self.endpoint, bucket)?;
        self.signed_request(&target, Method::PUT, &[])
            .await
            .context("failed to create bucket")
            .attach(format!("bucket: {bucket}"))?;
        Ok(())
    }

    pub async fn list_objects(&self, bucket: &str) -> Result<Vec<ObjectInfo>, Report> {
        let mut all_objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page_count = 0;
        let mut object_count = 0;

        loop {
            // S3 ListObjectsV2 pagination:
            // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
            let url = self.list_objects_url(bucket, &continuation_token)?;

            let body = self
                .signed_get(&url, &[])
                .await
                .context("failed to list objects")?
                .text()
                .await
                .context("failed to read ListObjectsV2 response body")?;
            let result: ListBucketResult =
                quick_xml::de::from_str(&body).context("failed to parse ListObjectsV2 XML")?;

            object_count += result.contents.len();
            all_objects.extend(result.contents.into_iter().map(|it| ObjectInfo {
                key: it.key,
                size: it.size,
                last_modified: it.last_modified,
            }));

            if result.is_truncated != Some(true) {
                break;
            }
            let Some(token) = result.next_continuation_token else {
                bail!("truncated listing of bucket {bucket} has no continuation token");
            };
            continuation_token = Some(token);

            page_count += 1;
            Span::current().pb_set_message(&format!(" page {page_count}, {object_count} objects"));
        }

        Ok(all_objects)
    }

    pub async fn put_file(
        &self,
        id: &S3ObjectId,
        data: impl AsyncRead + Unpin + Send + 'static,
        content_length: u64,
    ) -> Result<(), Report> {
        self.put_file_with_headers(id, data, content_length, &[])
            .await
    }

    /// Uploads with additional headers, e.g. the [`Download::headers`] of another copy.
    pub async fn put_file_with_headers(
        &self,
        id: &S3ObjectId,
        data: impl AsyncRead + Unpin + Send + 'static,
        content_length: u64,
        headers: &[(String, String)],
    ) -> Result<(), Report> {
        let target = self.object_url(id)?;

        let signed_headers = self.signing.sign_streaming(
            StreamingSignRequest::now(&target, content_length).with_extra_headers(headers),
        )?;

        let body = reqwest::Body::wrap_stream(chunked_upload_stream(data));

        let response = self
            .client
            .put(target.url.clone())
            .headers(signed_headers)
            .header(CONTENT_LENGTH, chunked_upload_length(content_length))
            .body(body)
            .send()
            .await
            .context("failed sending PUT request")
            .attach(format!("object: {id}"))?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(report!("failed S3 request")
                .attach("method: streaming PUT")
                .attach(UpstreamStatus(status))
                .attach(format!("url: {}", target.url))
                .attach(format!("response body: {text}")));
        }

        Ok(())
    }

    pub async fn get_file(
        &self,
        id: &S3ObjectId,
    ) -> Result<(Option<u64>, impl AsyncRead + use<>), Report> {
        let download = self.download(id).await?;
        Ok((download.size, download.data))
    }

    pub async fn download(
        &self,
        id: &S3ObjectId,
    ) -> Result<Download<impl AsyncRead + use<>>, Report> {
        let url = self.object_url(id)?;
        let response = self.signed_get(&url, &[]).await?;
        let size = response.content_length();

        let mut headers = Vec::new();
        for (name, value) in response.headers() {
            if OBJECT_HEADERS.contains(&name.as_str()) || name.as_str().starts_with("x-amz-meta-") {
                let value = value
                    .to_str()
                    .context("object header is not visible ASCII")
                    .attach(format!("header: {name}"))
                    .attach(format!("object: {id}"))?;
                headers.push((name.to_string(), value.to_string()));
            }
        }

        let bytes_stream = response.bytes_stream().map_err(std::io::Error::other);
        Ok(Download {
            size,
            headers,
            data: StreamReader::new(bytes_stream),
        })
    }

    pub async fn head_file(&self, id: &S3ObjectId) -> Result<ObjectInfo, Report> {
        self.try_head_file(id).await?.ok_or_else(|| {
            report!("object not found")
                .attach(format!("status: {}", StatusCode::NOT_FOUND))
                .attach(format!("object: {id}"))
        })
    }

    /// Like [`Self::head_file`], but returns `None` if the object does not exist.
    pub async fn try_head_file(&self, id: &S3ObjectId) -> Result<Option<ObjectInfo>, Report> {
        let url = self.object_url(id)?;
        let response = self.send_signed(&url, Method::HEAD, &[]).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = check_status(response, &Method::HEAD, &url.url).await?;

        let last_modified = response.headers().get("last-modified").ok_or_else(|| {
            report!("missing Last-Modified header in HEAD response").attach(format!("object: {id}"))
        })?;
        let last_modified = last_modified
            .to_str()
            .context("invalid Last-Modified header in HEAD response")
            .attach(format!("object: {id}"))
            .attach(format!("header value: {last_modified:?}"))?;
        let last_modified = jiff::fmt::strtime::parse("%a, %d %b %Y %H:%M:%S %Q", last_modified)?;
        let last_modified = last_modified
            .to_zoned()
            .context("failed to parse Last-Modified header in HEAD response")
            .attach(format!("object: {id}"))
            .attach(format!("header value: {last_modified:?}"))?;

        let size = response.headers().get("content-length").ok_or_else(|| {
            report!("missing Content-Length header in HEAD response")
                .attach(format!("object: {id}"))
        })?;
        let size = size
            .to_str()
            .context("invalid Content-Length header in HEAD response")
            .attach(format!("object: {id}"))
            .attach(format!("header value: {size:?}"))?;
        let size = size
            .parse::<u64>()
            .context("failed to parse Content-Length header in HEAD response")
            .attach(format!("object: {id}"))
            .attach(format!("header value: {size:?}"))?;

        Ok(Some(ObjectInfo {
            last_modified: last_modified.timestamp(),
            key: id.key.clone(),
            size,
        }))
    }

    /// Aborts a multipart upload. Returns `false` if the upload did not exist.
    pub async fn abort_multipart_upload(
        &self,
        id: &S3ObjectId,
        upload_id: &str,
    ) -> Result<bool, Report> {
        let mut target = self.object_url(id)?;
        target
            .url
            .query_pairs_mut()
            .append_pair("uploadId", upload_id);
        let response = self.send_signed(&target, Method::DELETE, &[]).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        check_status(response, &Method::DELETE, &target.url).await?;
        Ok(true)
    }

    /// Tries to delete a file. Returns `false` if the file did not exist in the first place and
    /// `true` if it was deleted.
    pub async fn delete_file(&self, id: &S3ObjectId) -> Result<bool, Report> {
        let target = self.object_url(id)?;
        let signed_headers = self
            .signing
            .sign(SignRequest::now(Method::DELETE, &target))?;
        let response = self
            .client
            .delete(target.url.clone())
            .headers(signed_headers)
            .send()
            .await
            .context("DELETE request failed")
            .attach(format!("object: {id}"))
            .attach(format!("url: {}", target.url))?;

        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(report!("S3 delete failed")
                .attach(UpstreamStatus(status))
                .attach(format!("object: {id}"))
                .attach(format!("response body: {text}")));
        }

        Ok(true)
    }

    async fn signed_request(
        &self,
        target: &ForwardObjectUrl,
        method: Method,
        extra_headers: &[(&str, &str)],
    ) -> Result<Response, Report> {
        let response = self
            .send_signed(target, method.clone(), extra_headers)
            .await?;
        check_status(response, &method, &target.url).await
    }

    // S3 SigV4 signed request with an explicit payload hash header.
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    async fn send_signed(
        &self,
        target: &ForwardObjectUrl,
        method: Method,
        extra_headers: &[(&str, &str)],
    ) -> Result<Response, Report> {
        let url = &target.url;
        let signed_headers = self
            .signing
            .sign(SignRequest::now(method.clone(), target).with_extra_headers(extra_headers))?;
        self.client
            .request(method, url.clone())
            .headers(signed_headers)
            .send()
            .await
            .context("failed to send S3 request")
            .attach(format!("url: {url}"))
            .map_err(Report::into_dynamic)
    }

    async fn signed_get(
        &self,
        target: &ForwardObjectUrl,
        extra_headers: &[(&str, &str)],
    ) -> Result<Response, Report> {
        self.signed_request(target, Method::GET, extra_headers)
            .await
    }

    /// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    fn list_objects_url(
        &self,
        bucket: &str,
        continuation_token: &Option<String>,
    ) -> Result<ForwardObjectUrl, Report> {
        let mut target = self
            .addressing_style
            .format_bucket_url(&self.endpoint, bucket)?;
        let url = &mut target.url;

        // V2 api
        url.query_pairs_mut().append_pair("list-type", "2");
        if let Some(token) = continuation_token {
            url.query_pairs_mut()
                .append_pair("continuation-token", token);
        }
        url.query_pairs_mut().append_pair("max-keys", "1000");

        Ok(target)
    }

    fn object_url(&self, id: &S3ObjectId) -> Result<ForwardObjectUrl, Report> {
        self.addressing_style
            .format_url(&self.endpoint, &id.bucket, Some(&id.key))
            .map_err(Report::into_dynamic)
    }
}

pub const CHUNK_SIZE: usize = 64 * 1024;

/// The length of [`chunked_upload_stream`] for `decoded` bytes of data.
fn chunked_upload_length(decoded: u64) -> u64 {
    let chunk = |len: u64| format!("{len:x}").len() as u64 + len + 4;
    let full = decoded / CHUNK_SIZE as u64;
    let rest = decoded % CHUNK_SIZE as u64;
    // "0\r\n" and "x-amz-checksum-sha256:<44 base64 chars>\r\n\r\n"
    let trailer = 3 + "x-amz-checksum-sha256:".len() as u64 + 44 + 4;
    full * chunk(CHUNK_SIZE as u64) + if rest > 0 { chunk(rest) } else { 0 } + trailer
}

fn chunked_upload_stream(
    reader: impl AsyncRead + Unpin,
) -> impl futures_util::Stream<Item = Result<Vec<u8>, Report>> {
    struct ChunkState<R> {
        reader: R,
        buf: Vec<u8>,
        checksum: sha2::Sha256,
        done: bool,
    }

    let state = ChunkState {
        reader: BufReader::new(reader),
        buf: vec![0u8; CHUNK_SIZE],
        checksum: sha2::Sha256::new(),
        done: false,
    };

    stream::unfold(state, |mut state| async move {
        if state.done {
            return None;
        }

        let written = match read_full(&mut state.reader, &mut state.buf).await {
            Ok(n) => n,
            Err(e) => {
                state.done = true;
                return Some((Err(e), state));
            }
        };

        let mut chunk = Vec::new();

        if written > 0 {
            state.checksum.update(&state.buf[..written]);
            write!(chunk, "{written:x}\r\n").unwrap();
            chunk.extend_from_slice(&state.buf[..written]);
            chunk.extend_from_slice(b"\r\n");
        }

        if written < CHUNK_SIZE {
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums-trailer-chunks
            let checksum_base64 = STANDARD.encode(state.checksum.clone().finalize());
            write!(chunk, "0\r\n").unwrap();
            write!(chunk, "x-amz-checksum-sha256:{checksum_base64}\r\n\r\n").unwrap();
            state.done = true;
        }

        Some((Ok(chunk), state))
    })
}

async fn read_full(reader: &mut (impl AsyncRead + Unpin), buf: &mut [u8]) -> Result<usize, Report> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader
            .read(&mut buf[filled..])
            .await
            .context("failed to read upload data")?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

// XML response types for S3 APIs

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListAllMyBucketsResult {
    buckets: Buckets,
    continuation_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Buckets {
    #[serde(default)]
    bucket: Vec<BucketXml>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BucketXml {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    contents: Vec<ContentsXml>,
    is_truncated: Option<bool>,
    next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ContentsXml {
    key: String,
    size: u64,
    last_modified: Timestamp,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetObjectAttributesOutput {
    checksum: Option<ChecksumXml>,
    #[serde(rename = "ETag")]
    etag: Option<String>,
    object_size: Option<u64>,
    storage_class: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ChecksumXml {
    #[serde(rename = "ChecksumSHA256")]
    sha256: String,
    checksum_type: String,
}

async fn check_status(response: Response, method: &Method, url: &Url) -> Result<Response, Report> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response
        .text()
        .await
        .context("failed to read S3 response body")?;
    Err(report!("failed S3 request")
        .attach(format!("method: {method}"))
        .attach(UpstreamStatus(status))
        .attach(format!("url: {url}"))
        .attach(format!("response body: {body}")))
}

#[cfg(test)]
mod tests {
    use crate::s3::client::{
        AddressingStyle, CHUNK_SIZE, S3Client, chunked_upload_length, chunked_upload_stream,
    };
    use futures_util::TryStreamExt;
    use reqwest::Client;
    use rootcause::Report;
    use std::collections::BTreeMap;
    use url::Url;

    #[tokio::test]
    async fn chunked_upload_length_matches_stream() -> Result<(), Report> {
        for len in [
            0,
            1,
            4095,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            3 * CHUNK_SIZE,
        ] {
            let encoded: Vec<Vec<u8>> = chunked_upload_stream(std::io::Cursor::new(vec![7u8; len]))
                .try_collect()
                .await?;
            let encoded = encoded.concat().len() as u64;
            assert_eq!(encoded, chunked_upload_length(len as u64), "length {len}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_list_objects_url_with_continuation_token() -> Result<(), Report> {
        // This test only checks URL construction — any bucket/key will do.
        let client = S3Client::new(
            Client::new(),
            Url::parse("https://goo.local")?,
            "garage",
            "dummy-key",
            "dummy-secret",
            AddressingStyle::Path,
        );
        let target = client.list_objects_url("bucket-name", &Some("next token/+".to_string()))?;
        let url = target.url;
        let host_header = target.host_header;

        assert_eq!(url.path(), "/bucket-name");
        assert_eq!(host_header, None);
        let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        assert_eq!(query.get("list-type"), Some(&"2".to_string()));
        assert_eq!(
            query.get("continuation-token"),
            Some(&"next token/+".to_string())
        );
        assert_eq!(query.get("max-keys"), Some(&"1000".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_list_objects_url_virtual_hosted() -> Result<(), Report> {
        let client = S3Client::new(
            Client::new(),
            Url::parse("https://goo.local")?,
            "garage",
            "dummy-key",
            "dummy-secret",
            AddressingStyle::VirtualHosted,
        );

        let target = client.list_objects_url("bucket-name", &None)?;
        let url = target.url;
        let host_header = target.host_header;

        assert_eq!(url.path(), "/");
        let expected_host = "bucket-name.goo.local";
        assert_eq!(host_header.as_deref(), Some(expected_host));
        let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        assert_eq!(query.get("list-type"), Some(&"2".to_string()));
        assert_eq!(query.get("max-keys"), Some(&"1000".to_string()));

        Ok(())
    }
}
