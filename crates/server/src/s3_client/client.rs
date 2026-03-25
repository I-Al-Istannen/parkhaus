use super::crypto::{SignRequest, SigningConfig, StreamingSignRequest};
use crate::config::{AddressingStyle, Upstream};
use crate::data::{ForwardObjectUrl, S3ObjectId};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::{TryStreamExt, stream};
use jiff::Timestamp;
use reqwest::{Client, Method, Response, StatusCode};
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use serde::Deserialize;
use sha2::Digest;
use std::io::Write;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;
use tracing::Span;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

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
    #[allow(unused)]
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
        // we just assume this is not paginated, screw it.
        let body = self
            .signed_get(&ForwardObjectUrl::no_host(self.endpoint.clone()), &[])
            .await
            .context("failed to list buckets")?
            .text()
            .await
            .context("failed to read ListBuckets response body")?;
        let result: ListAllMyBucketsResult =
            quick_xml::de::from_str(&body).context("failed to parse ListBuckets XML")?;

        Ok(result
            .buckets
            .bucket
            .into_iter()
            .map(|b| BucketInfo { name: b.name })
            .collect())
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

            if result.is_truncated == Some(true) {
                continuation_token = result.next_continuation_token;
            } else {
                break;
            }

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
        let target = self.object_url(id)?;

        let (signed_headers, chunk_signer) = self
            .signing
            .sign_streaming(StreamingSignRequest::now(&target, content_length))?;

        let body = reqwest::Body::wrap_stream(chunked_upload_stream(data, chunk_signer));

        let response = self
            .client
            .put(target.url.clone())
            .headers(signed_headers)
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
                .attach(format!("status: {status}"))
                .attach(format!("url: {}", target.url))
                .attach(format!("response body: {text}")));
        }

        Ok(())
    }

    pub async fn get_file(
        &self,
        id: &S3ObjectId,
    ) -> Result<(Option<u64>, impl AsyncRead + use<>), Report> {
        let url = self.object_url(id)?;
        let response = self.signed_get(&url, &[]).await?;
        let content_length = response.content_length();

        let bytes_stream = response.bytes_stream().map_err(std::io::Error::other);

        Ok((content_length, StreamReader::new(bytes_stream)))
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
                .attach(format!("status: {status}"))
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
        // S3 SigV4 signed GET with an explicit payload hash header.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        let url = &target.url;
        let signed_headers = self
            .signing
            .sign(SignRequest::now(method.clone(), target).with_extra_headers(extra_headers))?;
        let response = self
            .client
            .request(method.clone(), url.clone())
            .headers(signed_headers)
            .send()
            .await
            .context("failed to send S3 request")
            .attach(format!("url: {url}"))?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .context("failed to read S3 response body")?;
            return Err(report!("failed S3 request")
                .attach(format!("method: {method}"))
                .attach(format!("status: {status}"))
                .attach(format!("url: {url}"))
                .attach(format!("response body: {body}")));
        }

        Ok(response)
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

fn chunked_upload_stream(
    reader: impl AsyncRead + Unpin,
    signer: super::crypto::ChunkSigner,
) -> impl futures_util::Stream<Item = Result<Vec<u8>, Report>> {
    struct ChunkState<R> {
        reader: R,
        signer: super::crypto::ChunkSigner,
        buf: Vec<u8>,
        checksum: sha2::Sha256,
        done: bool,
    }

    let state = ChunkState {
        reader: BufReader::new(reader),
        signer,
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
            let sig = state.signer.sign_chunk(&state.buf[..written]);
            write!(chunk, "{written:x};chunk-signature={sig}\r\n").unwrap();
            chunk.extend_from_slice(&state.buf[..written]);
            chunk.extend_from_slice(b"\r\n");
        }

        if written < CHUNK_SIZE {
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums-trailer-chunks
            let checksum_base64 = STANDARD.encode(state.checksum.clone().finalize());
            let (final_sig, trailer_signature) =
                state.signer.sign_final_chunk_and_trailer(&checksum_base64);

            write!(chunk, "0;chunk-signature={final_sig}\r\n").unwrap();
            write!(chunk, "x-amz-checksum-sha256:{checksum_base64}\r\n").unwrap();
            write!(chunk, "x-amz-trailer-signature:{trailer_signature}\r\n\r\n").unwrap();
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

#[cfg(test)]
mod tests {
    use crate::config::AddressingStyle;
    use crate::s3_client::client::S3Client;
    use reqwest::Client;
    use rootcause::Report;
    use std::collections::BTreeMap;
    use url::Url;

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
