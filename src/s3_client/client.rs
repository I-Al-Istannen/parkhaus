use super::crypto::{SignRequest, SigningConfig, StreamingSignRequest};
use crate::data::S3ObjectId;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::stream;
use jiff::Timestamp;
use reqwest::{Client, Response};
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use rootcause::{Report, bail, report};
use serde::Deserialize;
use sha2::Digest;
use std::io::Write;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tracing::Span;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

pub struct S3Client {
    client: Client,
    signing: SigningConfig,
    endpoint: Url,
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
    pub fn new(client: Client, endpoint: Url, region: &str, key_id: &str, secret: &str) -> Self {
        Self {
            client,
            signing: SigningConfig::new(key_id, secret, region),
            endpoint,
        }
    }

    async fn signed_get(
        &self,
        url: &Url,
        extra_headers: &[(&str, &str)],
    ) -> Result<Response, Report> {
        // S3 SigV4 signed GET with an explicit payload hash header.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        let signed_headers = self
            .signing
            .sign(SignRequest::get(url).with_extra_headers(extra_headers))?;
        let response = self
            .client
            .get(url.as_str())
            .headers(signed_headers)
            .send()
            .await
            .context("S3 request failed")?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .context("failed to read S3 response body")?;
            bail!("S3 returned {status}: {body}");
        }

        Ok(response)
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, Report> {
        // we just assume this is not paginated, screw it.
        let body = self
            .signed_get(&self.endpoint, &[])
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

    /// Only works if the server supports the header and the object was uploaded with checksums
    async fn get_object_sha256(&self, id: &S3ObjectId) -> Result<String, Report> {
        let url = self.object_url(id)?;
        let signed_headers = self.signing.sign(
            SignRequest::now("HEAD", &url)
                .with_extra_headers(&[("x-amz-checksum-mode", "ENABLED")]),
        )?;
        let response = self
            .client
            .head(url.as_str())
            .headers(signed_headers)
            .send()
            .await
            .context("S3 request failed")?;

        let status = response.status();
        if !status.is_success() {
            return Err(report!("S3 returned {status}").attach(format!("object: {id:?}")));
        }

        let checksum_header = response
            .headers()
            .get("x-amz-checksum-sha256")
            .context("missing checksum header")
            .attach(format!("object: {id:?}"))?;
        let checksum = checksum_header
            .to_str()
            .context("invalid checksum header value")
            .attach(format!("header: {:?}", checksum_header))
            .attach(format!("object: {id:?}"))?;

        Ok(checksum.to_string())
    }

    /// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    fn list_objects_url(
        &self,
        bucket: &str,
        continuation_token: &Option<String>,
    ) -> Result<Url, Report> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| report!("endpoint URL cannot be a base URL"))?
            .push(bucket);

        // V2 api
        url.query_pairs_mut().append_pair("list-type", "2");
        if let Some(token) = continuation_token {
            url.query_pairs_mut()
                .append_pair("continuation-token", token);
        }
        url.query_pairs_mut().append_pair("max-keys", "1000");

        Ok(url)
    }

    fn object_url(&self, id: &S3ObjectId) -> Result<Url, Report> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| report!("endpoint URL cannot be a base URL"))?
            .push(&id.bucket)
            .extend(id.key.split('/'));
        Ok(url)
    }

    pub async fn put_file(
        &self,
        id: &S3ObjectId,
        data: impl AsyncRead + Unpin + Send + 'static,
        content_length: u64,
    ) -> Result<(), Report> {
        let url = self.object_url(id)?;

        let (signed_headers, chunk_signer) = self
            .signing
            .sign_streaming(StreamingSignRequest::now(&url, content_length))?;

        let body = reqwest::Body::wrap_stream(chunked_upload_stream(data, chunk_signer));

        let response = self
            .client
            .put(url.as_str())
            .headers(signed_headers)
            .body(body)
            .send()
            .await
            .context("PUT request failed")?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            bail!("S3 PUT returned {status}: {text}");
        }

        let attributes = self.get_object_sha256(id).await?;
        println!("Uploaded object metadata: {attributes:#?}");

        Ok(())
    }
}

const CHUNK_SIZE: usize = 64 * 1024;

fn chunked_upload_stream(
    reader: impl AsyncRead + Unpin + Send + 'static,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::sync::Mutex;
    use std::time::Duration;
    use testcontainers::{
        ContainerAsync, GenericImage,
        core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
        runners::AsyncRunner,
    };
    use tokio::sync::OnceCell;

    const S3MOCK_PORT: u16 = 9090;
    static S3MOCK: OnceCell<TestS3Mock> = OnceCell::const_new();

    struct TestS3Mock {
        container: Mutex<Option<ContainerAsync<GenericImage>>>,
        endpoint: Url,
    }

    #[ctor::dtor]
    fn shutdown_s3mock() {
        if let Some(s3mock) = S3MOCK.get() {
            let mut container = s3mock.container.lock().expect("s3mock mutex poisoned");
            if let Some(container) = container.take() {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build test teardown runtime")
                    .block_on(async move {
                        drop(container);
                        tokio::task::yield_now().await;
                    });
            }
        }
    }

    fn bucket_name(test_name: &str) -> String {
        test_name.replace('_', "-")
    }

    fn bucket_name_with_suffix(test_name: &str, suffix: &str) -> String {
        format!("{}-{suffix}", bucket_name(test_name))
    }

    async fn s3mock() -> Result<&'static TestS3Mock, Report> {
        S3MOCK
            .get_or_try_init(|| async {
                let container = GenericImage::new("adobe/s3mock", "latest")
                    .with_exposed_port(S3MOCK_PORT.tcp())
                    .with_wait_for(WaitFor::http(
                        HttpWaitStrategy::new("/")
                            .with_port(S3MOCK_PORT.tcp())
                            .with_response_matcher(|response| {
                                let status = response.status();
                                status.is_success() || status.is_client_error()
                            })
                            .with_poll_interval(Duration::from_millis(100)),
                    ))
                    .start()
                    .await
                    .context("failed to start s3mock container")?;

                let host = container
                    .get_host()
                    .await
                    .context("failed to resolve s3mock host")?;
                let port = container
                    .get_host_port_ipv4(S3MOCK_PORT)
                    .await
                    .context("failed to resolve s3mock port mapping")?;
                let endpoint: Url = format!("http://{host}:{port}")
                    .parse()
                    .context("failed to parse s3mock endpoint URL")?;

                Ok(TestS3Mock {
                    container: Mutex::new(Some(container)),
                    endpoint,
                })
            })
            .await
    }

    impl TestS3Mock {
        fn client(&self) -> S3Client {
            let endpoint = self.endpoint.clone();
            let client = Client::new();
            S3Client::new(client, endpoint, "us-east-1", "test-key", "test-secret")
        }

        async fn make_bucket(&self, bucket: &str) -> Result<(), Report> {
            let client = self.client();
            let mut url = client.endpoint.clone();
            url.path_segments_mut().unwrap().push(bucket);
            let signed_headers = client.signing.sign(SignRequest::now("PUT", &url))?;
            let response = client
                .client
                .put(url.as_str())
                .headers(signed_headers)
                .send()
                .await
                .context("failed to create bucket")?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("failed to create bucket {bucket}: {status}: {body}");
            }

            Ok(())
        }

        async fn put_object(&self, bucket: &str, key: &str, payload: &[u8]) -> Result<(), Report> {
            let id = S3ObjectId {
                bucket: bucket.into(),
                key: key.into(),
            };
            self.client()
                .put_file(&id, Cursor::new(payload.to_vec()), payload.len() as u64)
                .await
        }
    }

    #[tokio::test]
    async fn test_put_and_get_file() -> Result<(), Report> {
        let s3mock = s3mock().await?;
        let client = s3mock.client();
        let bucket = bucket_name("test_put_and_get_file");
        s3mock.make_bucket(&bucket).await?;

        let payload = b"Hello, chunked S3 upload!";
        let id = S3ObjectId {
            bucket,
            key: "test-key.txt".into(),
        };

        client
            .put_file(&id, Cursor::new(payload.to_vec()), payload.len() as u64)
            .await?;

        let actual_sha256 = client.get_object_sha256(&id).await?;
        let expected_checksum = STANDARD.encode(sha2::Sha256::digest(payload));

        assert_eq!(actual_sha256, expected_checksum);

        let get_url = client.object_url(&id)?;
        let body = client.signed_get(&get_url, &[]).await?.text().await?;
        assert_eq!(body, "Hello, chunked S3 upload!");
        Ok(())
    }

    #[tokio::test]
    async fn test_put_large_file() -> Result<(), Report> {
        let s3mock = s3mock().await?;
        let client = s3mock.client();
        let bucket = bucket_name("test_put_large_file");
        s3mock.make_bucket(&bucket).await?;

        let payload: Vec<u8> = (0..CHUNK_SIZE * 10 + 1000)
            .map(|i| (i % 256) as u8)
            .collect();
        let id = S3ObjectId {
            bucket,
            key: "large-file.bin".into(),
        };

        client
            .put_file(&id, Cursor::new(payload.clone()), payload.len() as u64)
            .await?;

        let get_url = client.object_url(&id)?;
        let signed_headers = client.signing.sign(SignRequest::get(&get_url))?;
        let resp = client
            .client
            .get(get_url.as_str())
            .headers(signed_headers)
            .send()
            .await?;
        let body = resp.bytes().await?;
        assert_eq!(body.len(), payload.len(), "body length mismatch");
        assert_eq!(body.as_ref(), payload.as_slice());
        Ok(())
    }

    #[tokio::test]
    async fn test_put_empty_file() -> Result<(), Report> {
        let s3mock = s3mock().await?;
        let client = s3mock.client();
        let bucket = bucket_name("test_put_empty_file");
        s3mock.make_bucket(&bucket).await?;

        let id = S3ObjectId {
            bucket,
            key: "empty.txt".into(),
        };

        client.put_file(&id, Cursor::new(vec![]), 0).await?;

        let get_url = client.object_url(&id)?;
        let body = client.signed_get(&get_url, &[]).await?.text().await?;
        assert_eq!(body, "");
        Ok(())
    }

    #[tokio::test]
    async fn test_list_buckets() -> Result<(), Report> {
        let s3mock = s3mock().await?;
        let client = s3mock.client();

        let expected_buckets = [
            bucket_name_with_suffix("test_list_buckets", "a"),
            bucket_name_with_suffix("test_list_buckets", "b"),
        ];
        for bucket in &expected_buckets {
            s3mock.make_bucket(bucket).await?;
        }

        let bucket_names = client
            .list_buckets()
            .await?
            .into_iter()
            .map(|bucket| bucket.name)
            .collect::<Vec<_>>();

        for bucket in &expected_buckets {
            assert!(
                bucket_names.iter().any(|name| name == bucket),
                "missing bucket {bucket} in {bucket_names:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_list_objects() -> Result<(), Report> {
        let s3mock = s3mock().await?;
        let client = s3mock.client();
        let bucket = bucket_name("test_list_objects");
        s3mock.make_bucket(&bucket).await?;

        let expected_objects = [
            ("alpha.txt", b"alpha".as_slice()),
            ("bravo.bin", &[1, 2, 3, 4, 5]),
            ("foo/bar.bin", &[1, 2, 3, 4, 5]),
            ("foos/bars/bar.bin", "ayyy".as_bytes()),
            ("charlie-empty", b"".as_slice()),
        ];
        for (key, payload) in expected_objects {
            s3mock.put_object(&bucket, key, payload).await?;
        }

        let mut objects = client.list_objects(&bucket).await?;
        objects.sort_by(|left, right| left.key.cmp(&right.key));

        let mut expected = expected_objects
            .into_iter()
            .map(|(key, payload)| (key.to_string(), payload.len() as u64))
            .collect::<Vec<_>>();
        expected.sort();

        assert_eq!(objects.len(), expected.len());
        for (object, (expected_key, expected_size)) in objects.iter().zip(expected) {
            assert_eq!(object.key, expected_key);
            assert_eq!(object.size, expected_size);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_list_objects_url_with_continuation_token() -> Result<(), Report> {
        let s3mock = s3mock().await?;
        let client = s3mock.client();
        let url = client.list_objects_url("bucket-name", &Some("next token/+".to_string()))?;

        assert_eq!(url.path(), "/bucket-name");
        let query = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
        assert_eq!(query.get("list-type"), Some(&"2".to_string()));
        assert_eq!(
            query.get("continuation-token"),
            Some(&"next token/+".to_string())
        );
        assert_eq!(query.get("max-keys"), Some(&"1000".to_string()));

        Ok(())
    }
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
