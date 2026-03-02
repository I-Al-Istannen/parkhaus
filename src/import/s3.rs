#[path = "s3-crypto.rs"]
mod s3_crypto;

use self::s3_crypto::sign_v4;
use reqwest::Client;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use serde::Deserialize;
use tracing::Span;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub struct S3Client {
    client: Client,
    key_id: String,
    secret: String,
    endpoint: Url,
    region: String,
}

#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified: jiff::Timestamp,
}

impl S3Client {
    pub fn new(client: Client, endpoint: Url, region: &str, key_id: &str, secret: &str) -> Self {
        Self {
            client,
            key_id: key_id.to_owned(),
            secret: secret.to_owned(),
            endpoint,
            region: region.to_owned(),
        }
    }

    async fn signed_get(&self, url: &Url) -> Result<String, Report> {
        // S3 SigV4 signed GET with an explicit payload hash header.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        let signed = sign_v4(
            &self.key_id,
            &self.secret,
            &self.region,
            "GET",
            url,
            EMPTY_PAYLOAD_SHA256,
        )?;

        let response = self
            .client
            .get(url.as_str())
            .header("host", signed.host)
            .header("x-amz-date", signed.amz_date)
            .header("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256)
            .header("authorization", signed.authorization)
            .send()
            .await
            .context("S3 request failed")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read S3 response body")?;

        if !status.is_success() {
            rootcause::bail!("S3 returned {status}: {body}");
        }

        Ok(body)
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, Report> {
        // we just assume this is not paginated, screw it.
        let body = self
            .signed_get(&self.endpoint)
            .await
            .context("failed to list buckets")?;
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
                .signed_get(&url)
                .await
                .context("failed to list objects")?;
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
    last_modified: jiff::Timestamp,
}
