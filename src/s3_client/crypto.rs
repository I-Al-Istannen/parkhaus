use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use jiff::Timestamp;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use reqwest::header::{AUTHORIZATION, HOST, HeaderMap, HeaderValue};
use rootcause::Report;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use sha2::{Digest, Sha256};
use url::Url;

// Spec: URI encode every byte except the unreserved characters:
//   'A'-'Z', 'a'-'z', '0'-'9', '-', '.', '_', and '~'.
// The space character is a reserved character and must be encoded as "%20" (and not as "+").
const SIGV4_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');
// Encode the forward slash character, '/', everywhere except in the object key name.
// For example, if the object key name is photos/Jan/sample.jpg, the forward slash in the key name
// is not encoded.
const SIGV4_ENCODE_SET_NAME: &AsciiSet = &SIGV4_ENCODE_SET.remove(b'/');

const STREAMING_PAYLOAD_HASH: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Debug, Clone)]
struct SigningResult {
    payload_hash: String,
    host: String,
    amz_date: String,
    authorization: String,
    signature: String,
    scope: String,
    signing_key: Vec<u8>,
}

impl SigningResult {
    fn headers(&self) -> Result<HeaderMap, Report> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HOST,
            HeaderValue::from_str(&self.host).context("invalid host")?,
        );
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&self.authorization).context("invalid authorization")?,
        );
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_str(&self.payload_hash).context("invalid hash")?,
        );
        headers.insert(
            "x-amz-date",
            HeaderValue::from_str(&self.amz_date).context("invalid date")?,
        );
        Ok(headers)
    }
}

pub struct ChunkSigner {
    signing_key: Vec<u8>,
    scope: String,
    amz_date: String,
    previous_signature: String,
}

impl ChunkSigner {
    pub fn sign_chunk(&mut self, chunk_data: &[u8]) -> String {
        let chunk_hash = hex::encode(Sha256::digest(chunk_data));
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{EMPTY_SHA256}\n{chunk_hash}",
            self.amz_date, self.scope, self.previous_signature,
        );
        let signature = hex::encode(hmac_sha256(&self.signing_key, string_to_sign.as_bytes()));
        self.previous_signature.clone_from(&signature);
        signature
    }
}

pub struct SignRequest<'a> {
    method: &'a str,
    url: &'a Url,
    payload_hash: &'a str,
    current_time: Timestamp,
    extra_headers: &'a [(&'a str, &'a str)],
}

impl<'a> SignRequest<'a> {
    pub fn new(method: &'a str, url: &'a Url, current_time: Timestamp) -> Self {
        Self {
            method,
            url,
            payload_hash: EMPTY_SHA256,
            current_time,
            extra_headers: &[],
        }
    }

    pub fn now(method: &'a str, url: &'a Url) -> Self {
        Self::new(method, url, Timestamp::now())
    }

    pub fn get(url: &'a Url) -> Self {
        Self::now("GET", url)
    }

    pub fn with_payload_hash(mut self, payload_hash: &'a str) -> Self {
        self.payload_hash = payload_hash;
        self
    }

    pub fn with_extra_headers(mut self, extra_headers: &'a [(&'a str, &'a str)]) -> Self {
        self.extra_headers = extra_headers;
        self
    }
}

pub struct StreamingSignRequest<'a> {
    url: &'a Url,
    decoded_content_length: u64,
    current_time: Timestamp,
}

impl<'a> StreamingSignRequest<'a> {
    pub fn new(url: &'a Url, decoded_content_length: u64, current_time: Timestamp) -> Self {
        Self {
            url,
            decoded_content_length,
            current_time,
        }
    }

    pub fn now(url: &'a Url, decoded_content_length: u64) -> Self {
        Self::new(url, decoded_content_length, Timestamp::now())
    }
}

#[derive(Clone)]
pub struct SigningConfig {
    key_id: String,
    secret: String,
    region: String,
}

impl SigningConfig {
    pub fn new(
        key_id: impl Into<String>,
        secret: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            key_id: key_id.into(),
            secret: secret.into(),
            region: region.into(),
        }
    }

    pub fn sign(&self, request: SignRequest<'_>) -> Result<HeaderMap, Report> {
        let res = self.sign_impl(request)?;
        res.headers()
    }

    pub fn sign_streaming(
        &self,
        request: StreamingSignRequest<'_>,
    ) -> Result<(HeaderMap, ChunkSigner), Report> {
        let decoded_len_str = request.decoded_content_length.to_string();
        let signed = self.sign_impl(
            SignRequest::new("PUT", request.url, request.current_time)
                .with_payload_hash(STREAMING_PAYLOAD_HASH)
                .with_extra_headers(&[
                    ("content-encoding", "aws-chunked"),
                    ("x-amz-decoded-content-length", decoded_len_str.as_str()),
                ]),
        )?;

        let chunk_signer = ChunkSigner {
            signing_key: signed.signing_key.clone(),
            amz_date: signed.amz_date.clone(),
            scope: signed.scope.clone(),
            previous_signature: signed.signature.clone(),
        };

        let mut headers = signed.headers()?;
        headers.insert("content-encoding", HeaderValue::from_str("aws-chunked")?);
        headers.insert(
            "x-amz-decoded-content-length",
            HeaderValue::from_str(&decoded_len_str)?,
        );

        Ok((headers, chunk_signer))
    }

    fn sign_impl(&self, request: SignRequest<'_>) -> Result<SigningResult, Report> {
        // Spec: Date must be in ISO 8601 basic format: YYYYMMDD'T'HHMMSS'Z'
        let amz_date = request.current_time.strftime("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = request.current_time.strftime("%Y%m%d").to_string();

        let host = request.url.host_str().context("URL must have a host")?;

        // Include the origin if it is not the default port for the scheme
        let host_header = match request.url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };

        let (canonical_request, signed_headers) = build_canonical_request(
            request.method,
            request.url,
            &host_header,
            &amz_date,
            request.payload_hash,
            request.extra_headers,
        );

        let scope = format!("{date_stamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = build_string_to_sign(&amz_date, &scope, &canonical_request);
        let signing_key = derive_signing_key(&self.secret, &date_stamp, &self.region);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.key_id,
        );

        Ok(SigningResult {
            payload_hash: request.payload_hash.to_string(),
            host: host_header,
            amz_date,
            authorization,
            signing_key,
            scope,
            signature,
        })
    }
}

// Task 1: Create a Canonical Request
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#canonical-request
//
// CanonicalRequest =
//   HTTPRequestMethod + '\n' +
//   CanonicalURI + '\n' +
//   CanonicalQueryString + '\n' +
//   CanonicalHeaders + '\n' +
//   SignedHeaders + '\n' +
//   HexEncode(Hash(RequestPayload))
fn build_canonical_request(
    method: &str,
    url: &Url,
    host: &str,
    amz_date: &str,
    payload_hash: &str,
    extra: &[(&str, &str)],
) -> (String, String) {
    let canonical_uri = canonical_uri(url);
    let canonical_query = canonical_query_string(url);

    let extra = extra
        .iter()
        .cloned()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect::<Vec<_>>();
    let mut headers = BTreeMap::new();
    headers.insert("host", host);
    headers.insert("x-amz-content-sha256", payload_hash);
    headers.insert("x-amz-date", amz_date);
    headers.extend(extra.iter().map(|(k, v)| (k.as_str(), *v)));

    // Individual header name and value pairs are separated by the newline character ("\n").
    // Header names must be in lowercase.
    // You must sort the header names alphabetically to construct the string
    //
    // For S3 you MUST include: host, x-amz-content-sha256, x-amz-date
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();

    // SignedHeaders is an alphabetically sorted, semicolon-separated list
    // of lowercase request header names.
    let signed_headers: String = headers.keys().copied().collect::<Vec<_>>().join(";");

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    (canonical_request, signed_headers.to_owned())
}

// CanonicalURI is the URI-encoded version of the absolute path component of the URI -- everything
// starting with the "/" that follows the domain name and up to the end of the string or to the
// question mark character ('?') if you have query string parameters.
fn canonical_uri(url: &Url) -> String {
    // path_segments() yields percent-encoded segments; decode first, then
    // re-encode with the SigV4 encode set so encoding is normalized per spec.
    let Some(segments) = url.path_segments() else {
        return "/".to_string();
    };
    let encoded: String = segments
        .map(|seg| {
            let decoded = percent_decode_str(seg).decode_utf8_lossy();
            utf8_percent_encode(&decoded, SIGV4_ENCODE_SET_NAME).to_string()
        })
        .collect::<Vec<_>>()
        .join("/");

    // The canonical URI must start with a "/". If the absolute path is empty, it just becomes "/".
    format!("/{encoded}")
}

// CanonicalQueryString specifies the URI-encoded query string parameters.
// You URI-encode name and values individually.
// You must also sort the parameters in the canonical query string alphabetically by key name.
// The sorting occurs after encoding.
fn canonical_query_string(url: &Url) -> String {
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| {
            (
                utf8_percent_encode(&k, SIGV4_ENCODE_SET).to_string(),
                utf8_percent_encode(&v, SIGV4_ENCODE_SET).to_string(),
            )
        })
        .collect();

    // Sort by key, then by value for duplicate keys
    pairs.sort();

    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

// Task 2: Create a String to Sign
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#request-string
// StringToSign =
//   Algorithm + '\n' +
//   RequestDateTime + '\n' +
//   CredentialScope + '\n' +
//   HexEncode(Hash(CanonicalRequest))
fn build_string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    let canonical_hash = sha256_hex(canonical_request);
    format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_hash}")
}

// Task 3: Calculate Signature -- Derive the Signing Key
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#signing-key
// DateKey              = HMAC-SHA256("AWS4" + SecretAccessKey, Date)
// DateRegionKey        = HMAC-SHA256(DateKey, Region)
// DateRegionServiceKey = HMAC-SHA256(DateRegionKey, Service)
// SigningKey           = HMAC-SHA256(DateRegionServiceKey, "aws4_request")
fn derive_signing_key(secret: &str, date_stamp: &str, region: &str) -> Vec<u8> {
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let date_region_key = hmac_sha256(&date_key, region.as_bytes());
    let date_region_service_key = hmac_sha256(&date_region_key, b"s3");
    hmac_sha256(&date_region_service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    Hmac::<Sha256>::new_from_slice(key)
        .expect("HMAC-SHA256 accepts any key length")
        .chain_update(data)
        .finalize()
        .into_bytes()
        .to_vec()
}

fn sha256_hex(data: &str) -> String {
    hex::encode(Sha256::digest(data.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test vectors from the AWS SigV4 signing example and the blog post at
    // https://czak.pl/2015/09/15/s3-rest-api-with-curl.html
    //
    // Example data:
    //   Access Key ID:     AKIAIOSFODNN7EXAMPLE
    //   Secret Access Key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    //   Region:            us-east-1
    //   Service:           s3
    //   Timestamp:         20150915T124500Z
    //   Method:            GET
    //   URL:               https://my-precious-bucket.s3.amazonaws.com/

    const EXAMPLE_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const EXAMPLE_SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const EXAMPLE_REGION: &str = "us-east-1";
    const EXAMPLE_DATE: &str = "20150915";
    const EXAMPLE_AMZ_DATE: &str = "20150915T124500Z";
    const EMPTY_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn test_canonical_uri_root() {
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(canonical_uri(&url), "/");
    }

    #[test]
    fn test_canonical_uri_empty() {
        let url = Url::parse("https://example.com").unwrap();
        assert_eq!(canonical_uri(&url), "/");
    }

    #[test]
    fn test_canonical_uri_with_segments() {
        // Encode space as %20, not +
        let url = Url::parse("https://example.com/bucket/key with spaces").unwrap();
        assert_eq!(canonical_uri(&url), "/bucket/key%20with%20spaces");

        // Do not double-encode
        let url = Url::parse("https://example.com/bucket/key%20with%20spaces").unwrap();
        assert_eq!(canonical_uri(&url), "/bucket/key%20with%20spaces");

        // Encode other chars
        let url = Url::parse("https://example.com/bucket/key+file").unwrap();
        assert_eq!(canonical_uri(&url), "/bucket/key%2Bfile");
    }

    #[test]
    fn test_canonical_query_string_empty() {
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(canonical_query_string(&url), "");
    }

    #[test]
    fn test_canonical_query_string_sorted() {
        let url = Url::parse("https://example.com/?z=1&a=2&m=3").unwrap();
        assert_eq!(canonical_query_string(&url), "a=2&m=3&z=1");

        // Sorting is done after encoding
        let url = Url::parse("https://example.com/?a=20&öa=21").unwrap();
        assert_eq!(canonical_query_string(&url), "%C3%B6a=21&a=20");
    }

    #[test]
    fn test_canonical_query_string_encodes_values() {
        let url = Url::parse("https://example.com/?key=hello+world").unwrap();
        assert_eq!(canonical_query_string(&url), "key=hello%20world");
    }

    #[test]
    fn test_canonical_request_list_buckets() {
        let url = Url::parse("https://my-precious-bucket.s3.amazonaws.com/").unwrap();

        let (canonical, signed) = build_canonical_request(
            "GET",
            &url,
            "my-precious-bucket.s3.amazonaws.com",
            EXAMPLE_AMZ_DATE,
            EMPTY_HASH,
            &[],
        );

        let expected = format!(
            "\
GET
/

host:my-precious-bucket.s3.amazonaws.com
x-amz-content-sha256:{EMPTY_HASH}
x-amz-date:{EXAMPLE_AMZ_DATE}

host;x-amz-content-sha256;x-amz-date
{EMPTY_HASH}"
        );

        assert_eq!(canonical, expected);
        assert_eq!(signed, "host;x-amz-content-sha256;x-amz-date");
    }

    #[test]
    fn test_string_to_sign() {
        let url = Url::parse("https://my-precious-bucket.s3.amazonaws.com/").unwrap();
        let (canonical, _) = build_canonical_request(
            "GET",
            &url,
            "my-precious-bucket.s3.amazonaws.com",
            EXAMPLE_AMZ_DATE,
            EMPTY_HASH,
            &[],
        );

        let scope = format!("{EXAMPLE_DATE}/{EXAMPLE_REGION}/s3/aws4_request");
        let sts = build_string_to_sign(EXAMPLE_AMZ_DATE, &scope, &canonical);

        // The canonical request hash from the blog post example
        let canonical_hash = sha256_hex(&canonical);
        let expected = format!("AWS4-HMAC-SHA256\n{EXAMPLE_AMZ_DATE}\n{scope}\n{canonical_hash}");
        assert_eq!(sts, expected);
    }

    #[test]
    fn test_derive_signing_key() {
        // From the blog post: signing key for the example credentials/date/region
        let key = derive_signing_key(EXAMPLE_SECRET, EXAMPLE_DATE, EXAMPLE_REGION);
        assert_eq!(
            hex::encode(&key),
            "7b0b3063e375aa1e25890e0cae1c674785b8d8709cd2bf11ec670b96587650da"
        );
    }

    #[test]
    fn test_full_signature() -> Result<(), Report> {
        // Reproduce the full example signature from the blog post
        let url = Url::parse("https://my-precious-bucket.s3.amazonaws.com/")?;
        let ts: Timestamp = EXAMPLE_AMZ_DATE.parse()?;
        let signing = SigningConfig::new(EXAMPLE_KEY_ID, EXAMPLE_SECRET, EXAMPLE_REGION);

        let signed = signing.sign(SignRequest::new("GET", &url, ts))?;

        assert_eq!(
            signed.get("host").context("expected host header")?,
            "my-precious-bucket.s3.amazonaws.com"
        );
        assert_eq!(
            signed.get("x-amz-date").context("expected x-amz-date")?,
            EXAMPLE_AMZ_DATE
        );

        let expected_auth_credential = format!(
            "Credential=AKIAIOSFODNN7EXAMPLE/{EXAMPLE_DATE}/{EXAMPLE_REGION}/s3/aws4_request"
        );
        let expected_auth_headers = "SignedHeaders=host;x-amz-content-sha256;x-amz-date";
        let expected_auth_signature =
            "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a";
        assert_eq!(
            signed
                .get("authorization")
                .context("expected authorization header")?,
            &format!(
                "AWS4-HMAC-SHA256 {expected_auth_credential}, {expected_auth_headers}, {expected_auth_signature}"
            )
        );

        Ok(())
    }

    #[test]
    fn test_sign_with_query_params() -> Result<(), Report> {
        // Verify query string parameters get included
        let url = Url::parse("https://bucket.s3.amazonaws.com/path?list-type=2&prefix=foo")?;
        let ts: Timestamp = EXAMPLE_AMZ_DATE.parse()?;
        let signing = SigningConfig::new(EXAMPLE_KEY_ID, EXAMPLE_SECRET, EXAMPLE_REGION);

        let signed = signing.sign(SignRequest::new("GET", &url, ts))?;

        assert!(
            !signed
                .get("authorization")
                .context("expected authorization header")?
                .to_str()
                .context("expect string value")?
                .contains(
                    "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a"
                ),
            "signature should change when query parameters are included"
        );

        Ok(())
    }

    #[test]
    fn test_sign_with_port() -> Result<(), Report> {
        let url = Url::parse("https://localhost:9000/bucket")?;
        let ts: Timestamp = EXAMPLE_AMZ_DATE.parse()?;
        let signing = SigningConfig::new(EXAMPLE_KEY_ID, EXAMPLE_SECRET, EXAMPLE_REGION);

        let signed = signing.sign(SignRequest::new("GET", &url, ts))?;

        assert_eq!(
            signed.get("host").context("expected host header")?,
            "localhost:9000"
        );

        Ok(())
    }
}
