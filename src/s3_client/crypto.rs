use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use rootcause::Report;
use rootcause::option_ext::OptionExt;
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

/// Result of signing a request with AWS Signature Version 4.
#[derive(Debug, Clone)]
pub struct SignedRequest {
    pub host: String,
    pub amz_date: String,
    pub authorization: String,
}

/// Sign an HTTP request using AWS Signature Version 4 (header-based auth) at the current time.
///
/// See <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>
pub fn sign_v4(
    key_id: &str,
    secret: &str,
    region: &str,
    method: &str,
    url: &Url,
    payload_hash: &str,
) -> Result<SignedRequest, Report> {
    sign_v4_at(
        key_id,
        secret,
        region,
        method,
        url,
        payload_hash,
        jiff::Timestamp::now(),
    )
}

/// Sign an HTTP request at a specific point in time.
pub fn sign_v4_at(
    key_id: &str,
    secret: &str,
    region: &str,
    method: &str,
    url: &Url,
    payload_hash: &str,
    current_time: jiff::Timestamp,
) -> Result<SignedRequest, Report> {
    // Spec: Date must be in ISO 8601 basic format: YYYYMMDD'T'HHMMSS'Z'
    let amz_date = current_time.strftime("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = current_time.strftime("%Y%m%d").to_string();

    let host = url.host_str().context("URL must have a host")?;

    // Include the origin if it is not the default port for the scheme
    let host_header = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };

    // Task 1: Create a Canonical Request
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#canonical-request
    let (canonical_request, signed_headers) =
        build_canonical_request(method, url, &host_header, &amz_date, payload_hash);

    // Task 2: Create a String to Sign
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#request-string
    let scope = format!("{date_stamp}/{region}/s3/aws4_request");
    let string_to_sign = build_string_to_sign(&amz_date, &scope, &canonical_request);

    // Task 3: Calculate Signature
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#signing-key
    // The final signature is the HMAC-SHA256 hash of the string to sign,
    // using the signing key as the key.
    let signing_key = derive_signing_key(secret, &date_stamp, region);
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    // Task 4: Authorization header
    // Spec: Authorization = algorithm Credential=access key ID/credential scope,
    //        SignedHeaders=signed headers, Signature=signature
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={key_id}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    Ok(SignedRequest {
        host: host_header,
        amz_date,
        authorization,
    })
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
) -> (String, String) {
    let canonical_uri = canonical_uri(url);
    let canonical_query = canonical_query_string(url);

    // Individual header name and value pairs are separated by the newline character ("\n").
    // Header names must be in lowercase.
    // You must sort the header names alphabetically to construct the string
    //
    // For S3 you MUST include: host, x-amz-content-sha256, x-amz-date
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");

    // SignedHeaders is an alphabetically sorted, semicolon-separated list
    // of lowercase request header names.
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

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
            utf8_percent_encode(&decoded, SIGV4_ENCODE_SET).to_string()
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
        let ts: jiff::Timestamp = EXAMPLE_AMZ_DATE.parse()?;

        let signed = sign_v4_at(
            EXAMPLE_KEY_ID,
            EXAMPLE_SECRET,
            EXAMPLE_REGION,
            "GET",
            &url,
            EMPTY_HASH,
            ts,
        )?;

        assert_eq!(signed.host, "my-precious-bucket.s3.amazonaws.com");
        assert_eq!(signed.amz_date, EXAMPLE_AMZ_DATE);

        let expected_auth_credential = format!(
            "Credential=AKIAIOSFODNN7EXAMPLE/{EXAMPLE_DATE}/{EXAMPLE_REGION}/s3/aws4_request"
        );
        let expected_auth_headers = "SignedHeaders=host;x-amz-content-sha256;x-amz-date";
        let expected_auth_signature =
            "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a";
        assert_eq!(
            signed.authorization,
            format!(
                "AWS4-HMAC-SHA256 {expected_auth_credential}, {expected_auth_headers}, {expected_auth_signature}"
            )
        );

        Ok(())
    }

    #[test]
    fn test_sign_with_query_params() -> Result<(), Report> {
        // Verify query string parameters get included
        let url = Url::parse("https://bucket.s3.amazonaws.com/path?list-type=2&prefix=foo")?;
        let ts: jiff::Timestamp = EXAMPLE_AMZ_DATE.parse()?;

        let signed = sign_v4_at(
            EXAMPLE_KEY_ID,
            EXAMPLE_SECRET,
            EXAMPLE_REGION,
            "GET",
            &url,
            EMPTY_HASH,
            ts,
        )?;

        assert!(
            !signed.authorization.contains(
                "Signature=182072eb53d85c36b2d791a1fa46a12d23454ec1e921b02075c23aee40166d5a"
            ),
            "signature should change when query parameters are included"
        );

        Ok(())
    }

    #[test]
    fn test_sign_with_port() -> Result<(), Report> {
        let url = Url::parse("https://localhost:9000/bucket")?;
        let ts: jiff::Timestamp = EXAMPLE_AMZ_DATE.parse()?;

        let signed = sign_v4_at(
            EXAMPLE_KEY_ID,
            EXAMPLE_SECRET,
            EXAMPLE_REGION,
            "GET",
            &url,
            EMPTY_HASH,
            ts,
        )?;

        assert_eq!(signed.host, "localhost:9000");

        Ok(())
    }
}
