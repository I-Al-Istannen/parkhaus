//! Maps requests onto the subset of S3 parkhaus keeps exact bookkeeping for. Everything else is
//! rejected, so no request can change an object without parkhaus noticing.

use crate::data::S3ObjectId;
use crate::error::TierError;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use percent_encoding::percent_decode_str;
use rootcause::prelude::ResultExt;
use rootcause::report;
use std::collections::BTreeSet;
use url::form_urlencoded;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Classified {
    ListBuckets,
    Bucket { bucket: String, op: BucketOp },
    Object { id: S3ObjectId, op: ObjectOp },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum BucketOp {
    ListObjectsV2,
    DeleteObjects,
    /// Read-only bucket request answered by the hottest upstream.
    Read,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ObjectOp {
    Read,
    /// CORS preflight, answered by the hottest upstream.
    Preflight,
    Put,
    Copy {
        source: S3ObjectId,
    },
    Delete,
    CreateMultipart,
    /// UploadPart, ListParts and AbortMultipartUpload.
    MultipartStep,
    UploadPartCopy {
        source: S3ObjectId,
    },
    CompleteMultipart,
}

impl ObjectOp {
    pub(super) fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Put | Self::Copy { .. } | Self::Delete | Self::CompleteMultipart
        )
    }
}

pub(super) fn classify(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<Classified, TierError> {
    let path = percent_decode_str(uri.path())
        .decode_utf8()
        .context("request path is not valid UTF-8")
        .attach(StatusCode::BAD_REQUEST)?;
    let path = path.strip_prefix('/').unwrap_or(&path);
    let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
    let params = Params::parse(uri.query());

    if is_sse_c(headers) {
        return Err(not_supported("SSE-C"));
    }
    if params.has("versionId") {
        return Err(not_supported("addressing object versions"));
    }

    if bucket.is_empty() {
        return match *method {
            Method::GET | Method::HEAD => Ok(Classified::ListBuckets),
            _ => Err(not_supported("this request")),
        };
    }
    if key.is_empty() {
        return Ok(Classified::Bucket {
            bucket: bucket.to_string(),
            op: classify_bucket(method, &params)?,
        });
    }
    let id = S3ObjectId {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };
    let op = classify_object(method, &params, headers)?;
    Ok(Classified::Object { id, op })
}

fn classify_bucket(method: &Method, params: &Params) -> Result<BucketOp, TierError> {
    const LIST_V1_PARAMS: [&str; 5] =
        ["prefix", "delimiter", "marker", "max-keys", "encoding-type"];

    match *method {
        Method::GET if params.value("list-type") == Some("2") => Ok(BucketOp::ListObjectsV2),
        Method::GET
            if params
                .names
                .iter()
                .all(|it| LIST_V1_PARAMS.contains(&it.as_str())) =>
        {
            Err(not_supported("ListObjects (v1), use ListObjectsV2"))
        }
        Method::GET | Method::HEAD if params.has("versions") => {
            Err(not_supported("ListObjectVersions"))
        }
        Method::GET | Method::HEAD if params.has("uploads") => {
            Err(not_supported("ListMultipartUploads"))
        }
        Method::GET | Method::HEAD | Method::OPTIONS => Ok(BucketOp::Read),
        Method::POST if params.is(&["delete"]) => Ok(BucketOp::DeleteObjects),
        _ => Err(not_supported("modifying buckets")),
    }
}

fn classify_object(
    method: &Method,
    params: &Params,
    headers: &HeaderMap,
) -> Result<ObjectOp, TierError> {
    let copy_source = parse_copy_source(headers)?;
    let op = match *method {
        Method::GET | Method::HEAD if params.has("uploadId") => ObjectOp::MultipartStep,
        Method::GET | Method::HEAD => ObjectOp::Read,
        Method::OPTIONS => ObjectOp::Preflight,
        Method::PUT if params.is(&[]) => match copy_source {
            Some(source) => ObjectOp::Copy { source },
            None => ObjectOp::Put,
        },
        Method::PUT if params.is(&["partNumber", "uploadId"]) => match copy_source {
            Some(source) => ObjectOp::UploadPartCopy { source },
            None => ObjectOp::MultipartStep,
        },
        Method::POST if params.is(&["uploads"]) => ObjectOp::CreateMultipart,
        Method::POST if params.is(&["uploadId"]) => ObjectOp::CompleteMultipart,
        Method::DELETE if params.is(&[]) => ObjectOp::Delete,
        Method::DELETE if params.is(&["uploadId"]) => ObjectOp::MultipartStep,
        _ => return Err(not_supported("this object request")),
    };
    Ok(op)
}

/// Query parameters that select an operation. Presigning parameters, `x-id` and response
/// overrides do not change what a request does.
struct Params {
    names: BTreeSet<String>,
    pairs: Vec<(String, String)>,
}

impl Params {
    fn parse(query: Option<&str>) -> Self {
        let pairs: Vec<(String, String)> =
            form_urlencoded::parse(query.unwrap_or_default().as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .filter(|(k, _)| !is_ignored_param(k))
                .collect();
        let names = pairs.iter().map(|(k, _)| k.clone()).collect();
        Self { names, pairs }
    }

    fn has(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    fn is(&self, expected: &[&str]) -> bool {
        self.names.len() == expected.len() && expected.iter().all(|it| self.has(it))
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

fn is_ignored_param(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("x-amz-")
        || lower.starts_with("response-")
        || matches!(
            lower.as_str(),
            "x-id" | "awsaccesskeyid" | "signature" | "expires"
        )
}

/// The recording HEAD and migrations cannot provide the customer key.
fn is_sse_c(headers: &HeaderMap) -> bool {
    headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-amz-server-side-encryption-customer-")
            || name.starts_with("x-amz-copy-source-server-side-encryption-customer-")
    })
}

/// Parses `x-amz-copy-source` (`[/]bucket/key[?versionId=...]`, URL-encoded).
fn parse_copy_source(headers: &HeaderMap) -> Result<Option<S3ObjectId>, TierError> {
    let Some(value) = headers.get("x-amz-copy-source") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .context("invalid x-amz-copy-source header")
        .attach(StatusCode::BAD_REQUEST)?;
    let (value, query) = value.split_once('?').unwrap_or((value, ""));
    if Params::parse(Some(query)).has("versionId") {
        return Err(not_supported("copying object versions"));
    }
    let decoded = percent_decode_str(value.trim_start_matches('/'))
        .decode_utf8()
        .context("x-amz-copy-source is not valid UTF-8")
        .attach(StatusCode::BAD_REQUEST)?;
    let Some((bucket, key)) = decoded.split_once('/').filter(|(_, key)| !key.is_empty()) else {
        return Err(report!("x-amz-copy-source misses key: '{decoded}'")
            .attach(StatusCode::BAD_REQUEST)
            .into());
    };
    Ok(Some(S3ObjectId {
        bucket: bucket.to_string(),
        key: key.to_string(),
    }))
}

pub(super) fn not_supported(what: &str) -> TierError {
    report!("{what} is not supported")
        .attach(StatusCode::NOT_IMPLEMENTED)
        .into()
}

#[cfg(test)]
mod tests {
    use super::{BucketOp, Classified, ObjectOp, classify};
    use crate::data::S3ObjectId;
    use axum::http::{HeaderMap, HeaderValue, Method, Uri};

    fn id(bucket: &str, key: &str) -> S3ObjectId {
        S3ObjectId {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }
    }

    fn run(method: Method, uri: &str, headers: &[(&'static str, &str)]) -> Option<Classified> {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(*name, HeaderValue::from_str(value).unwrap());
        }
        classify(&method, &uri.parse::<Uri>().unwrap(), &map).ok()
    }

    fn object(method: Method, query: &str) -> Option<ObjectOp> {
        object_with(method, query, &[])
    }

    fn object_with(
        method: Method,
        query: &str,
        headers: &[(&'static str, &str)],
    ) -> Option<ObjectOp> {
        match run(method, &format!("/b/k?{query}"), headers)? {
            Classified::Object { op, .. } => Some(op),
            other => panic!("not an object request: {other:?}"),
        }
    }

    fn bucket(method: Method, query: &str) -> Option<BucketOp> {
        match run(method, &format!("/b?{query}"), &[])? {
            Classified::Bucket { op, .. } => Some(op),
            other => panic!("not a bucket request: {other:?}"),
        }
    }

    #[test]
    fn parses_targets() {
        let bucket = |name: &str| {
            Some(Classified::Bucket {
                bucket: name.to_string(),
                op: BucketOp::Read,
            })
        };
        let object = |key: &str| {
            Some(Classified::Object {
                id: id("b", key),
                op: ObjectOp::Read,
            })
        };
        assert_eq!(run(Method::GET, "/", &[]), Some(Classified::ListBuckets));
        assert_eq!(
            run(Method::GET, "/?max-buckets=1", &[]),
            Some(Classified::ListBuckets)
        );
        assert_eq!(run(Method::HEAD, "/b", &[]), bucket("b"));
        assert_eq!(run(Method::HEAD, "/b/", &[]), bucket("b"));
        assert_eq!(run(Method::GET, "/b/k", &[]), object("k"));
        assert_eq!(run(Method::GET, "/b/k/", &[]), object("k/"));
        assert_eq!(run(Method::GET, "/b/a//c%20d", &[]), object("a//c d"));
        assert_eq!(run(Method::PUT, "/", &[]), None);
    }

    #[test]
    fn classifies_object_requests() {
        use ObjectOp::*;
        let cases = [
            (Method::GET, "", Some(Read)),
            (Method::HEAD, "", Some(Read)),
            (
                Method::GET,
                "partNumber=1&response-content-type=a",
                Some(Read),
            ),
            (Method::GET, "tagging", Some(Read)),
            (Method::GET, "uploadId=u&max-parts=1", Some(MultipartStep)),
            (Method::OPTIONS, "", Some(Preflight)),
            (Method::PUT, "", Some(Put)),
            (Method::PUT, "x-id=PutObject", Some(Put)),
            (
                Method::PUT,
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=s&X-Amz-Expires=60",
                Some(Put),
            ),
            (Method::PUT, "partNumber=1&uploadId=u", Some(MultipartStep)),
            (Method::POST, "uploads", Some(CreateMultipart)),
            (Method::POST, "uploads=", Some(CreateMultipart)),
            (Method::POST, "uploadId=u", Some(CompleteMultipart)),
            (Method::DELETE, "", Some(Delete)),
            (Method::DELETE, "x-id=DeleteObject", Some(Delete)),
            (Method::DELETE, "uploadId=u", Some(MultipartStep)),
            (Method::GET, "versionId=1", None),
            (Method::DELETE, "versionId=1", None),
            (Method::DELETE, "tagging", None),
            (Method::PUT, "tagging", None),
            (Method::PUT, "acl", None),
            (Method::PUT, "retention", None),
            (Method::PUT, "legal-hold", None),
            (Method::PUT, "uploadId=u", None),
            (Method::POST, "", None),
            (Method::POST, "restore", None),
            (Method::POST, "select&select-type=2", None),
            (Method::PATCH, "", None),
        ];
        for (method, query, expected) in cases {
            assert_eq!(object(method.clone(), query), expected, "{method} ?{query}");
        }
    }

    #[test]
    fn classifies_copies() {
        assert_eq!(
            object_with(Method::PUT, "", &[("x-amz-copy-source", "/b/s%20rc?x=1")]),
            Some(ObjectOp::Copy {
                source: id("b", "s rc")
            })
        );
        assert_eq!(
            object_with(
                Method::PUT,
                "partNumber=1&uploadId=u",
                &[("x-amz-copy-source", "b/src")]
            ),
            Some(ObjectOp::UploadPartCopy {
                source: id("b", "src")
            })
        );
        assert_eq!(
            object_with(
                Method::PUT,
                "",
                &[("x-amz-copy-source", "b/src?versionId=1")]
            ),
            None
        );
        assert_eq!(
            object_with(Method::PUT, "", &[("x-amz-copy-source", "b")]),
            None
        );
        assert_eq!(
            object_with(Method::PUT, "", &[("x-amz-copy-source", "b/")]),
            None
        );
    }

    #[test]
    fn rejects_sse_c() {
        let header = |name| object_with(Method::GET, "", &[(name, "AES256")]);
        assert_eq!(
            header("x-amz-server-side-encryption-customer-algorithm"),
            None
        );
        assert_eq!(
            header("x-amz-copy-source-server-side-encryption-customer-algorithm"),
            None
        );
        assert_eq!(header("x-amz-server-side-encryption"), Some(ObjectOp::Read));
    }

    #[test]
    fn classifies_bucket_requests() {
        use BucketOp::*;
        let cases = [
            (Method::GET, "list-type=2", Some(ListObjectsV2)),
            (
                Method::GET,
                "list-type=2&prefix=a&x-id=ListObjectsV2",
                Some(ListObjectsV2),
            ),
            (Method::GET, "", None),
            (Method::GET, "prefix=a&marker=b", None),
            (Method::GET, "x-id=ListObjects", None),
            (Method::GET, "versions", None),
            (Method::GET, "uploads", None),
            (Method::GET, "location", Some(Read)),
            (Method::GET, "cors", Some(Read)),
            (Method::GET, "versioning", Some(Read)),
            (Method::HEAD, "", Some(Read)),
            (Method::OPTIONS, "", Some(Read)),
            (Method::POST, "delete", Some(DeleteObjects)),
            (
                Method::POST,
                "delete&x-id=DeleteObjects",
                Some(DeleteObjects),
            ),
            (Method::POST, "", None),
            (Method::PUT, "", None),
            (Method::PUT, "x-id=CreateBucket", None),
            (Method::PUT, "cors", None),
            (Method::PUT, "lifecycle", None),
            (Method::PUT, "versioning", None),
            (Method::DELETE, "", None),
            (Method::DELETE, "cors", None),
        ];
        for (method, query, expected) in cases {
            assert_eq!(bucket(method.clone(), query), expected, "{method} ?{query}");
        }
    }
}
