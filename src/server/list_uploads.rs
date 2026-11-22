//! ListMultipartUploads, answered from the database, which knows every upload in progress.

use crate::db::MultipartUpload;
use crate::error::TierError;
use crate::server::listing::query_param;
use crate::server::state::AppState;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use jiff::Timestamp;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use quick_xml::escape::escape;
use rootcause::prelude::ResultExt;
use std::fmt::Write;

const MAX_UPLOADS: usize = 1000;
/// What S3 leaves unencoded with `encoding-type=url`.
const URL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

#[derive(Debug, Default)]
struct ListUploadsParams {
    prefix: String,
    delimiter: Option<String>,
    key_marker: String,
    upload_id_marker: Option<String>,
    max_uploads: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum Listed<'a> {
    Upload(&'a MultipartUpload, Timestamp),
    Prefix(&'a str),
}

/// Selects the page the parameters ask for from uploads ordered by key and upload ID, and whether
/// more follow.
fn select<'a>(
    uploads: &'a [(MultipartUpload, Timestamp)],
    params: &ListUploadsParams,
) -> (Vec<Listed<'a>>, bool) {
    let marker = params.key_marker.as_str();
    let mut page = Vec::new();
    for (upload, created) in uploads {
        let key = upload.object.key.as_str();
        let after_marker = match &params.upload_id_marker {
            Some(id) if !marker.is_empty() && key == marker => upload.upload_id > *id,
            _ => key > marker,
        };
        if !key.starts_with(&params.prefix) || !after_marker {
            continue;
        }
        let rest = &key[params.prefix.len()..];
        let common = params
            .delimiter
            .as_deref()
            .filter(|it| !it.is_empty())
            .and_then(|delimiter| rest.find(delimiter).map(|i| i + delimiter.len()))
            .map(|end| &key[..params.prefix.len() + end]);
        let listed = match common {
            // Handed out on an earlier page, which ended on it
            Some(prefix) if prefix <= marker => continue,
            Some(prefix) if page.last() == Some(&Listed::Prefix(prefix)) => continue,
            Some(prefix) => Listed::Prefix(prefix),
            None => Listed::Upload(upload, *created),
        };
        if page.len() == params.max_uploads {
            return (page, true);
        }
        page.push(listed);
    }
    (page, false)
}

/// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
pub(super) async fn list_multipart_uploads(
    state: &AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, TierError> {
    let max_uploads = match query_param(&req, "max-uploads") {
        None => MAX_UPLOADS,
        Some(it) => it
            .parse::<usize>()
            .context("max-uploads is not a number")
            .attach(StatusCode::BAD_REQUEST)?
            .min(MAX_UPLOADS),
    };
    let params = ListUploadsParams {
        prefix: query_param(&req, "prefix").unwrap_or_default(),
        delimiter: query_param(&req, "delimiter"),
        key_marker: query_param(&req, "key-marker").unwrap_or_default(),
        upload_id_marker: query_param(&req, "upload-id-marker"),
        max_uploads,
    };
    let url_encoded = query_param(&req, "encoding-type").as_deref() == Some("url");
    let uploads = state.db.get_uploads_in_bucket(bucket).await?;
    let (page, truncated) = select(&uploads, &params);
    let body = render(bucket, &params, &page, truncated, url_encoded);
    Ok(([(CONTENT_TYPE, "application/xml")], body).into_response())
}

fn render(
    bucket: &str,
    params: &ListUploadsParams,
    page: &[Listed<'_>],
    truncated: bool,
    url_encoded: bool,
) -> String {
    let encode = |it: &str| {
        if url_encoded {
            utf8_percent_encode(it, URL_ENCODE_SET).to_string()
        } else {
            escape(it).into_owned()
        }
    };
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    let _ = write!(
        out,
        "<Bucket>{}</Bucket><KeyMarker>{}</KeyMarker><UploadIdMarker>{}</UploadIdMarker>\
         <Prefix>{}</Prefix><MaxUploads>{}</MaxUploads><IsTruncated>{truncated}</IsTruncated>",
        escape(bucket),
        encode(&params.key_marker),
        escape(params.upload_id_marker.as_deref().unwrap_or_default()),
        encode(&params.prefix),
        params.max_uploads,
    );
    if let Some(delimiter) = &params.delimiter {
        let _ = write!(out, "<Delimiter>{}</Delimiter>", encode(delimiter));
    }
    if url_encoded {
        out.push_str("<EncodingType>url</EncodingType>");
    }
    if truncated {
        let (key, upload_id) = match page.last() {
            Some(Listed::Upload(upload, _)) => {
                (upload.object.key.as_str(), upload.upload_id.as_str())
            }
            Some(Listed::Prefix(prefix)) => (*prefix, ""),
            None => ("", ""),
        };
        let _ = write!(
            out,
            "<NextKeyMarker>{}</NextKeyMarker><NextUploadIdMarker>{}</NextUploadIdMarker>",
            encode(key),
            escape(upload_id)
        );
    }
    for listed in page {
        match listed {
            Listed::Upload(upload, created) => {
                let _ = write!(
                    out,
                    "<Upload><Key>{}</Key><UploadId>{}</UploadId>\
                     <StorageClass>STANDARD</StorageClass><Initiated>{}</Initiated></Upload>",
                    encode(&upload.object.key),
                    escape(&upload.upload_id),
                    created.strftime("%Y-%m-%dT%H:%M:%S%.3fZ"),
                );
            }
            Listed::Prefix(prefix) => {
                let _ = write!(
                    out,
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    encode(prefix)
                );
            }
        }
    }
    out.push_str("</ListMultipartUploadsResult>");
    out
}

#[cfg(test)]
mod tests {
    use super::{ListUploadsParams, Listed, select};
    use crate::data::{S3ObjectId, UpstreamId};
    use crate::db::MultipartUpload;
    use jiff::Timestamp;

    fn uploads(entries: &[(&str, &str)]) -> Vec<(MultipartUpload, Timestamp)> {
        entries
            .iter()
            .map(|(key, id)| {
                let upload = MultipartUpload {
                    upload_id: id.to_string(),
                    object: S3ObjectId {
                        bucket: "b".to_string(),
                        key: key.to_string(),
                    },
                    upstream: UpstreamId("hot".to_string()),
                };
                (upload, Timestamp::UNIX_EPOCH)
            })
            .collect()
    }

    fn names(page: &[Listed<'_>]) -> Vec<String> {
        page.iter()
            .map(|it| match it {
                Listed::Upload(upload, _) => format!("{}#{}", upload.object.key, upload.upload_id),
                Listed::Prefix(prefix) => prefix.to_string(),
            })
            .collect()
    }

    fn params(max_uploads: usize) -> ListUploadsParams {
        ListUploadsParams {
            max_uploads,
            ..Default::default()
        }
    }

    #[test]
    fn pages_by_key_and_upload_id_markers() {
        let all = uploads(&[("a", "1"), ("a", "2"), ("b", "1")]);
        let (page, truncated) = select(&all, &params(2));
        assert_eq!(names(&page), ["a#1", "a#2"]);
        assert!(truncated);

        let next = ListUploadsParams {
            key_marker: "a".to_string(),
            upload_id_marker: Some("1".to_string()),
            ..params(2)
        };
        let (page, truncated) = select(&all, &next);
        assert_eq!(names(&page), ["a#2", "b#1"]);
        assert!(!truncated);

        let without_id = ListUploadsParams {
            key_marker: "a".to_string(),
            ..params(2)
        };
        assert_eq!(names(&select(&all, &without_id).0), ["b#1"]);
    }

    #[test]
    fn groups_common_prefixes_once() {
        let all = uploads(&[("p/a/1", "1"), ("p/a/2", "1"), ("p/b", "1"), ("q", "1")]);
        let grouped = ListUploadsParams {
            prefix: "p/".to_string(),
            delimiter: Some("/".to_string()),
            ..params(1)
        };
        let (page, truncated) = select(&all, &grouped);
        assert_eq!(names(&page), ["p/a/"]);
        assert!(truncated);

        let next = ListUploadsParams {
            key_marker: "p/a/".to_string(),
            ..grouped
        };
        let (page, truncated) = select(&all, &next);
        assert_eq!(names(&page), ["p/b#1"]);
        assert!(!truncated);
    }
}
