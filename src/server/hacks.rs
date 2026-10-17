//! Workarounds for S3 operations whose object key is not part of the request URL.

use crate::data::S3ObjectId;
use crate::error::TierError;
use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::Method;
use axum::http::header::CONTENT_TYPE;
use futures_util::{StreamExt, TryStreamExt, future, stream};
use reqwest::StatusCode;
use rootcause::prelude::ResultExt;
use rootcause::report;
use std::convert::Infallible;

/// PostObject: a browser-style upload where the key is a form field.
pub(super) fn is_post_object(req: &Request) -> bool {
    *req.method() == Method::POST
        && req.uri().query().is_none_or(str::is_empty)
        && req
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|it| it.to_str().ok())
            .is_some_and(|it| it.starts_with("multipart/form-data"))
}

/// Buffers the form up to the `file` field, which S3 requires to be the last one, to extract the
/// object key. The response does not reliably contain it: the XML body is only sent for
/// `success_action_status=201` and the `Location` header is unspecified (and malformed in Garage).
/// The buffered prefix is replayed in front of the remaining body.
pub(super) async fn read_post_object_key(
    bucket: &str,
    req: Request,
) -> Result<(S3ObjectId, Request), TierError> {
    const MAX_PREFIX_SIZE: usize = 1024 * 1024;

    let boundary = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|it| it.to_str().ok())
        .and_then(|it| multer::parse_boundary(it).ok())
        .ok_or_else(|| report!("missing multipart boundary").attach(StatusCode::BAD_REQUEST))?;

    let (parts, body) = req.into_parts();
    let mut body = body.into_data_stream();
    let mut prefix = Vec::new();
    let key = loop {
        if let Some(key) = form_key(Bytes::copy_from_slice(&prefix), &boundary).await {
            break key;
        }
        let Some(chunk) = body
            .try_next()
            .await
            .context("failed to read form upload")
            .attach(StatusCode::BAD_REQUEST)?
        else {
            return Err(report!("form upload has no key or file field")
                .attach(StatusCode::BAD_REQUEST)
                .into());
        };
        prefix.extend_from_slice(&chunk);
        if prefix.len() > MAX_PREFIX_SIZE {
            return Err(
                report!("form fields before the file exceed {MAX_PREFIX_SIZE} bytes")
                    .attach(StatusCode::BAD_REQUEST)
                    .into(),
            );
        }
    };

    let body = stream::once(future::ready(Ok(Bytes::from(prefix)))).chain(body);
    let object_id = S3ObjectId {
        bucket: bucket.to_string(),
        key,
    };
    Ok((
        object_id,
        Request::from_parts(parts, Body::from_stream(body)),
    ))
}

/// Returns the key once the form prefix contains the `key` field and the `file` field's headers.
async fn form_key(prefix: Bytes, boundary: &str) -> Option<String> {
    let stream = stream::once(future::ready(Ok::<_, Infallible>(prefix)));
    let mut form = multer::Multipart::new(stream, boundary);
    let mut key = None;
    while let Ok(Some(field)) = form.next_field().await {
        let name = field.name().unwrap_or_default();
        if name.eq_ignore_ascii_case("file") {
            let file_name = field.file_name().unwrap_or_default();
            return key.map(|it: String| it.replace("${filename}", file_name));
        }
        if name.eq_ignore_ascii_case("key") {
            key = Some(field.text().await.ok()?);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::form_key;
    use axum::body::Bytes;

    #[tokio::test]
    async fn extracts_post_object_key() {
        let form = |key_field: &str| {
            format!(
                "--b\r\nContent-Disposition: form-data; name=\"Policy\"\r\n\r\nabc\r\n\
                 {key_field}\
                 --b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"up load.bin\"\r\n\r\n\
                 file contents"
            )
        };
        let key = |value: &str| {
            format!("--b\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\n{value}\r\n")
        };
        let parse = |body: String| form_key(Bytes::from(body), "b");

        assert_eq!(parse(form(&key("a/b c"))).await.as_deref(), Some("a/b c"));
        assert_eq!(
            parse(form(&key("dir/${filename}"))).await.as_deref(),
            Some("dir/up load.bin")
        );
        assert_eq!(parse(form("")).await, None);

        let complete = form(&key("a"));
        let truncated = &complete[..complete.find("name=\"file\"").unwrap()];
        assert_eq!(parse(truncated.to_string()).await, None);
    }
}
