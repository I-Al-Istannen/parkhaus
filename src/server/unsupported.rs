//! Requests parkhaus rejects because they cannot work across multiple upstreams.

use crate::data::S3ObjectId;
use crate::error::TierError;
use crate::server::endpoints::{ObjectOp, Target, parse_copy_source};
use crate::server::state::AppState;
use axum::extract::Request;
use axum::http::{HeaderMap, Method};
use reqwest::StatusCode;
use rootcause::prelude::ResultExt;
use rootcause::report;

/// Returns the request unchanged if parkhaus can handle it.
pub(super) async fn reject_unsupported(
    state: &AppState,
    target: &Target,
    req: Request,
) -> Result<Request, TierError> {
    if is_sse_c(req.headers()) {
        return Err(not_supported("SSE-C"));
    }
    match target {
        Target::Bucket(_) if is_bucket_creation_or_deletion(&req) => {
            Err(not_supported("creating and deleting buckets"))
        }
        Target::Object(upload) => {
            if let Some(source) = part_copy_source(&req)?
                && is_cross_upstream(state, upload, &source).await?
            {
                return Err(not_supported("UploadPartCopy across upstreams"));
            }
            Ok(req)
        }
        Target::Bucket(_) => Ok(req),
    }
}

fn not_supported(what: &str) -> TierError {
    report!("{what} is not supported")
        .attach(StatusCode::NOT_IMPLEMENTED)
        .into()
}

/// The recording HEAD and migrations cannot provide the customer key.
fn is_sse_c(headers: &HeaderMap) -> bool {
    headers.keys().any(|name| {
        name.as_str()
            .trim_start_matches("x-amz-copy-source-")
            .starts_with("x-amz-server-side-encryption-customer-")
    })
}

/// CreateBucket/DeleteBucket would need to be coordinated across all upstreams.
fn is_bucket_creation_or_deletion(req: &Request) -> bool {
    req.uri().query().is_none_or(str::is_empty)
        && matches!(*req.method(), Method::PUT | Method::DELETE)
}

/// UploadPartCopy: a multipart upload is bound to one upstream, which can only copy parts from
/// its own objects.
fn part_copy_source(req: &Request) -> Result<Option<S3ObjectId>, TierError> {
    let op = ObjectOp::classify(req.method(), req.uri().query());
    if op != ObjectOp::MultipartStep || *req.method() != Method::PUT {
        return Ok(None);
    }
    parse_copy_source(req.headers())
}

async fn is_cross_upstream(
    state: &AppState,
    upload: &S3ObjectId,
    source: &S3ObjectId,
) -> Result<bool, TierError> {
    // Mirrors `get_fallback_upstream`: unknown multipart uploads live on the hottest upstream,
    // unknown copy sources are assumed to be on the coldest.
    let upload_upstream = state
        .db
        .get_upstream(upload)
        .await
        .context("failed to get upstream for multipart upload")?
        .unwrap_or_else(|| state.config.hottest_upstream().name.clone());
    let source_upstream = state
        .db
        .get_upstream(source)
        .await
        .context("failed to get upstream for copy source")?
        .unwrap_or_else(|| state.config.coldest_upstream().name.clone());
    Ok(upload_upstream != source_upstream)
}
