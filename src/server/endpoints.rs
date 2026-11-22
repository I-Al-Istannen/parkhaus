use crate::config::Upstream;
use crate::data::S3ObjectId;
use crate::db::{DirtyObject, MultipartUpload};
use crate::error::TierError;
use crate::s3::types::ForwardObjectUrl;
use crate::server::batch_delete::proxy_delete_objects;
use crate::server::classify::{BucketOp, Classified, ObjectOp, classify, not_supported};
use crate::server::listing::proxy_list_objects_v2;
use crate::server::metrics::{
    COUNTER_OBJECT_CREATIONS_TOTAL, COUNTER_OBJECT_DELETIONS_TOTAL,
    COUNTER_UPSTREAM_FALLBACKS_TOTAL, COUNTER_UPSTREAM_FORWARDS_TOTAL,
};
use crate::server::resolve::resolve;
use crate::server::state::AppState;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{OriginalUri, Request, State};
use axum::http::response::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use axum_prometheus::metrics::counter;
use jiff::{Timestamp, Zoned};
use rootcause::Report;
use rootcause::prelude::ResultExt;
use serde::Deserialize;
use std::collections::HashSet;
use tracing::{debug, warn};

pub async fn proxy_request(
    State(state): State<AppState>,
    OriginalUri(original_uri): OriginalUri,
    req: Request,
) -> Result<Response, TierError> {
    debug!(%original_uri, method=%req.method(), "received request for URL");

    match classify(req.method(), &original_uri, req.headers())? {
        Classified::ListBuckets => forward_to_hottest(&state, "", None, req).await,
        Classified::Bucket { bucket, op } => match op {
            BucketOp::ListObjectsV2 => proxy_list_objects_v2(&state, &bucket, req).await,
            BucketOp::DeleteObjects => proxy_delete_objects(&state, &bucket, req).await,
            BucketOp::Read => forward_to_hottest(&state, &bucket, None, req).await,
        },
        Classified::Object { id, op } => proxy_object_request(state, id, op, req).await,
    }
}

async fn forward_to_hottest(
    state: &AppState,
    bucket: &str,
    key: Option<&str>,
    req: Request,
) -> Result<Response, TierError> {
    let upstream = state.config.hottest_upstream();
    let mut url = upstream.format_url(bucket, key)?;
    url.url.set_query(req.uri().query());
    forward_request(state, upstream, url, req, async {}).await
}

async fn proxy_object_request(
    state: AppState,
    object_id: S3ObjectId,
    op: ObjectOp,
    req: Request,
) -> Result<Response, TierError> {
    if op == ObjectOp::Preflight {
        return forward_to_hottest(&state, &object_id.bucket, Some(&object_id.key), req).await;
    }
    if op.is_mutation() {
        // Detached, so a client disconnect cannot cancel the bookkeeping.
        return tokio::spawn(mutate(state, object_id, op, req))
            .await
            .context("mutation task failed")?;
    }

    let upstream = match op.upload_id() {
        Some(upload_id) => upload_upstream(&state, &object_id, upload_id, req.method()).await?,
        None => upstream_for(&state, &object_id, req.method()).await?,
    };
    if let ObjectOp::UploadPartCopy { source, .. } = &op
        && upstream_for(&state, source, req.method()).await?.name != upstream.name
    {
        return Err(not_supported("UploadPartCopy across upstreams"));
    }

    let mut target_url = upstream.format_url(&object_id.bucket, Some(&object_id.key))?;
    target_url.url.set_query(req.uri().query());
    if op == ObjectOp::CreateMultipart {
        // Detached, so a client disconnect cannot lose the upload ID.
        let upstream = upstream.clone();
        return tokio::spawn(async move {
            create_multipart_upload(&state, &upstream, object_id, target_url, req).await
        })
        .await
        .context("multipart creation task failed")?;
    }
    let on_success = async {
        match &op {
            ObjectOp::Read => drop(tokio::spawn(record_access(state.clone(), object_id))),
            ObjectOp::AbortMultipart { upload_id } => {
                if let Err(e) = state.db.remove_upload(upload_id).await {
                    warn!(%e, %upload_id, "failed to forget aborted multipart upload");
                }
            }
            _ => {}
        }
    };
    forward_request(&state, upstream, target_url, req, on_success).await
}

/// Remembers the upstream of the new upload, so later steps reach it even if the object moves.
async fn create_multipart_upload(
    state: &AppState,
    upstream: &Upstream,
    object: S3ObjectId,
    target_url: ForwardObjectUrl,
    req: Request,
) -> Result<Response, TierError> {
    #[derive(Deserialize)]
    struct InitiateMultipartUploadResult {
        #[serde(rename = "UploadId")]
        upload_id: String,
    }

    let response =
        buffer_response(forward_request(state, upstream, target_url, req, async {}).await?).await?;
    if response.succeeded() {
        let result: InitiateMultipartUploadResult = quick_xml::de::from_reader(&response.body[..])
            .context("invalid CreateMultipartUpload response")
            .attach(format!("upstream: {}", upstream.name))?;
        let upload = MultipartUpload {
            upload_id: result.upload_id,
            object,
            upstream: upstream.name.clone(),
        };
        state
            .db
            .add_upload(&upload, Timestamp::now())
            .await
            .context("failed to record multipart upload")?;
    }
    Ok(response.into_response())
}

/// Uploads created before parkhaus tracked them follow the object.
async fn upload_upstream<'a>(
    state: &'a AppState,
    object_id: &S3ObjectId,
    upload_id: &str,
    method: &Method,
) -> Result<&'a Upstream, TierError> {
    let recorded = state
        .db
        .get_upload_upstream(upload_id)
        .await
        .context("failed to get upstream for multipart upload")?
        .and_then(|it| state.config.upstreams.get(&it));
    match recorded {
        Some(upstream) => Ok(upstream),
        None => upstream_for(state, object_id, method).await,
    }
}

/// Marks the object dirty on every upstream the request may change, forwards it and resolves the
/// outcome. If anything fails in between, the dirty marks stay for the sweep.
async fn mutate(
    state: AppState,
    object_id: S3ObjectId,
    op: ObjectOp,
    req: Request,
) -> Result<Response, TierError> {
    let _guard = state.key_locks.lock(&object_id).await;
    resolve(&state.config, &state.db, &state.http, &object_id)
        .await
        .context("failed to resolve earlier mutation")?;

    // CopyObject is executed by the upstream holding the source, the destination follows it.
    let routing_id = match &op {
        ObjectOp::Copy { source } => source,
        _ => &object_id,
    };
    let upstream = match &op {
        ObjectOp::CompleteMultipart { upload_id } => {
            upload_upstream(&state, &object_id, upload_id, req.method()).await?
        }
        _ => upstream_for(&state, routing_id, req.method()).await?,
    };
    let assigned = state
        .db
        .get_upstream(&object_id)
        .await
        .context("failed to get upstream for object")?;
    let dirty = DirtyObject {
        target: upstream.name.clone(),
        previous: assigned,
        migration: false,
    };
    state
        .db
        .mark_dirty(&object_id, &dirty)
        .await
        .context("failed to mark object dirty")?;

    let mut target_url = upstream.format_url(&object_id.bucket, Some(&object_id.key))?;
    target_url.url.set_query(req.uri().query());
    // Without a complete response the upstream may still apply the change, so it stays dirty.
    let response =
        buffer_response(forward_request(&state, upstream, target_url, req, async {}).await?)
            .await?;

    // A rejected request changed nothing. Resolving would mistake a migrated copy for a new write.
    let resolved = if response.parts.status.is_client_error() {
        state.db.clear_dirty(&object_id).await
    } else {
        resolve(&state.config, &state.db, &state.http, &object_id).await
    };
    if let Err(e) = resolved {
        warn!(%e, %object_id, "failed to resolve mutation, leaving it to the sweep");
        // Reads still go to the previous copy, so the change must not look finished yet.
        if response.succeeded() {
            return Err(e
                .context("failed to record mutation")
                .attach(StatusCode::SERVICE_UNAVAILABLE)
                .into());
        }
    }
    if response.succeeded() {
        let name = match op {
            ObjectOp::Delete => COUNTER_OBJECT_DELETIONS_TOTAL,
            _ => COUNTER_OBJECT_CREATIONS_TOTAL,
        };
        counter!(name, "upstream" => upstream.name.0.clone()).increment(1);
        if let ObjectOp::CompleteMultipart { upload_id } = &op
            && let Err(e) = state.db.remove_upload(upload_id).await
        {
            warn!(%e, %upload_id, "failed to forget completed multipart upload");
        }
        if op != ObjectOp::Delete {
            record_access(state.clone(), object_id).await;
        }
    }
    Ok(response.into_response())
}

struct BufferedResponse {
    parts: Parts,
    body: Bytes,
}

impl BufferedResponse {
    /// CompleteMultipartUpload and CopyObject can report errors in a 200 response.
    fn succeeded(&self) -> bool {
        self.parts.status.is_success() && !self.body.windows(7).any(|it| it == b"<Error>")
    }

    fn into_response(self) -> Response {
        Response::from_parts(self.parts, Body::from(self.body))
    }
}

/// Waits for the complete response, as some operations only report errors in the body.
async fn buffer_response(response: Response) -> Result<BufferedResponse, TierError> {
    const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, MAX_BODY_SIZE)
        .await
        .context("failed to read upstream response")?;
    Ok(BufferedResponse { parts, body })
}

async fn upstream_for<'a>(
    state: &'a AppState,
    object_id: &S3ObjectId,
    method: &Method,
) -> Result<&'a Upstream, TierError> {
    let assigned = state
        .db
        .get_upstream(object_id)
        .await
        .context("failed to get upstream for object")?
        .and_then(|it| state.config.upstreams.get(&it));
    if let Some(upstream) = assigned {
        counter!(COUNTER_UPSTREAM_FORWARDS_TOTAL,
            "upstream" => upstream.name.0.clone(),
            "method" => method.to_string()
        )
        .increment(1);
        return Ok(upstream);
    }

    // Unknown objects do not exist, the hottest upstream answers as if for any missing key.
    let fallback = state.config.hottest_upstream();
    counter!(COUNTER_UPSTREAM_FALLBACKS_TOTAL, "method" => method.to_string()).increment(1);
    debug!(?object_id, %fallback.name, "object not found in database, using hottest upstream");
    Ok(fallback)
}

pub(super) async fn forward_request(
    state: &AppState,
    upstream: &Upstream,
    target: ForwardObjectUrl,
    in_req: Request,
    on_success: impl Future<Output = ()>,
) -> Result<Response, TierError> {
    let mut out_req = state
        .http
        .request(in_req.method().clone(), target.url.clone())
        .header(HeaderName::from_static("x-tiering-proxy"), "parkhaus");

    if let Some(host_header) = &target.host_header {
        out_req = out_req.header(reqwest::header::HOST, host_header);
    }

    let connection_headers = connection_header_names(in_req.headers());
    for (name, val) in in_req.headers() {
        if is_hop_by_hop_header(name) || connection_headers.contains(name) {
            continue;
        }
        out_req = out_req.header(name, val);
    }

    out_req = out_req.body(reqwest::Body::wrap_stream(
        in_req.into_body().into_data_stream(),
    ));

    debug!(
        upstream = %upstream.name,
        %target,
        "forwarding request to upstream"
    );

    let in_response = state
        .http
        .execute(
            out_req
                .build()
                .context("failed to build HTTP request")
                .attach(format!("url: {target}"))?,
        )
        .await
        .context("failed to send request")
        .attach(format!("url: {target}"))?;

    let in_resp_status = in_response.status();
    let in_resp_headers = in_response.headers().clone();
    let mut out_response = Response::new(Body::from_stream(in_response.bytes_stream()));
    *out_response.status_mut() = in_resp_status;
    for (name, value) in &in_resp_headers {
        if is_hop_by_hop_header(name) {
            continue;
        }
        out_response.headers_mut().append(name, value.clone());
    }

    // Add a header to indicate which upstream we forwarded to, for debugging purposes
    match HeaderValue::from_bytes(upstream.name.0.as_bytes()) {
        Ok(val) => {
            out_response.headers_mut().append("x-tiering-upstream", val);
        }
        Err(err) => {
            warn!(%err, "failed to set x-tiering-upstream header");
        }
    }

    if in_resp_status.is_success() {
        on_success.await;
    }

    Ok(out_response)
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    // https://datatracker.ietf.org/doc/html/rfc2068#section-13.5.1
    //   - Connection
    //   - Keep-Alive
    //   - Public
    //   - Proxy-Authenticate
    //   - Transfer-Encoding
    //   - Upgrade
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "public"
            | "proxy-authenticate"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn connection_header_names(headers: &HeaderMap) -> HashSet<HeaderName> {
    headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| HeaderName::from_bytes(value.as_bytes()).ok())
        .collect()
}

async fn record_access(state: AppState, object_id: S3ObjectId) {
    let recording = async {
        if state.db.has_object(&object_id).await? {
            state.db.record_access(&object_id, &Zoned::now()).await?;
        }
        Ok::<_, Report>(())
    };
    if let Err(e) = recording.await {
        warn!(%e, %object_id, "failed to record access");
    }
}

#[cfg(test)]
mod tests {
    use super::{connection_header_names, is_hop_by_hop_header};
    use axum::http::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn strips_headers_named_by_connection() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "connection",
            HeaderValue::from_static("keep-alive, x-foo, x-bar"),
        );

        let names = connection_header_names(&headers);

        assert!(names.contains(&HeaderName::from_static("keep-alive")));
        assert!(names.contains(&HeaderName::from_static("x-foo")));
        assert!(names.contains(&HeaderName::from_static("x-bar")));
    }

    #[test]
    fn ignores_invalid_connection_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", HeaderValue::from_static("x-ok, bad token"));

        let names = connection_header_names(&headers);

        assert!(names.contains(&HeaderName::from_static("x-ok")));
        assert_eq!(names.len(), 1);
    }

    #[test]
    fn fixed_hop_by_hop_headers_remain_filtered() {
        assert!(is_hop_by_hop_header(&HeaderName::from_static("connection")));
        assert!(is_hop_by_hop_header(&HeaderName::from_static("upgrade")));
        assert!(!is_hop_by_hop_header(&HeaderName::from_static(
            "x-not-hop-by-hop"
        )));
    }
}
