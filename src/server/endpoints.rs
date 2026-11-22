use crate::config::Upstream;
use crate::data::{S3Object, S3ObjectId, UpstreamId};
use crate::error::TierError;
use crate::s3::client::S3Client;
use crate::s3::types::ForwardObjectUrl;
use crate::server::batch_delete::proxy_delete_objects;
use crate::server::classify::{BucketOp, Classified, ObjectOp, classify, not_supported};
use crate::server::listing::proxy_list_objects_v2;
use crate::server::metrics::{
    COUNTER_OBJECT_CREATIONS_TOTAL, COUNTER_OBJECT_DELETIONS_TOTAL,
    COUNTER_OBJECT_IMPORTED_ON_THE_FLY, COUNTER_UPSTREAM_FALLBACKS_TOTAL,
    COUNTER_UPSTREAM_FORWARDS_TOTAL,
};
use crate::server::state::{AppState, MutationLockGuard};
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use axum::response::Response;
use axum_prometheus::metrics::counter;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
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

    let mutation_guard = state
        .migration_locks
        .wait_for_migration(req.method(), &object_id)
        .await;

    // CopyObject is executed by the upstream holding the source, the destination follows it.
    let routing_id = match &op {
        ObjectOp::Copy { source } => source,
        _ => &object_id,
    };
    let upstream = upstream_for(&state, routing_id, &op, req.method()).await?;

    if let ObjectOp::UploadPartCopy { source } = &op
        && upstream_for(&state, source, &op, req.method()).await?.name != upstream.name
    {
        return Err(not_supported("UploadPartCopy across upstreams"));
    }

    let stale_upstream = match &op {
        ObjectOp::Copy { .. } => state
            .db
            .get_upstream(&object_id)
            .await
            .context("failed to get upstream for copy destination")?
            .filter(|it| *it != upstream.name),
        _ => None,
    };

    let mut target_url = upstream.format_url(&object_id.bucket, Some(&object_id.key))?;
    target_url.url.set_query(req.uri().query());
    let on_success = record_successful_request(
        op,
        object_id,
        state.clone(),
        upstream.name.clone(),
        stale_upstream,
        mutation_guard,
    );
    forward_request(&state, upstream, target_url, req, on_success).await
}

async fn upstream_for<'a>(
    state: &'a AppState,
    object_id: &S3ObjectId,
    op: &ObjectOp,
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

    let fallback = match op {
        ObjectOp::Read | ObjectOp::Delete | ObjectOp::Copy { .. } => {
            state.config.coldest_upstream()
        }
        _ => state.config.hottest_upstream(),
    };
    counter!(COUNTER_UPSTREAM_FALLBACKS_TOTAL, "method" => method.to_string()).increment(1);
    debug!(?object_id, %fallback.name, "object not found in database, defaulting upstream");
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

async fn record_successful_request(
    op: ObjectOp,
    obj_id: S3ObjectId,
    state: AppState,
    upstream_name: UpstreamId,
    stale_upstream: Option<UpstreamId>,
    mutation_guard: MutationLockGuard,
) {
    let is_mutation = op.is_mutation();
    let recording = async move {
        let _mutation_guard = mutation_guard;
        if let Err(e) = record(
            &op,
            &obj_id,
            &state,
            &upstream_name,
            stale_upstream.as_ref(),
        )
        .await
        {
            warn!(%e, object_id=?obj_id, "failed to record object creation/deletion");
        }
    };
    // The client's next request must be routed according to its writes and deletes.
    if is_mutation {
        recording.await;
    } else {
        drop(tokio::spawn(recording));
    }
}

async fn record(
    op: &ObjectOp,
    obj_id: &S3ObjectId,
    state: &AppState,
    upstream_name: &UpstreamId,
    stale_upstream: Option<&UpstreamId>,
) -> Result<(), Report> {
    match op {
        ObjectOp::Delete => {
            counter!(COUNTER_OBJECT_DELETIONS_TOTAL, "upstream" => upstream_name.0.clone())
                .increment(1);
            state
                .db
                .delete_object(obj_id)
                .await
                .context("failed to record deletion")
                .attach(format!("object: {obj_id:?}"))?;
            return Ok(());
        }
        ObjectOp::Put | ObjectOp::Copy { .. } | ObjectOp::CompleteMultipart => {
            counter!(COUNTER_OBJECT_CREATIONS_TOTAL, "upstream" => upstream_name.0.clone())
                .increment(1);
            record_from_upstream(state, upstream_name, obj_id)
                .await
                .context("failed to record creation")
                .attach(format!("object: {obj_id:?}"))?;
            if let Some(stale) = stale_upstream {
                delete_from_upstream(state, stale, obj_id)
                    .await
                    .context("failed to delete overwritten object")
                    .attach(format!("object: {obj_id:?}"))?;
            }
        }
        ObjectOp::Read => {
            if !state
                .db
                .has_object(obj_id)
                .await
                .context("failed to check object existence")?
            {
                record_from_upstream(state, upstream_name, obj_id)
                    .await
                    .context("failed to import object on the fly")
                    .attach(format!("object: {obj_id:?}"))?;
                counter!(COUNTER_OBJECT_IMPORTED_ON_THE_FLY, "upstream" => upstream_name.0.clone())
                    .increment(1);
            }
        }
        ObjectOp::Preflight
        | ObjectOp::CreateMultipart
        | ObjectOp::MultipartStep
        | ObjectOp::UploadPartCopy { .. } => return Ok(()),
    }

    state
        .db
        .record_access(obj_id, &jiff::Zoned::now())
        .await
        .context("failed to record access")
        .attach(format!("object: {obj_id:?}"))?;

    Ok(())
}

async fn delete_from_upstream(
    state: &AppState,
    upstream_name: &UpstreamId,
    obj_id: &S3ObjectId,
) -> Result<(), Report> {
    let upstream = state.config.upstreams.get(upstream_name).ok_or_else(|| {
        report!("upstream not found in config").attach(format!("upstream: '{upstream_name:?}'"))
    })?;
    S3Client::for_upstream(state.http.clone(), upstream)
        .delete_file(obj_id)
        .await
        .context("failed to DELETE file")
        .attach(format!("upstream: '{upstream_name:?}'"))?;
    Ok(())
}

async fn record_from_upstream(
    state: &AppState,
    upstream_name: &UpstreamId,
    obj_id: &S3ObjectId,
) -> Result<(), Report> {
    let upstream = state.config.upstreams.get(upstream_name).ok_or_else(|| {
        report!("upstream not found in config").attach(format!("upstream: '{upstream_name:?}'"))
    })?;
    let metadata = S3Client::for_upstream(state.http.clone(), upstream)
        .head_file(obj_id)
        .await
        .context("failed to HEAD file")
        .attach(format!("object: {obj_id:?}"))?;
    state
        .db
        .record_creation(&S3Object {
            id: obj_id.clone(),
            assigned_upstream: upstream_name.clone(),
            last_modified: metadata.last_modified,
            size: metadata.size,
        })
        .await
        .context("failed to record object")?;

    Ok(())
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
