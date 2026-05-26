use crate::config::{Config, Upstream};
use crate::data::{S3Object, S3ObjectId, UpstreamId};
use crate::error::TierError;
use crate::s3::client::S3Client;
use crate::s3::types::ForwardObjectUrl;
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
use futures_util::TryStreamExt;
use reqwest::StatusCode;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

const fn nop(_size: u64) {}

pub async fn proxy_request(
    State(state): State<AppState>,
    OriginalUri(original_uri): OriginalUri,
    req: Request,
) -> Result<Response, TierError> {
    debug!(%original_uri, method=%req.method(), "received request for URL");

    if req.uri().path().chars().filter(|&it| it == '/').count() == 1 {
        // bucket-specific operation, nothing for us to track
        let upstream = state.config.hottest_upstream();
        let bucket = req.uri().path().trim_start_matches('/');
        debug!(%original_uri, method = %req.method(), %bucket, "handling bucket-level request for URL");

        let mut upstream_url = upstream.format_url(bucket, None)?;
        upstream_url.url.set_query(req.uri().query());
        return forward_request(&state, upstream, upstream_url, req, nop).await;
    }

    debug!(foo=%original_uri, method=%req.method(), "handling request for URL");
    let Some((bucket, key)) = original_uri.path().trim_start_matches('/').split_once('/') else {
        return Err(report!("url misses bucket: '{original_uri}'")
            .attach(StatusCode::BAD_REQUEST)
            .into());
    };

    let object_id = S3ObjectId {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };
    let mutation_guard = state
        .migration_locks
        .wait_for_migration(req.method(), &object_id)
        .await;

    let upstream = state
        .db
        .get_upstream(&object_id)
        .await
        .context("failed to get upstream for object")?
        .and_then(|it| {
            counter!(COUNTER_UPSTREAM_FORWARDS_TOTAL,
                "upstream" => it.0.clone(),
                "method" => req.method().to_string()
            )
            .increment(1);
            state.config.upstreams.get(&it)
        })
        // default to the coldest upstream
        .unwrap_or_else(|| get_fallback_upstream(&object_id, &state.config, &req));

    let on_success = record_successful_request(
        req.method().clone(),
        object_id.clone(),
        state.clone(),
        upstream.name.clone(),
        mutation_guard,
    );
    let mut target_url = upstream.format_url(&object_id.bucket, Some(&object_id.key))?;
    target_url.url.set_query(req.uri().query());
    forward_request(&state, upstream, target_url, req, on_success).await
}

fn get_fallback_upstream<'a>(
    object_id: &'_ S3ObjectId,
    config: &'a Config,
    request: &'_ Request,
) -> &'a Upstream {
    let coldest = if is_creation(request.method()) {
        config.hottest_upstream()
    } else {
        config.coldest_upstream()
    };

    counter!(COUNTER_UPSTREAM_FALLBACKS_TOTAL, "method" => request.method().to_string())
        .increment(1);
    debug!(
        ?object_id,
        %coldest.name,
        "object not found in database, defaulting upstream"
    );

    coldest
}

async fn forward_request(
    state: &AppState,
    upstream: &Upstream,
    target: ForwardObjectUrl,
    in_req: Request,
    on_success: impl FnOnce(u64),
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

    let request_size = Arc::new(AtomicU64::new(0));
    let streamed_request_size = Arc::clone(&request_size);
    let request_body = in_req.into_body().into_data_stream().map_ok(move |chunk| {
        streamed_request_size.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        chunk
    });

    out_req = out_req.body(reqwest::Body::wrap_stream(request_body));

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
        on_success(request_size.load(Ordering::Relaxed));
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

fn record_successful_request(
    req_method: Method,
    obj_id: S3ObjectId,
    state: AppState,
    upstream_name: UpstreamId,
    mutation_guard: MutationLockGuard,
) -> impl FnOnce(u64) {
    move |size| {
        let obj_id_clone = obj_id.clone();
        let recording = async move {
            let _mutation_guard = mutation_guard;
            if is_delete(&req_method) {
                counter!(COUNTER_OBJECT_DELETIONS_TOTAL, "upstream" => upstream_name.0.clone())
                    .increment(1);
                state
                    .db
                    .delete_object(&obj_id_clone)
                    .await
                    .context("failed to record deletion")
                    .attach(format!("object: {obj_id_clone:?}"))?;
                return Ok::<_, Report>(());
            }

            let now = jiff::Zoned::now();

            if is_creation(&req_method) {
                counter!(COUNTER_OBJECT_CREATIONS_TOTAL, "upstream" => upstream_name.0.clone())
                    .increment(1);
                state
                    .db
                    .record_creation(&S3Object {
                        id: obj_id_clone.clone(),
                        assigned_upstream: upstream_name.clone(),
                        last_modified: now.timestamp(),
                        size,
                    })
                    .await
                    .context("failed to record creation")
                    .attach(format!("object: {obj_id_clone:?}"))?;
            } else if !state
                .db
                .has_object(&obj_id_clone)
                .await
                .context("failed to check object existence")?
            {
                import_on_the_fly(&state, &upstream_name, &obj_id_clone)
                    .await
                    .context("failed to import object on the fly")
                    .attach(format!("object: {obj_id_clone:?}"))?;
            }

            state
                .db
                .record_access(&obj_id_clone, &now)
                .await
                .context("failed to record access")
                .attach(format!("object: {obj_id_clone:?}"))?;

            Ok(())
        };

        tokio::spawn(async move {
            if let Err(e) = recording.await {
                warn!(
                    %e,
                    object_id=?obj_id,
                    "failed to record object creation/deletion"
                );
            }
        });
    }
}

async fn import_on_the_fly(
    state: &AppState,
    upstream_name: &UpstreamId,
    obj_id_clone: &S3ObjectId,
) -> Result<(), Report> {
    let upstream = state.config.upstreams.get(upstream_name).ok_or_else(|| {
        report!("upstream not found in config").attach(format!("upstream: '{upstream_name:?}'"))
    })?;
    let s3_client = S3Client::for_upstream(state.http.clone(), upstream);
    let metadata = s3_client
        .head_file(obj_id_clone)
        .await
        .context("failed to HEAD file")
        .attach(format!("object: {obj_id_clone:?}"))?;
    state
        .db
        .record_creation(&S3Object {
            id: obj_id_clone.clone(),
            assigned_upstream: upstream_name.clone(),
            last_modified: metadata.last_modified,
            size: metadata.size,
        })
        .await
        .context("failed to record creation of previously unknown object")?;

    counter!(COUNTER_OBJECT_IMPORTED_ON_THE_FLY, "upstream" => upstream_name.0.clone())
        .increment(1);

    Ok(())
}

fn is_creation(method: &Method) -> bool {
    method == Method::PUT
}

fn is_delete(method: &Method) -> bool {
    method == Method::DELETE
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
