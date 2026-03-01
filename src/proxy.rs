use crate::config::Upstream;
use crate::data::{S3Object, S3ObjectId};
use crate::error::TierError;
use crate::AppState;
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;
use rootcause::prelude::ResultExt;
use rootcause::report;
use tracing::{debug, info, trace, warn};
use url::Url;

pub async fn handle_request(
    State(state): State<AppState>,
    OriginalUri(original_uri): OriginalUri,
    req: Request,
) -> Result<Response, TierError> {
    debug!(%original_uri, method=%req.method(), "received request for URL");

    if req.uri().path().chars().filter(|&it| it == '/').count() == 1 {
        // bucket-specific operation, nothing for us to track
        let upstream = state.config.hottest_upstream();
        debug!(%original_uri, method=%req.method(), "handling bucket-level request for URL");
        let mut upstream_url = upstream.base_url.clone();
        upstream_url.set_query(req.uri().query());
        upstream_url.set_path(original_uri.path());
        return forward_request(&state, upstream, upstream_url, req).await;
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

    let upstream = state
        .db
        .get_upstream(&object_id)
        .await
        .context("failed to get upstream for object")?
        .and_then(|it| state.config.upstreams.get(&it))
        // default to the coldest upstream
        .unwrap_or_else(|| {
            let coldest = state.config.coldest_upstream();
            debug!(
                ?object_id,
                %coldest.name,
                "object not found in database, defaulting to coldest upstream"
            );
            coldest
        });

    // TODO: Only do this if the result is 200?
    if req.method() == Method::PUT {
        state
            .db
            .record_creation(&S3Object {
                id: object_id.clone(),
                assigned_upstream: upstream.name.clone(),
                last_modified: jiff::Timestamp::now(),
            })
            .await
            .context("could not record creation")
            .attach(format!("object: {object_id:?}"))?;
    } else if req.method() == Method::DELETE {
        state
            .db
            .delete_object(&object_id)
            .await
            .context("could not record deletion")
            .attach(format!("object: {object_id:?}"))?;
    }

    forward_request(&state, upstream, upstream.format_url(&object_id), req).await
}

async fn forward_request(
    state: &AppState,
    upstream: &Upstream,
    target: Url,
    in_req: Request,
) -> Result<Response, TierError> {
    let mut out_req = state.http.request(in_req.method().clone(), target.clone());
    for (name, val) in in_req.headers() {
        if is_hop_by_hop_header(name) {
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
