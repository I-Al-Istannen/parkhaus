use crate::config::Upstream;
use crate::error::TierError;
use crate::AppState;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use rootcause::prelude::ResultExt;
use rootcause::report;
use tracing::warn;
use url::Url;

pub async fn handle_request(
    State(state): State<AppState>,
    req: Request,
) -> Result<Response, TierError> {
    let upstream = state.config.upstreams.values().next().unwrap();
    if req.uri().path() == "/" {
        // bucket-specific operation, nothing for us to track
        let mut upstream_url = upstream.base_url.clone();
        upstream_url.set_query(req.uri().query());
        return forward_request(&state, upstream, upstream_url, req).await;
    }

    Err(report!("handling request: {} {}", req.method(), req.uri())
        .attach(StatusCode::NOT_ACCEPTABLE)
        .into())
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
