use crate::error::TierError;
use crate::AppState;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use rootcause::report;

pub async fn handle_request(State(state): State<AppState>, req: Request) -> Result<(), TierError> {
    Err(report!("handling request: {} {}", req.method(), req.uri())
        .attach(StatusCode::NOT_ACCEPTABLE)
        .into())
}
