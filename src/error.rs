use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rootcause::Report;
use tracing::warn;

pub struct TierError {
    report: Report,
}

impl IntoResponse for TierError {
    fn into_response(self) -> Response {
        let status_code = self
            .report
            .iter_reports()
            .flat_map(|node| node.attachments().iter())
            .filter_map(|attachment| attachment.downcast_inner::<StatusCode>())
            .next();
        let error_message = format!("error: {}", self.report);

        if let Some(status_code) = status_code {
            (*status_code, error_message).into_response()
        } else {
            warn!(%self.report, "error without status code during handling");
            (StatusCode::INTERNAL_SERVER_ERROR, error_message).into_response()
        }
    }
}

impl<T: ?Sized> From<Report<T>> for TierError {
    fn from(report: Report<T>) -> Self {
        Self {
            report: report.into_dynamic(),
        }
    }
}
