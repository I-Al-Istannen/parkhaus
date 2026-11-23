use crate::policy::expr::AnnotatedError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use quick_xml::escape::escape;
use rootcause::handlers::{ContextFormattingStyle, FormattingFunction};
use rootcause::hooks::context_formatter::ContextFormatterHook;
use rootcause::markers::{Dynamic, Local, Uncloneable};
use rootcause::{Report, ReportRef};
use std::fmt::Formatter;
use tracing::error;

const X_AMZ_REQUEST_ID: &str = "x-amz-request-id";

pub struct TierError {
    report: Report,
}

impl TierError {
    /// The status attached to the error and the message of the report it is attached to, if any.
    fn status_and_message(&self) -> Option<(StatusCode, String)> {
        self.report.iter_reports().find_map(|node| {
            node.attachments()
                .iter()
                .find_map(|it| it.downcast_inner::<StatusCode>())
                .map(|status| (*status, node.format_current_context().to_string()))
        })
    }

    /// The message of an error answered with `501 Not Implemented`.
    pub fn not_implemented_message(&self) -> Option<String> {
        self.status_and_message()
            .filter(|(status, _)| *status == StatusCode::NOT_IMPLEMENTED)
            .map(|(_, message)| message)
    }
}

impl IntoResponse for TierError {
    /// Answers with an S3 error document. Only errors with a status carry their message to the
    /// client, everything else is logged and reported as an internal error.
    fn into_response(self) -> Response {
        let request_id = format!("{:016x}", rand::random::<u64>());
        let (status, message) = self.status_and_message().unwrap_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "We encountered an internal error. Please try again.".to_string(),
            )
        });
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            error!(%request_id, report = %self.report, "Failed to handle request");
        }

        let code = match status {
            StatusCode::BAD_REQUEST => "InvalidRequest",
            StatusCode::NOT_FOUND => "NoSuchKey",
            StatusCode::NOT_IMPLEMENTED => "NotImplemented",
            StatusCode::SERVICE_UNAVAILABLE => "ServiceUnavailable",
            _ => "InternalError",
        };
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
             <Message>{}</Message><RequestId>{request_id}</RequestId></Error>",
            escape(&message)
        );
        (
            status,
            [
                ("content-type", "application/xml"),
                (X_AMZ_REQUEST_ID, request_id.as_str()),
            ],
            body,
        )
            .into_response()
    }
}

impl<T: ?Sized> From<Report<T>> for TierError {
    fn from(report: Report<T>) -> Self {
        Self {
            report: report.into_dynamic(),
        }
    }
}

pub struct ReqwestErrorFormatter;

impl ContextFormatterHook<reqwest::Error> for ReqwestErrorFormatter {
    fn preferred_context_formatting_style(
        &self,
        _report: ReportRef<'_, Dynamic, Uncloneable, Local>,
        report_formatting_function: FormattingFunction,
    ) -> ContextFormattingStyle {
        ContextFormattingStyle {
            function: report_formatting_function,
            follow_source: true,
            follow_source_depth: None,
        }
    }
}

pub struct AriadneErrorFormatter;

impl ContextFormatterHook<AnnotatedError> for AriadneErrorFormatter {
    fn display(
        &self,
        report: ReportRef<'_, AnnotatedError, Uncloneable, Local>,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        let val = report
            .current_context()
            .format()
            .map_err(|_| std::fmt::Error)?;
        formatter.write_str(&val)
    }
}

#[cfg(test)]
mod tests {
    use super::TierError;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use rootcause::prelude::ResultExt;
    use rootcause::report;

    async fn render(error: TierError) -> (StatusCode, String) {
        let response = error.into_response();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn client_errors_carry_their_message() {
        let error: TierError = report!("versionId <x> is not supported")
            .attach(StatusCode::NOT_IMPLEMENTED)
            .into();
        let (status, body) = render(error).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body.contains("<Code>NotImplemented</Code>"), "{body}");
        assert!(
            body.contains("<Message>versionId &lt;x&gt; is not supported</Message>"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn internal_errors_hide_details() {
        let error: TierError = Err::<(), _>(report!("secret upstream http://10.0.0.1"))
            .context("failed to forward")
            .unwrap_err()
            .into();
        let (status, body) = render(error).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("<Code>InternalError</Code>"), "{body}");
        assert!(!body.contains("10.0.0.1"), "{body}");
    }
}
