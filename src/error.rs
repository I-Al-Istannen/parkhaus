use crate::policy::expr::AnnotatedError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rootcause::handlers::{ContextFormattingStyle, FormattingFunction};
use rootcause::hooks::context_formatter::ContextFormatterHook;
use rootcause::markers::{Dynamic, Local, Uncloneable};
use rootcause::{Report, ReportRef};
use std::fmt::Formatter;
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
