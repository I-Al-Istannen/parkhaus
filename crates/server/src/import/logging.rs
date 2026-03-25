use dialoguer::console::style;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use std::env;
use tracing::Subscriber;
use tracing::field::Visit;
use tracing_indicatif::style::ProgressStyle;
use tracing_indicatif::{IndicatifLayer, IndicatifWriter};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

pub struct DimmedFieldNames;

impl<'writer> FormatFields<'writer> for DimmedFieldNames {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        let mut visitor = DimmedFieldVisitor::new(writer);
        fields.record(&mut visitor);
        Ok(())
    }
}

struct DimmedFieldVisitor<'writer> {
    writer: Writer<'writer>,
}

impl<'writer> DimmedFieldVisitor<'writer> {
    fn new(writer: Writer<'writer>) -> Self {
        Self { writer }
    }
}

impl Visit for DimmedFieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.index() > 0 {
            let _ = write!(self.writer, " ");
        }

        let _ = write!(
            self.writer,
            "{}={:?}",
            style(field.name().to_string()).dim(),
            value
        );
    }
}

pub struct TierLogFormatter;

impl<S, N> FormatEvent<S, N> for TierLogFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        use tracing::Level;

        let meta = event.metadata();

        let styled_level = match *meta.level() {
            Level::ERROR => style("ERROR").red(),
            Level::WARN => style(" WARN").yellow(),
            Level::INFO => style(" INFO").green(),
            Level::DEBUG => style("DEBUG").blue(),
            Level::TRACE => style("TRACE").magenta(),
        };
        write!(writer, "{} ", styled_level)?;

        // include the target in the log output if RUST_LOG is set, to help with restricting logs
        // further.
        if env::var("RUST_LOG").is_ok() {
            write!(writer, "{}: ", style(meta.target()).dim())?;
        }

        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

pub fn get_indicatif_layer<T: Subscriber + for<'a> LookupSpan<'a>>()
-> Result<IndicatifLayer<T, DimmedFieldNames>, Report> {
    Ok(IndicatifLayer::new()
        .with_span_field_formatter(DimmedFieldNames)
        .with_progress_style(spinner_style()?)
        .with_span_child_prefix_indent("  ")
        .with_span_child_prefix_symbol("↳ "))
}

pub fn logger_config(writer: IndicatifWriter) -> Box<dyn Layer<Registry> + Send + Sync> {
    Box::new(
        tracing_subscriber::fmt::layer()
            .event_format(TierLogFormatter)
            .with_writer(writer),
    )
}

pub fn bar_progress_style() -> Result<ProgressStyle, Report> {
    let template = "{span_child_prefix} [{elapsed_precise:.bold}] {span_name:.bold.cyan} \
{span_fields} {wide_bar:.cyan} {pos}/{len}";

    ProgressStyle::with_template(template)
        .context("failed to set progress style")
        .map_err(Report::into_dynamic)
}

pub fn spinner_style() -> Result<ProgressStyle, Report> {
    let template = "{span_child_prefix}{spinner:.cyan} [{elapsed_precise:.bold}] {span_name:.bold.cyan}{msg} {span_fields}";

    Ok(ProgressStyle::with_template(template)
        .context("failed to create progress style")?
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⢰", "⣰", "⣠", "⣄", "⣆", "⠇", "⠏", "✓"]))
}
