use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use serde::Deserialize;
use std::fs;
use std::path::Path;

pub fn load_from_file<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<T, Report> {
    let raw =
        fs::read_to_string(path).context(format!("failed reading data at {}", path.display()))?;

    Ok(toml::from_str(&raw).map_err(|e| pretty_toml_error(path, &raw, e))?)
}

fn pretty_toml_error(path: &Path, raw: &str, error: toml::de::Error) -> Report<String> {
    let mut summary = format!("failed to parse TOML config at {}", path.display());

    if let Some(span) = error.span() {
        let (line, column) = byte_offset_to_line_col(raw, span.start);
        summary.push_str(&format!(" (line {line}, column {column})"));
    }

    report!(summary).attach(error.message().to_string())
}

fn byte_offset_to_line_col(input: &str, offset: usize) -> (usize, usize) {
    let bounded = offset.min(input.len());
    let prefix = input.get(..bounded).unwrap_or(input);

    let line = prefix.bytes().filter(|&byte| byte == b'\n').count() + 1;
    let column = prefix
        .rsplit('\n')
        .next()
        .map_or(1, |it| it.chars().count() + 1);

    (line, column)
}
