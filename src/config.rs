use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::toml_utils;
use rootcause::bail;
use rootcause::prelude::ResultExt;
use rootcause::Report;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressingStyle {
    Path,
    VirtualHosted,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub db_path: PathBuf,
    pub upstreams: HashMap<String, Upstream>,
}

#[derive(Debug, Clone)]
pub struct Upstream {
    pub name: String,
    pub priority: usize,
    pub base_url: String,
    pub addressing_style: AddressingStyle,
}

#[derive(Debug, Clone, Deserialize)]
struct RawConfig {
    pub listen: String,
    pub db_path: PathBuf,
    pub upstreams: HashMap<String, RawUpstream>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawUpstream {
    pub priority: usize,
    pub base_url: String,
    pub addressing_style: AddressingStyle,
}

pub fn load(path: &Path) -> Result<Config, Report> {
    let raw: RawConfig = toml_utils::load_from_file(path).context("failed to load config")?;

    if raw.upstreams.is_empty() {
        bail!("config must contain at least one upstream");
    }

    let priorities = raw
        .upstreams
        .iter()
        .map(|it| it.1.priority)
        .collect::<HashSet<_>>();
    if priorities.len() != raw.upstreams.len() {
        bail!("upstream priorities must be unique");
    }

    let parsed_upstreams = raw
        .upstreams
        .into_iter()
        .map(|(name, raw)| {
            let upstream = Upstream {
                name: name.clone(),
                priority: raw.priority,
                base_url: raw.base_url,
                addressing_style: raw.addressing_style,
            };
            (name, upstream)
        })
        .collect();

    Ok(Config {
        listen: raw.listen,
        db_path: raw.db_path,
        upstreams: parsed_upstreams,
    })
}
