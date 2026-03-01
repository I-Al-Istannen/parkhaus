use super::toml_utils;
use crate::data::S3ObjectId;
use derive_more::{Display, From};
use rootcause::Report;
use rootcause::bail;
use rootcause::prelude::ResultExt;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Type;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use url::Url;

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
    pub upstreams: HashMap<UpstreamId, Upstream>,
}

impl Config {
    pub fn hottest_upstream(&self) -> &Upstream {
        self.upstreams
            .values()
            .min_by_key(|u| u.order)
            .expect("config must contain at least one upstream")
    }

    pub fn coldest_upstream(&self) -> &Upstream {
        self.upstreams
            .values()
            .min_by_key(|u| u.order)
            .expect("config must contain at least one upstream")
    }
}

#[derive(Debug, Clone, From, Display, PartialEq, Eq, Hash, Type)]
#[sqlx(transparent)]
pub struct UpstreamId(pub String);

#[derive(Debug, Clone)]
pub struct Upstream {
    pub name: UpstreamId,
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
}

impl Upstream {
    pub fn format_url(&self, s3object_id: &S3ObjectId) -> Url {
        let mut url = self.base_url.clone();
        match self.addressing_style {
            AddressingStyle::Path => {
                url.path_segments_mut()
                    .expect("base URL can't be cannot-be-a-base")
                    .push(&s3object_id.bucket)
                    .push(&s3object_id.key);
                url
            }
            AddressingStyle::VirtualHosted => {
                let host = url.host_str().expect("base URL can't be cannot-be-a-base");
                let host = format!("{}.{}", s3object_id.bucket, host);
                url.set_host(Some(&host)).expect("failed to set host");
                url.path_segments_mut()
                    .expect("base URL can't be cannot-be-a-base")
                    .push(&s3object_id.key);
                url
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawConfig {
    pub listen: String,
    pub db_path: PathBuf,
    pub upstreams: HashMap<String, RawUpstream>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawUpstream {
    pub order: usize,
    pub base_url: Url,
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
        .map(|it| it.1.order)
        .collect::<HashSet<_>>();
    if priorities.len() != raw.upstreams.len() {
        bail!("upstream priorities must be unique");
    }

    let parsed_upstreams = raw
        .upstreams
        .into_iter()
        .map(|(name, raw)| {
            let upstream = Upstream {
                name: UpstreamId(name.clone()),
                order: raw.order,
                base_url: raw.base_url,
                addressing_style: raw.addressing_style,
            };
            (UpstreamId(name), upstream)
        })
        .collect();

    Ok(Config {
        listen: raw.listen,
        db_path: raw.db_path,
        upstreams: parsed_upstreams,
    })
}
