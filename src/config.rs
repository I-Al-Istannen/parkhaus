use super::toml_utils;
use crate::data::S3ObjectId;
use cmp::Ordering;
use derive_more::{Display, From};
use jiff::{Timestamp, Zoned};
use rootcause::bail;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use serde::Deserialize;
use serde::Serialize;
use sqlx::Type;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
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

#[derive(Debug, Clone, From, Display, PartialEq, Eq, Hash, Type, Serialize)]
#[sqlx(transparent)]
pub struct UpstreamId(pub String);

#[derive(Clone)]
pub struct S3Secret(pub String);

impl Debug for S3Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3:{}", "*".repeat(self.0.len()))
    }
}

#[derive(Debug, Clone)]
pub struct Upstream {
    pub name: UpstreamId,
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub max_age: Option<jiff::Span>,
    pub s3_access_key: String,
    pub s3_secret: S3Secret,
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

    pub fn is_too_old(&self, now: &Zoned, last_modified: Timestamp) -> bool {
        if let Some(max_age) = self.max_age {
            last_modified < (now - max_age).timestamp()
        } else {
            false
        }
    }
}

#[derive(Clone, Deserialize)]
struct RawConfig {
    pub listen: String,
    pub db_path: PathBuf,
    pub upstreams: HashMap<String, RawUpstream>,
}

#[derive(Clone, Deserialize, Serialize)]
struct RawUpstream {
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub max_age: Option<jiff::Span>,
    pub s3_access_key: String,
    pub s3_secret: String,
}

pub fn load(path: &Path) -> Result<Config, Report> {
    let raw: RawConfig = toml_utils::load_from_file(path).context("failed to load config")?;
    check_upstreams(&raw)?;

    let parsed_upstreams = raw
        .upstreams
        .into_iter()
        .map(|(name, raw)| {
            let upstream = Upstream {
                name: UpstreamId(name.clone()),
                order: raw.order,
                base_url: raw.base_url,
                addressing_style: raw.addressing_style,
                max_age: raw.max_age,
                s3_access_key: raw.s3_access_key,
                s3_secret: S3Secret(raw.s3_secret),
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

fn check_upstreams(raw: &RawConfig) -> Result<(), Report> {
    let mut upstreams = raw.upstreams.iter().collect::<Vec<_>>();

    if upstreams.is_empty() {
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

    upstreams.sort_unstable_by_key(|it| it.1.order);

    let coldest = upstreams.pop();
    if let Some((name, val)) = coldest
        && let Some(max_age) = val.max_age
    {
        return Err(report!("the coldest upstream must not have max_age set")
            .attach(format!("upstream: {name:?}"))
            .attach(format!("max_age: {max_age:?}")));
    }

    // Verify all others have a max age
    for (name, val) in &upstreams {
        if val.max_age.is_none() {
            return Err(
                report!("all upstreams except the coldest one must have max_age set")
                    .attach(format!("upstream: {name:?}")),
            );
        }
    }

    // Verify that hotter upstreams have smaller max_age than colder upstreams, otherwise
    // migration is hard.
    for window in upstreams.windows(2) {
        let hotter_name = &window[0].0;
        let colder_name = &window[1].0;
        let hotter = &window[0].1;
        let colder = &window[1].1;

        let hotter_age = hotter.max_age.unwrap();
        let colder_age = colder.max_age.unwrap();
        let age_compare = hotter_age.compare((colder_age, &Zoned::now()))?;

        if age_compare != Ordering::Less {
            return Err(
                report!("hotter upstream must have smaller max_age than colder upstream")
                    .attach(format!(
                        "hotter upstream: {} (order {})",
                        hotter_name, hotter.order
                    ))
                    .attach(format!("hotter max_age: {}", hotter_age))
                    .attach(format!(
                        "colder upstream: {} (order {})",
                        colder_name, colder.order
                    ))
                    .attach(format!("colder max_age: {}", colder_age)),
            );
        }
    }

    Ok(())
}
