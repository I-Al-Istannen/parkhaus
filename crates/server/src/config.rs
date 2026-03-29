use super::toml_utils;
use crate::data::ForwardObjectUrl;
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
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddressingStyle {
    Path,
    VirtualHosted,
    VirtualHostedResolveDns,
}

impl AddressingStyle {
    pub fn format_url(
        &self,
        base_url: &Url,
        bucket: &str,
        key: Option<&str>,
    ) -> Result<ForwardObjectUrl, Report> {
        let mut url = base_url.clone();
        let host = base_url.host_str().ok_or_else(|| {
            report!("base URL must have a host").attach(format!("url: {base_url}"))
        })?;
        let host = format!("{bucket}.{host}");
        let mut segments = url.path_segments_mut().map_err(|()| {
            report!("base URL cannot be cannot-be-a-base").attach(format!("url: {base_url}"))
        })?;

        Ok(match self {
            Self::Path => {
                segments.push(bucket);
                if let Some(key) = key {
                    segments.extend(key.split('/'));
                }
                drop(segments);
                ForwardObjectUrl::no_host(url)
            }
            Self::VirtualHosted => {
                if let Some(key) = key {
                    segments.extend(key.split('/'));
                }
                drop(segments);
                ForwardObjectUrl::with_host(url, host)
            }
            Self::VirtualHostedResolveDns => {
                if let Some(key) = key {
                    segments.extend(key.split('/'));
                }
                drop(segments);
                url.set_host(Some(&host)).map_err(|err| {
                    report!("failed to set virtual hosted endpoint").attach(format!("error: {err}"))
                })?;
                ForwardObjectUrl::no_host(url)
            }
        })
    }

    pub fn format_bucket_url(
        &self,
        base_url: &Url,
        bucket: &str,
    ) -> Result<ForwardObjectUrl, Report> {
        self.format_url(base_url, bucket, None)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub metrics_listen: Option<String>,
    pub db_path: PathBuf,
    pub upstreams: HashMap<UpstreamId, Upstream>,
    _prevent_construction: PhantomData<String>,
}

impl Config {
    pub fn new(
        listen: String,
        metrics_listen: Option<String>,
        db_path: PathBuf,
        upstream_map: HashMap<UpstreamId, Upstream>,
    ) -> Result<Self, Report> {
        let mut upstreams = upstream_map.iter().collect::<Vec<_>>();

        if upstreams.is_empty() {
            bail!("config must contain at least one upstream");
        }

        let priorities = upstreams
            .iter()
            .map(|it| it.1.order)
            .collect::<HashSet<_>>();
        if priorities.len() != upstreams.len() {
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
                return Err(report!(
                    "hotter upstream must have smaller max_age than colder upstream"
                )
                .attach(format!(
                    "hotter upstream: {} (order {})",
                    hotter_name, hotter.order
                ))
                .attach(format!("hotter max_age: {}", hotter_age))
                .attach(format!(
                    "colder upstream: {} (order {})",
                    colder_name, colder.order
                ))
                .attach(format!("colder max_age: {}", colder_age)));
            }
        }

        Ok(Self {
            listen,
            metrics_listen,
            db_path,
            upstreams: upstream_map,
            _prevent_construction: PhantomData,
        })
    }

    pub fn hottest_upstream(&self) -> &Upstream {
        self.upstreams
            .values()
            .min_by_key(|u| u.order)
            .expect("config must contain at least one upstream")
    }

    pub fn coldest_upstream(&self) -> &Upstream {
        self.upstreams
            .values()
            .max_by_key(|u| u.order)
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
    /// The order of the upstream, where smaller numbers are hotter.
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub max_age: Option<jiff::Span>,
    pub s3_access_key: String,
    pub s3_secret: S3Secret,
    pub region: String,
}

impl Upstream {
    pub fn format_url(&self, bucket: &str, key: Option<&str>) -> Result<ForwardObjectUrl, Report> {
        self.addressing_style
            .format_url(&self.base_url, bucket, key)
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
    pub metrics_listen: Option<String>,
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
    pub region: String,
}

pub fn load(path: &Path) -> Result<Config, Report> {
    let raw: RawConfig = toml_utils::load_from_file(path)
        .context("failed to load config")
        .attach(format!("hint: tried '{}'", path.display()))
        .attach("hint: use '--config <path>' to specify a different config file")?;

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
                s3_access_key: maybe_env(raw.s3_access_key)?,
                s3_secret: S3Secret(maybe_env(raw.s3_secret)?),
                region: raw.region,
            };
            Ok((UpstreamId(name), upstream))
        })
        .collect::<Result<HashMap<_, _>, Report>>()?;

    Config::new(
        raw.listen,
        raw.metrics_listen,
        raw.db_path,
        parsed_upstreams,
    )
}

fn maybe_env(value: String) -> Result<String, Report> {
    let Some((_, env_var)) = value.split_once("env:") else {
        return Ok(value);
    };
    std::env::var(env_var)
        .context("failed to detect env variable")
        .attach("hint: value starts with 'env:', so it is expected to be an env variable")
        .attach(format!("env variable: `{env_var}`"))
        .map_err(Report::into_dynamic)
}
