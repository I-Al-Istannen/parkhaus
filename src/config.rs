use super::toml_utils;
use crate::data::ForwardObjectUrl;
use cmp::Ordering;
use derive_more::{Display, From};
use jiff::{Timestamp, Zoned};
use rootcause::bail;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use serde::Serialize;
use serde::de::Error;
use serde::{Deserialize, Deserializer};
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
        upstreams: Vec<Upstream>,
    ) -> Result<Self, Report> {
        Self::validate_upstreams(upstreams.iter().collect())?;

        Ok(Self {
            listen,
            metrics_listen,
            db_path,
            upstreams: upstreams
                .into_iter()
                .map(|up| (up.name.clone(), up))
                .collect(),
            _prevent_construction: PhantomData,
        })
    }

    fn validate_upstreams(mut upstreams: Vec<&Upstream>) -> Result<(), Report> {
        if upstreams.is_empty() {
            bail!("config must contain at least one upstream");
        }

        let all_buckets = upstreams
            .iter()
            .flat_map(|it| it.age_limits.get_buckets().into_iter())
            .flatten()
            // If all were AgeLimits::Uniform, we would not check anything (as all_buckets = [])
            // So we add another bucket for simplicity :)
            .chain(["test-bucket-for-fallback-behaviour"])
            .collect::<HashSet<_>>();

        let priorities = upstreams.iter().map(|it| it.order).collect::<HashSet<_>>();
        if priorities.len() != upstreams.len() {
            bail!("upstream order must be unique");
        }

        upstreams.sort_unstable_by_key(|it| it.order);

        if let Some(coldest) = upstreams.last() {
            for bucket in &all_buckets {
                if !matches!(coldest.age_limits.get_max_age(bucket), MaxAge::Forever) {
                    return Err(report!("the coldest upstream must not have max_age set")
                        .attach(format!("upstream: {:?}", coldest.name))
                        .attach(format!("max_age: {:?}", coldest.age_limits))
                        .attach(format!("hint: check failed for bucket {:?}", bucket)));
                }
            }
        }

        // Verify that hotter upstreams have smaller max_age than colder upstreams, otherwise
        // migration is hard.
        for window in upstreams.windows(2) {
            let hotter = &window[0];
            let colder = &window[1];

            let hotter_age = &hotter.age_limits;
            let colder_age = &colder.age_limits;

            if hotter_age.has_any_higher_limit_than(colder_age, &all_buckets)? {
                return Err(report!(
                    "hotter upstream must have smaller max_age than colder upstream"
                )
                .attach(format!(
                    "hotter upstream: {} (order {})",
                    hotter.name, hotter.order
                ))
                .attach(format!("hotter max_age: {:?}", hotter_age))
                .attach(format!(
                    "colder upstream: {} (order {})",
                    colder.name, colder.order
                ))
                .attach(format!("colder max_age: {:?}", colder_age)));
            }
        }

        Ok(())
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

    pub fn upstreams_in_order(&self) -> Vec<&Upstream> {
        let mut res = self.upstreams.values().collect::<Vec<_>>();
        res.sort_unstable_by_key(|it| it.order);
        res
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

#[derive(Debug, Copy, Clone, Default)]
pub enum MaxAge {
    #[default]
    Forever,
    Limited(jiff::Span),
}

impl<'de> Deserialize<'de> for MaxAge {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let as_str = String::deserialize(deserializer)?;
        if as_str.trim().to_lowercase() == "forever" {
            return Ok(Self::Forever);
        }
        match as_str.parse() {
            Ok(span) => Ok(Self::Limited(span)),
            Err(e) => Err(D::Error::custom(format!("invalid max_age: {e}"))),
        }
    }
}

impl From<jiff::Span> for MaxAge {
    fn from(span: jiff::Span) -> Self {
        Self::Limited(span)
    }
}

impl MaxAge {
    pub fn cmp(&self, other: &Self, now: &Zoned) -> Result<Ordering, Report> {
        Ok(match (self, other) {
            (Self::Forever, Self::Forever) => Ordering::Equal,
            (Self::Forever, Self::Limited(_)) => Ordering::Greater,
            (Self::Limited(_), Self::Forever) => Ordering::Less,
            (Self::Limited(we), Self::Limited(they)) => we.compare((*they, now))?,
        })
    }

    pub fn limit(&self) -> Option<jiff::Span> {
        match self {
            Self::Forever => None,
            Self::Limited(max) => Some(*max),
        }
    }

    pub fn is_time_within(&self, now: &Zoned, target: Timestamp) -> bool {
        match self {
            Self::Forever => true,
            Self::Limited(max) => target > (now - *max).timestamp(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AgeLimits {
    Uniform(MaxAge),
    PerBucket {
        #[serde(default)]
        fallback: MaxAge,
        per_bucket: HashMap<String, MaxAge>,
    },
}

impl AgeLimits {
    pub fn no_limits() -> Self {
        Self::Uniform(MaxAge::Forever)
    }

    fn has_any_higher_limit_than(
        &self,
        other: &Self,
        all_buckets: &HashSet<&str>,
    ) -> Result<bool, Report> {
        for bucket in all_buckets {
            let my_max = self.get_max_age(bucket);
            let other_max = other.get_max_age(bucket);
            if my_max.cmp(&other_max, &Zoned::now())? == Ordering::Greater {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_buckets(&self) -> Option<impl Iterator<Item = &str>> {
        match self {
            Self::Uniform(_) => None,
            Self::PerBucket { per_bucket, .. } => Some(per_bucket.keys().map(|s| s.as_str())),
        }
    }

    pub fn get_max_age(&self, bucket: &str) -> MaxAge {
        match self {
            Self::Uniform(time) => *time,
            Self::PerBucket {
                fallback,
                per_bucket,
            } => per_bucket.get(bucket).cloned().unwrap_or(*fallback),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Upstream {
    pub name: UpstreamId,
    /// The order of the upstream, where smaller numbers are hotter.
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub age_limits: AgeLimits,
    pub s3_access_key: String,
    pub s3_secret: S3Secret,
    pub region: String,
}

impl Upstream {
    pub fn format_url(&self, bucket: &str, key: Option<&str>) -> Result<ForwardObjectUrl, Report> {
        self.addressing_style
            .format_url(&self.base_url, bucket, key)
    }

    pub fn age_is_within_limits(
        &self,
        bucket: &str,
        now: &Zoned,
        last_modified: Timestamp,
    ) -> bool {
        self.age_limits
            .get_max_age(bucket)
            .is_time_within(now, last_modified)
    }
}

#[derive(Clone, Deserialize)]
struct RawConfig {
    pub listen: String,
    pub metrics_listen: Option<String>,
    pub db_path: PathBuf,
    pub upstreams: HashMap<String, RawUpstream>,
}

#[derive(Clone, Deserialize)]
struct RawUpstream {
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub max_age: Option<AgeLimits>,
    pub s3_access_key: String,
    pub s3_secret: String,
    pub region: String,
}

pub fn load(path: &Path) -> Result<Config, Report> {
    let raw: RawConfig = toml_utils::load_from_file(path)
        .context("failed to load config")
        .attach(format!("hint: tried '{}'", path.display()))
        .attach("hint: use '--config <path>' to specify a different config file")?;

    from_raw(raw)
}

fn from_raw(raw: RawConfig) -> Result<Config, Report> {
    let parsed_upstreams = raw
        .upstreams
        .into_iter()
        .map(|(name, raw)| {
            Ok(Upstream {
                name: UpstreamId(name.clone()),
                order: raw.order,
                base_url: raw.base_url,
                addressing_style: raw.addressing_style,
                age_limits: raw.max_age.unwrap_or(AgeLimits::Uniform(MaxAge::Forever)),
                s3_access_key: maybe_env(raw.s3_access_key)?,
                s3_secret: S3Secret(maybe_env(raw.s3_secret)?),
                region: raw.region,
            })
        })
        .collect::<Result<Vec<_>, Report>>()?;

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

#[cfg(test)]
mod tests {
    use crate::config::{
        AddressingStyle, AgeLimits, Config, MaxAge, RawConfig, RawUpstream, S3Secret, Upstream,
        from_raw,
    };
    use crate::data::UpstreamId;
    use jiff::Span;
    use rootcause::Report;
    use std::path::PathBuf;

    fn create_upstream(order: usize, max_age: AgeLimits) -> Upstream {
        Upstream {
            name: UpstreamId(format!("upstream{order}")),
            order,
            base_url: "http://localhost:3000".parse().unwrap(),
            addressing_style: AddressingStyle::Path,
            age_limits: max_age,
            s3_access_key: "access".to_string(),
            s3_secret: S3Secret("secret".to_string()),
            region: "garage".to_string(),
        }
    }

    fn create_config(upstreams: Vec<Upstream>) -> Result<Config, Report> {
        Config::new("127.0.0.1".to_string(), None, PathBuf::default(), upstreams)
    }

    fn uniform_age(hours: i64) -> AgeLimits {
        AgeLimits::Uniform(MaxAge::Limited(Span::new().hours(hours)))
    }

    #[allow(single_use_lifetimes)] // clippy does not reach a steady-state
    fn per_bucket_age<'a>(
        fallback: MaxAge,
        ages: impl IntoIterator<Item = (&'a str, MaxAge)>,
    ) -> AgeLimits {
        AgeLimits::PerBucket {
            fallback,
            per_bucket: ages
                .into_iter()
                .map(|(bucket, span)| (bucket.to_owned(), span))
                .collect(),
        }
    }

    #[test]
    fn test_duplicate_order() {
        let res = create_config(vec![
            create_upstream(2, uniform_age(2)),
            create_upstream(2, AgeLimits::no_limits()),
        ]);
        assert!(res.is_err(), "Expected error, got {res:?}");
    }

    #[test]
    fn test_not_increasing_max_age() {
        let res = create_config(vec![
            create_upstream(1, uniform_age(2)),
            create_upstream(2, uniform_age(3)),
            create_upstream(3, uniform_age(1)),
        ]);
        assert!(res.is_err(), "Expected error, got {res:?}");
    }

    #[test]
    fn test_increasing_max_age_no_none() {
        let res = create_config(vec![
            create_upstream(1, uniform_age(2)),
            create_upstream(2, uniform_age(3)),
            create_upstream(3, uniform_age(5)),
        ]);
        assert!(res.is_err(), "Expected error, got {res:?}");
    }

    #[test]
    fn test_uniform_valid() {
        let res = create_config(vec![
            create_upstream(1, uniform_age(2)),
            create_upstream(2, uniform_age(3)),
            create_upstream(3, AgeLimits::no_limits()),
        ]);
        assert!(res.is_ok(), "Expected ok, got {}", res.err().unwrap());
    }

    #[test]
    fn test_per_bucket_not_increasing() {
        let res = create_config(vec![
            create_upstream(
                1,
                per_bucket_age(
                    Span::new().hours(2).into(),
                    [("bucket1", Span::new().hours(3).into())],
                ),
            ),
            create_upstream(2, uniform_age(4)),
            create_upstream(3, AgeLimits::no_limits()),
        ]);
        assert!(res.is_ok(), "Expected ok, got {}", res.err().unwrap());
    }

    #[test]
    fn test_per_bucket_wrong_fallback() {
        let res = create_config(vec![
            create_upstream(
                1,
                per_bucket_age(
                    Span::new().hours(2).into(),
                    [("bucket1", Span::new().hours(3).into())],
                ),
            ),
            create_upstream(2, uniform_age(4)),
            create_upstream(
                3,
                per_bucket_age(Span::new().hours(2).into(), [("bucket1", MaxAge::Forever)]),
            ),
        ]);
        assert!(res.is_err(), "Expected err, got {res:?}");
    }

    #[test]
    fn test_per_bucket_stop_bucket_in_middle() {
        let res = create_config(vec![
            create_upstream(
                1,
                per_bucket_age(
                    Span::new().hours(2).into(),
                    [("bucket1", Span::new().hours(3).into())],
                ),
            ),
            create_upstream(
                2,
                per_bucket_age(
                    Span::new().hours(3).into(),
                    [
                        ("bucket1", MaxAge::Forever),
                        ("bucket2", Span::new().hours(4).into()),
                    ],
                ),
            ),
            create_upstream(3, AgeLimits::no_limits()),
        ]);
        assert!(res.is_ok(), "Expected ok, got {}", res.err().unwrap());
    }

    #[test]
    fn test_env_interpolation_fails() {
        let res = from_raw(RawConfig {
            listen: "127.0.0.1".to_string(),
            metrics_listen: None,
            db_path: PathBuf::default(),
            upstreams: vec![(
                "test".to_string(),
                RawUpstream {
                    order: 1,
                    base_url: "http://localhost:3000".parse().unwrap(),
                    addressing_style: AddressingStyle::Path,
                    max_age: None,
                    s3_access_key: "env:NON_EXISTENT_ENV_VAR".to_string(),
                    s3_secret: "env:NON_EXISTENT_ENV_VAR".to_string(),
                    region: "garage".to_string(),
                },
            )]
            .into_iter()
            .collect(),
        });
        assert!(res.is_err(), "Expected err, got {:?}", res);
        assert!(format!("{:?}", res.unwrap_err()).contains("NON_EXISTENT_ENV_VAR"));
    }

    #[test]
    fn test_env_interpolation() {
        let (var_key, var_val) = std::env::vars().next().unwrap();
        let res = from_raw(RawConfig {
            listen: "127.0.0.1".to_string(),
            metrics_listen: None,
            db_path: PathBuf::default(),
            upstreams: vec![(
                "test".to_string(),
                RawUpstream {
                    order: 1,
                    base_url: "http://localhost:3000".parse().unwrap(),
                    addressing_style: AddressingStyle::Path,
                    max_age: None,
                    s3_access_key: format!("env:{var_key}"),
                    s3_secret: format!("env:{var_key}"),
                    region: "garage".to_string(),
                },
            )]
            .into_iter()
            .collect(),
        });
        assert!(res.is_ok(), "Expected ok, got {}", res.unwrap_err());
        assert_eq!(
            res.as_ref()
                .unwrap()
                .upstreams
                .values()
                .next()
                .unwrap()
                .s3_access_key,
            var_val
        );
        assert_eq!(
            res.as_ref()
                .unwrap()
                .upstreams
                .values()
                .next()
                .unwrap()
                .s3_secret
                .0,
            var_val
        );
    }
}
