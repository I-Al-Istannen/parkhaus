use crate::data::TieringRule;
use crate::policy::expr::{parse_expr, typecheck};
use crate::s3::types::{AddressingStyle, ForwardObjectUrl};
use derive_more::{Display, From};
use rootcause::bail;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use serde::Deserialize;
use serde::Serialize;
use sqlx::Type;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub metrics_listen: Option<String>,
    pub db_path: PathBuf,
    pub upstreams: HashMap<UpstreamId, Upstream>,
    pub tiering_rules: Vec<TieringRule>,
    _prevent_construction: PhantomData<String>,
}

impl Config {
    pub fn new(
        listen: String,
        metrics_listen: Option<String>,
        db_path: PathBuf,
        upstreams: Vec<Upstream>,
        tiering_rules: Vec<TieringRule>,
    ) -> Result<Self, Report> {
        let upstreams = Self::validate_upstreams(upstreams)?;

        Ok(Self {
            listen,
            metrics_listen,
            db_path,
            upstreams: upstreams
                .into_iter()
                .map(|up| (up.name.clone(), up))
                .collect(),
            tiering_rules,
            _prevent_construction: PhantomData,
        })
    }

    fn validate_upstreams(mut upstreams: Vec<Upstream>) -> Result<Vec<Upstream>, Report> {
        if upstreams.is_empty() {
            bail!("config must contain at least one upstream");
        }

        let priorities = upstreams.iter().map(|it| it.order).collect::<HashSet<_>>();
        if priorities.len() != upstreams.len() {
            bail!("upstream order must be unique");
        }
        upstreams.sort_unstable_by_key(|it| it.order);

        Ok(upstreams)
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

#[derive(Debug, Clone)]
pub struct Upstream {
    pub name: UpstreamId,
    /// The order of the upstream, where smaller numbers are hotter.
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub s3_access_key: String,
    pub s3_secret: S3Secret,
    pub region: String,
}

impl Upstream {
    pub fn format_url(&self, bucket: &str, key: Option<&str>) -> Result<ForwardObjectUrl, Report> {
        self.addressing_style
            .format_url(&self.base_url, bucket, key)
    }
}

#[derive(Clone, Deserialize)]
struct RawConfig {
    pub listen: String,
    pub metrics_listen: Option<String>,
    pub db_path: PathBuf,
    pub upstreams: HashMap<String, RawUpstream>,
    pub tiering_rules: Vec<RawTieringRule>,
}

#[derive(Clone, Deserialize)]
struct RawUpstream {
    pub order: usize,
    pub base_url: Url,
    pub addressing_style: AddressingStyle,
    pub s3_access_key: String,
    pub s3_secret: String,
    pub region: String,
}

#[derive(Clone, Deserialize)]
struct RawTieringRule {
    pub when: String,
    pub to: String,
}

pub fn load(path: &Path) -> Result<Config, Report> {
    let text =
        fs::read_to_string(path).context(format!("failed reading data at {}", path.display()))?;
    let raw: RawConfig = toml::from_str(&text)
        .map_err(|e| pretty_toml_error(path, &text, e))
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
                s3_access_key: maybe_env(raw.s3_access_key)?,
                s3_secret: S3Secret(maybe_env(raw.s3_secret)?),
                region: raw.region,
            })
        })
        .collect::<Result<Vec<_>, Report>>()?;
    let parsed_tiering_rules = raw
        .tiering_rules
        .into_iter()
        .map(|it| {
            let expr = parse_expr(&it.when).context("failed to parse tiering rule query")?;
            let expr =
                typecheck(expr, &it.when).context("failed to typecheck tiering rule query")?;
            Ok(TieringRule {
                filter: expr,
                to: UpstreamId(it.to),
            })
        })
        .collect::<Result<Vec<_>, Report>>()
        .context("failed to parse tiering rules")?;

    Config::new(
        raw.listen,
        raw.metrics_listen,
        raw.db_path,
        parsed_upstreams,
        parsed_tiering_rules,
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

#[cfg(test)]
mod tests {
    use crate::config::{
        AddressingStyle, Config, RawConfig, RawUpstream, S3Secret, Upstream, from_raw,
    };
    use crate::data::{TieringRule, UpstreamId};
    use rootcause::Report;
    use std::path::PathBuf;

    fn create_upstream(order: usize) -> Upstream {
        Upstream {
            name: UpstreamId(format!("upstream{order}")),
            order,
            base_url: "http://localhost:3000".parse().unwrap(),
            addressing_style: AddressingStyle::Path,
            s3_access_key: "access".to_string(),
            s3_secret: S3Secret("secret".to_string()),
            region: "garage".to_string(),
        }
    }

    fn create_config(upstreams: Vec<Upstream>, rules: Vec<TieringRule>) -> Result<Config, Report> {
        Config::new(
            "127.0.0.1".to_string(),
            None,
            PathBuf::default(),
            upstreams,
            rules,
        )
    }

    #[test]
    fn test_duplicate_order() {
        let res = create_config(vec![create_upstream(2), create_upstream(2)], vec![]);
        assert!(res.is_err(), "Expected error, got {res:?}");
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
                    s3_access_key: "env:NON_EXISTENT_ENV_VAR".to_string(),
                    s3_secret: "env:NON_EXISTENT_ENV_VAR".to_string(),
                    region: "garage".to_string(),
                },
            )]
            .into_iter()
            .collect(),
            tiering_rules: vec![],
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
                    s3_access_key: format!("env:{var_key}"),
                    s3_secret: format!("env:{var_key}"),
                    region: "garage".to_string(),
                },
            )]
            .into_iter()
            .collect(),
            tiering_rules: vec![],
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
