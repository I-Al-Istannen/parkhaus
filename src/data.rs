pub(crate) use crate::config::UpstreamId;
use derive_more::Display;
use serde::Serialize;
use std::fmt::{Display, Formatter};
use url::Url;

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
pub struct S3ObjectId {
    pub bucket: String,
    pub key: String,
}

impl Display for S3ObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bucket, self.key)
    }
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub id: S3ObjectId,
    pub assigned_upstream: UpstreamId,
    pub last_modified: jiff::Timestamp,
}

#[derive(Debug, Copy, Clone, Serialize, Display, sqlx::Type)]
pub enum MigrationState {
    Pending,
    CopiedToTarget,
    Finished,
}

impl MigrationState {
    pub fn all() -> &'static [Self] {
        &[Self::Pending, Self::CopiedToTarget, Self::Finished]
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingMigration {
    pub object: S3ObjectId,
    pub source_upstream: UpstreamId,
    pub target_upstream: UpstreamId,
    pub state: MigrationState,
}

pub struct ForwardObjectUrl {
    pub url: Url,
    pub host_header: Option<String>,
}

impl Display for ForwardObjectUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(host_header) = &self.host_header {
            write!(f, "{} (host: {})", self.url, host_header)
        } else {
            write!(f, "{}", self.url)
        }
    }
}

impl ForwardObjectUrl {
    pub fn no_host(url: Url) -> Self {
        Self {
            url,
            host_header: None,
        }
    }

    pub fn with_host(url: Url, host_header: String) -> Self {
        Self {
            url,
            host_header: Some(host_header),
        }
    }
}
