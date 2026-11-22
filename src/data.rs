pub(crate) use crate::config::UpstreamId;
use crate::db::TieringRuleEnv;
use crate::policy::expr::{Expr, Typechecked};
use serde::Serialize;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct S3ObjectId {
    pub bucket: String,
    pub key: String,
}

/// URLs normalize away `.` and `..` path segments, even percent-encoded ones, so such keys cannot
/// be addressed.
/// URLs normalize `.` and `..` segments away and drop tabs and line breaks, so such keys would
/// address a different object.
pub fn is_addressable_key(key: &str) -> bool {
    !key.split('/').any(|it| it == "." || it == "..") && !key.contains(['\t', '\n', '\r'])
}

impl Display for S3ObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.pad(&format!("{}/{}", self.bucket, self.key))
    }
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub id: S3ObjectId,
    pub assigned_upstream: UpstreamId,
    pub last_modified: jiff::Timestamp,
    /// In bytes
    pub size: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingMigration {
    pub object: S3ObjectId,
    pub source_upstream: UpstreamId,
    pub target_upstream: UpstreamId,
}

#[derive(Debug, Clone)]
pub struct TieringRule {
    pub filter: Expr<Typechecked<TieringRuleEnv>>,
    pub to: UpstreamId,
}
