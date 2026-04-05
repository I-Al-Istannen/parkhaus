pub(crate) use crate::config::UpstreamId;
use crate::db::TieringRuleEnv;
use crate::policy::expr::{Expr, Typechecked};
use derive_more::Display;
use serde::Serialize;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct S3ObjectId {
    pub bucket: String,
    pub key: String,
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
}

#[derive(Debug, Copy, Clone, Serialize, Display, sqlx::Type)]
pub enum MigrationState {
    Started,
    CopiedToTarget,
}

impl MigrationState {
    pub fn all() -> &'static [Self] {
        &[Self::Started, Self::CopiedToTarget]
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingMigration {
    pub object: S3ObjectId,
    pub source_upstream: UpstreamId,
    pub target_upstream: UpstreamId,
}

#[derive(Debug, Clone)]
pub struct InFlightMigration {
    pub pending: PendingMigration,
    pub state: MigrationState,
}

#[derive(Debug, Clone)]
pub struct TieringRule {
    pub filter: Expr<Typechecked<TieringRuleEnv>>,
    pub to: UpstreamId,
}
