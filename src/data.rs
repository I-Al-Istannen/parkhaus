use derive_more::{Display, From};
use sqlx::Type;

#[derive(Debug, Clone)]
pub struct S3ObjectId {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone, From, Display, PartialEq, Eq, Hash, Type)]
#[sqlx(transparent)]
pub struct UpstreamId(pub String);

#[derive(Debug, Clone)]
pub struct S3Object {
    pub id: S3ObjectId,
    pub assigned_upstream: UpstreamId,
    pub last_modified: jiff::Timestamp,
}
