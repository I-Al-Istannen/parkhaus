pub(crate) use crate::config::UpstreamId;
use serde::Serialize;
use std::fmt::Display;

#[derive(Debug, Clone, Serialize)]
pub struct S3ObjectId {
    pub bucket: String,
    pub key: String,
}

impl Display for S3ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.bucket, self.key)
    }
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub id: S3ObjectId,
    pub assigned_upstream: UpstreamId,
    pub last_modified: jiff::Timestamp,
}
