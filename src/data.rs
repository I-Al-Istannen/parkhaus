pub(crate) use crate::config::UpstreamId;

#[derive(Debug, Clone)]
pub struct S3ObjectId {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub id: S3ObjectId,
    pub assigned_upstream: UpstreamId,
    pub last_modified: jiff::Timestamp,
}
