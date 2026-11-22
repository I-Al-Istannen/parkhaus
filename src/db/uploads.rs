use crate::data::{S3ObjectId, UpstreamId};
use jiff::Timestamp;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{SqliteConnection, query};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub object: S3ObjectId,
    pub upstream: UpstreamId,
}

pub(super) async fn add(
    con: &mut SqliteConnection,
    upload: &MultipartUpload,
    created: Timestamp,
) -> Result<(), Report> {
    let created = created.as_millisecond();
    query!(
        "INSERT OR REPLACE INTO MultipartUploads (upload_id, bucket, key, upstream, created) \
         VALUES (?, ?, ?, ?, ?)",
        upload.upload_id,
        upload.object.bucket,
        upload.object.key,
        upload.upstream,
        created
    )
    .execute(con)
    .await
    .context("failed to record multipart upload")
    .attach(format!("upload: {upload:?}"))?;
    Ok(())
}

pub(super) async fn get_upstream(
    con: &mut SqliteConnection,
    upload_id: &str,
) -> Result<Option<UpstreamId>, Report> {
    Ok(query!(
        "SELECT upstream FROM MultipartUploads WHERE upload_id = ?",
        upload_id
    )
    .map(|row| UpstreamId(row.upstream))
    .fetch_optional(con)
    .await
    .context("failed to get multipart upload")
    .attach(format!("upload id: {upload_id}"))?)
}

pub(super) async fn remove(con: &mut SqliteConnection, upload_id: &str) -> Result<(), Report> {
    query!(
        "DELETE FROM MultipartUploads WHERE upload_id = ?",
        upload_id
    )
    .execute(con)
    .await
    .context("failed to remove multipart upload")
    .attach(format!("upload id: {upload_id}"))?;
    Ok(())
}

pub(super) async fn get_older_than(
    con: &mut SqliteConnection,
    cutoff: Timestamp,
) -> Result<Vec<MultipartUpload>, Report> {
    let cutoff = cutoff.as_millisecond();
    Ok(query!(
        "SELECT upload_id, bucket, key, upstream FROM MultipartUploads WHERE created < ?",
        cutoff
    )
    .map(|row| MultipartUpload {
        upload_id: row.upload_id,
        object: S3ObjectId {
            bucket: row.bucket,
            key: row.key,
        },
        upstream: UpstreamId(row.upstream),
    })
    .fetch_all(con)
    .await
    .context("failed to get old multipart uploads")?)
}
