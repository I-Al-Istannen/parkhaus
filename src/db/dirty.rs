use crate::data::{S3Object, S3ObjectId, UpstreamId};
use crate::db::objects;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{SqliteConnection, query};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyObject {
    /// The upstream that should hold the object after the change.
    pub target: UpstreamId,
    /// The upstream that held the object before, if it differs from the target.
    pub previous: Option<UpstreamId>,
    /// Whether the change only moves the object, keeping its timestamps.
    pub migration: bool,
}

/// What the upstreams of a dirty object say about it.
#[derive(Debug, Clone)]
pub enum Settlement {
    /// The DB row is still accurate.
    Keep,
    /// The object moved unchanged.
    Move(UpstreamId),
    Record(S3Object),
    Delete,
}

pub(super) async fn mark(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
    dirty: &DirtyObject,
) -> Result<(), Report> {
    let previous = dirty.previous.as_ref().filter(|it| **it != dirty.target);
    query!(
        "INSERT INTO DirtyObjects (bucket, key, target, previous, migration) \
         VALUES (?, ?, ?, ?, ?)",
        obj.bucket,
        obj.key,
        dirty.target,
        previous,
        dirty.migration
    )
    .execute(con)
    .await
    .context("failed to mark object dirty")
    .attach(format!("object: {obj}"))
    .attach(format!("dirty: {dirty:?}"))?;
    Ok(())
}

pub(super) async fn find(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<Option<DirtyObject>, Report> {
    Ok(query!(
        "SELECT target, previous, migration FROM DirtyObjects WHERE bucket = ? AND key = ?",
        obj.bucket,
        obj.key
    )
    .map(|row| DirtyObject {
        target: UpstreamId(row.target),
        previous: row.previous.map(UpstreamId),
        migration: row.migration,
    })
    .fetch_optional(con)
    .await
    .context("failed to find dirty object")
    .attach(format!("object: {obj}"))?)
}

pub(super) async fn get_all(con: &mut SqliteConnection) -> Result<Vec<S3ObjectId>, Report> {
    Ok(query!("SELECT bucket, key FROM DirtyObjects")
        .map(|row| S3ObjectId {
            bucket: row.bucket,
            key: row.key,
        })
        .fetch_all(con)
        .await
        .context("failed to get dirty objects")?)
}

pub(super) async fn apply(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
    settlement: &Settlement,
) -> Result<(), Report> {
    match settlement {
        Settlement::Keep => Ok(()),
        Settlement::Move(upstream) => objects::set_upstream(con, obj, upstream).await,
        Settlement::Record(object) => objects::record_creation(con, object).await,
        Settlement::Delete => objects::delete_object(con, obj).await,
    }
}

pub(super) async fn clear(con: &mut SqliteConnection, obj: &S3ObjectId) -> Result<(), Report> {
    query!(
        "DELETE FROM DirtyObjects WHERE bucket = ? AND key = ?",
        obj.bucket,
        obj.key
    )
    .execute(con)
    .await
    .context("failed to clear dirty object")
    .attach(format!("object: {obj}"))?;
    Ok(())
}
