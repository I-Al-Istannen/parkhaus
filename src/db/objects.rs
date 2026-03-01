use crate::data::{S3Object, S3ObjectId, UpstreamId};
use jiff::Timestamp;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{SqliteConnection, query, query_as};

struct DbObject {
    bucket: String,
    key: String,
    assigned_upstream: UpstreamId,
    last_modified: i64,
}

impl TryFrom<DbObject> for S3Object {
    type Error = Report;

    fn try_from(db_obj: DbObject) -> Result<Self, Report> {
        let id = S3ObjectId {
            bucket: db_obj.bucket,
            key: db_obj.key,
        };
        let last_modified = Timestamp::from_millisecond(db_obj.last_modified)
            .context("invalid last_modified timestamp")
            .attach(format!("object: {id:?}"))?;

        Ok(Self {
            id,
            assigned_upstream: db_obj.assigned_upstream,
            last_modified,
        })
    }
}

pub(super) async fn get_object(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<S3Object, Report> {
    query_as!(
        DbObject,
        "SELECT * FROM objects WHERE bucket = $1 AND key = $2",
        obj.bucket,
        obj.key
    )
    .fetch_one(con)
    .await
    .context("Failed to fetch object")
    .attach(format!("object: {obj:?}"))?
    .try_into()
}

pub(super) async fn get_upstream(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<Option<UpstreamId>, Report> {
    Ok(query!(
        "SELECT assigned_upstream FROM objects WHERE bucket = $1 AND key = $2",
        obj.bucket,
        obj.key
    )
    .map(|row| row.assigned_upstream)
    .fetch_optional(con)
    .await
    .context("Failed to fetch object")
    .attach(format!("object: {obj:?}"))?
    .map(UpstreamId))
}

pub(super) async fn delete_object(
    con: &mut SqliteConnection,
    obj: &S3ObjectId,
) -> Result<(), Report> {
    query!(
        "DELETE FROM objects WHERE bucket = $1 AND key = $2",
        obj.bucket,
        obj.key
    )
    .execute(con)
    .await
    .context("Failed to delete object")
    .attach(format!("object: {obj:?}"))?;

    Ok(())
}

pub(super) async fn record_creation(
    con: &mut SqliteConnection,
    obj: &S3Object,
) -> Result<(), Report> {
    let last_modified = obj.last_modified.as_millisecond();

    query!(
        r#"
        INSERT INTO objects (bucket, key, assigned_upstream, last_modified)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (bucket, key) DO UPDATE SET
            assigned_upstream = excluded.assigned_upstream,
            last_modified = excluded.last_modified
        "#,
        obj.id.bucket,
        obj.id.key,
        obj.assigned_upstream,
        last_modified
    )
    .execute(con)
    .await
    .context("Failed to record object creation")
    .attach(format!("object: {obj:?}"))?;

    Ok(())
}
