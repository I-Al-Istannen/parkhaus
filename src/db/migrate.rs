use crate::data::{S3ObjectId, UpstreamId};
use jiff::Timestamp;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::SqliteConnection;

pub(super) async fn get_objects_in_range(
    con: &mut SqliteConnection,
    upstream: &UpstreamId,
    start: Timestamp,
    end: Timestamp,
) -> Result<Vec<(S3ObjectId, Timestamp)>, Report> {
    let start_ms = start.as_millisecond();
    let end_ms = end.as_millisecond();
    let objects = sqlx::query!(
        r#"
        SELECT bucket, key, last_modified
        FROM objects
        WHERE assigned_upstream = ? AND last_modified >= ? AND last_modified <= ?
        "#,
        upstream,
        start_ms,
        end_ms
    )
    .map(|row| {
        let id = S3ObjectId {
            bucket: row.bucket,
            key: row.key,
        };
        let time = Timestamp::from_millisecond(row.last_modified)
            .context("invalid last_modified timestamp")
            .attach(format!("upstream: {upstream}"))
            .attach(format!("last_modified: {}", row.last_modified))
            .attach(format!("object: {}", id))?;
        Ok((id, time))
    })
    .fetch_all(con)
    .await
    .context("failed to get objects in range")
    .attach(format!("upstream: {upstream}"))
    .attach(format!("start: {start}"))
    .attach(format!("end: {end}"))?
    .into_iter()
    .collect::<Result<Vec<_>, Report>>()?;

    Ok(objects)
}
