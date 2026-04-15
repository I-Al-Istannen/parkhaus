use crate::data::S3ObjectId;
use jiff::Zoned;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{SqliteConnection, query};

pub(super) async fn record_access(
    con: &mut SqliteConnection,
    object: &S3ObjectId,
    now: &Zoned,
) -> Result<(), Report> {
    let time_bucket = to_bucket(now);
    query!(
        r#"
        INSERT INTO AccessCounters
            (obj_bucket, obj_key, time_bucket, count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT DO UPDATE SET
            count = count + 1
        "#,
        object.bucket,
        object.key,
        time_bucket
    )
    .execute(con)
    .await
    .context("Failed to add access counter")?;

    Ok(())
}

pub(super) async fn cleanup_old(con: &mut SqliteConnection, now: &Zoned) -> Result<(), Report> {
    let last_bucket = to_bucket(&(now - jiff::Span::new().days(30)));
    query!(
        "DELETE FROM AccessCounters WHERE time_bucket < ?",
        last_bucket
    )
    .execute(con)
    .await
    .context("Failed to clean up old access counters")?;

    Ok(())
}

pub(crate) fn to_bucket(now: &Zoned) -> i64 {
    let date = now.date();
    let year = date.year();
    let month = now.date().month();
    let day = now.date().day();

    (year as i64) * 10000 + (month as i64) * 100 + (day as i64)
}
