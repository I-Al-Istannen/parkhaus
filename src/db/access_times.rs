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
    let time_bucket = to_time_bucket_ms(now)?;
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
    let last_bucket = to_time_bucket_ms(&(now - jiff::Span::new().days(30)))?;
    query!(
        "DELETE FROM AccessCounters WHERE time_bucket < ?",
        last_bucket
    )
    .execute(con)
    .await
    .context("Failed to clean up old access counters")?;

    Ok(())
}

pub(crate) fn to_time_bucket_ms(now: &Zoned) -> Result<i64, Report> {
    Ok(now
        .start_of_day()
        .context("failed to get start of day")
        .attach(format!("now: {now}"))?
        .timestamp()
        .as_millisecond())
}
