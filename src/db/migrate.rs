use crate::data::{InFlightMigration, MigrationState, PendingMigration, S3ObjectId, UpstreamId};
use jiff::Timestamp;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::{SqliteConnection, query, query_as};

pub(crate) async fn get_objects_not_in_range(
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
        WHERE assigned_upstream = ? AND (last_modified < ? OR last_modified > ?)
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

pub(crate) async fn add_or_update_in_flight(
    con: &mut SqliteConnection,
    migration: &InFlightMigration,
) -> Result<(), Report> {
    let pending = &migration.pending;
    query!(
        r#"
        INSERT INTO InFlightMigrations
            (source_upstream, target_upstream, bucket, key, state)
        VALUES
            ($1, $2, $3, $4, $5)
        ON CONFLICT DO UPDATE SET
            state = EXCLUDED.state,
            target_upstream = EXCLUDED.target_upstream
        "#,
        pending.source_upstream,
        pending.target_upstream,
        pending.object.bucket,
        pending.object.key,
        migration.state
    )
    .execute(con)
    .await
    .context("failed to add or update in-flight migration")
    .attach(format!("migration: {:?}", migration))?;

    Ok(())
}

pub(crate) async fn delete_in_flight(
    con: &mut SqliteConnection,
    source_upstream: &UpstreamId,
    object: &S3ObjectId,
) -> Result<(), Report> {
    query!(
        r#"
        DELETE FROM InFlightMigrations
        WHERE source_upstream = $1 AND bucket = $2 AND key = $3
        "#,
        source_upstream,
        object.bucket,
        object.key
    )
    .execute(con)
    .await
    .context("failed to delete in-flight migration")
    .attach(format!("upstream: {source_upstream}"))
    .attach(format!("object: {object}"))?;

    Ok(())
}

pub(crate) async fn get_in_flight(
    con: &mut SqliteConnection,
) -> Result<Vec<InFlightMigration>, Report> {
    query_as!(
        DbInFlightMigration,
        r#"
        SELECT
            source_upstream, target_upstream, bucket, key, state as "state: MigrationState"
        FROM InFlightMigrations
        "#
    )
    .map(InFlightMigration::from)
    .fetch_all(con)
    .await
    .context("failed to get pending migrations")
    .map_err(Report::into_dynamic)
}

#[derive(Debug, Clone)]
struct DbInFlightMigration {
    bucket: String,
    key: String,
    source_upstream: UpstreamId,
    target_upstream: UpstreamId,
    state: MigrationState,
}

impl From<DbInFlightMigration> for InFlightMigration {
    fn from(value: DbInFlightMigration) -> Self {
        Self {
            pending: PendingMigration {
                object: S3ObjectId {
                    bucket: value.bucket,
                    key: value.key,
                },
                source_upstream: value.source_upstream,
                target_upstream: value.target_upstream,
            },
            state: value.state,
        }
    }
}
