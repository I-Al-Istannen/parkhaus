use crate::data::{MigrationState, PendingMigration, S3ObjectId, UpstreamId};
use jiff::Timestamp;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use sqlx::{SqliteConnection, query, query_as};

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

pub(super) async fn add_pending(
    con: &mut SqliteConnection,
    migration: &PendingMigration,
) -> Result<(), Report> {
    query!(
        r#"
        INSERT INTO PendingMigrations
            (source_upstream, target_upstream, bucket, key, state)
        VALUES
            ($1, $2, $3, $4, $5)
        ON CONFLICT DO NOTHING
        "#,
        migration.source_upstream,
        migration.target_upstream,
        migration.object.bucket,
        migration.object.key,
        migration.state
    )
    .execute(con)
    .await
    .context("failed to add pending migration")
    .attach(format!("migration: {:?}", migration))?;

    Ok(())
}

pub(super) async fn set_pending_state(
    con: &mut SqliteConnection,
    source_upstream: &UpstreamId,
    object: &S3ObjectId,
    state: MigrationState,
) -> Result<(), Report> {
    let res = query!(
        r#"
        UPDATE PendingMigrations
        SET state = $1
        WHERE source_upstream = $2 AND bucket = $3 AND key = $4
        "#,
        state,
        source_upstream,
        object.bucket,
        object.key
    )
    .execute(con)
    .await
    .context("failed to set pending migration state")
    .attach(format!("upstream: {source_upstream}"))
    .attach(format!("object: {object}"))
    .attach(format!("new state: {state:?}"))?;

    if res.rows_affected() == 0 {
        return Err(
            report!("failed to find existing pending migration to update")
                .attach(format!("upstream: {source_upstream}"))
                .attach(format!("object: {object}"))
                .attach(format!("new state: {state:?}")),
        );
    }

    Ok(())
}

pub(super) async fn delete_pending(
    con: &mut SqliteConnection,
    source_upstream: &UpstreamId,
    object: &S3ObjectId,
) -> Result<(), Report> {
    query!(
        r#"
        DELETE FROM PendingMigrations
               WHERE
                   source_upstream = $1 AND bucket = $2 AND key = $3 "#,
        source_upstream,
        object.bucket,
        object.key
    )
    .execute(con)
    .await
    .context("failed to delete pending migration")
    .attach(format!("upstream: {source_upstream}"))
    .attach(format!("object: {object}"))?;

    Ok(())
}

pub(super) async fn delete_finished_pending(con: &mut SqliteConnection) -> Result<(), Report> {
    query!(
        r#"
        DELETE FROM PendingMigrations
        WHERE state = 'Finished'
        "#
    )
    .execute(con)
    .await
    .context("failed to delete finished pending migrations")?;

    Ok(())
}

pub(super) async fn get_pending_with_state(
    con: &mut SqliteConnection,
    state: Option<MigrationState>,
) -> Result<Vec<PendingMigration>, Report> {
    let res = query_as!(
        DbPendingMigration,
        r#"
        SELECT
            source_upstream, target_upstream, bucket, key, state as "state: MigrationState"
        FROM PendingMigrations"#,
    )
    .map(PendingMigration::from)
    .fetch_all(con)
    .await
    .context("failed to get pending migrations")
    .attach(format!("state: {:?}", state))?;

    Ok(res)
}

#[derive(Debug, Clone)]
struct DbPendingMigration {
    bucket: String,
    key: String,
    source_upstream: UpstreamId,
    target_upstream: UpstreamId,
    state: MigrationState,
}

impl From<DbPendingMigration> for PendingMigration {
    fn from(value: DbPendingMigration) -> Self {
        Self {
            object: S3ObjectId {
                bucket: value.bucket,
                key: value.key,
            },
            source_upstream: value.source_upstream,
            target_upstream: value.target_upstream,
            state: value.state,
        }
    }
}
