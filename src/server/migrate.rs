use super::metrics::{
    COUNTER_MIGRATED_OBJECTS_TOTAL, COUNTER_MIGRATION_RUNS_TOTAL, GAUGE_PENDING_ACTIONS,
};
use crate::config::Config;
use crate::data::{InFlightMigration, MigrationState, PendingMigration, UpstreamId};
use crate::db::Database;
use crate::s3::client::S3Client;
use axum_prometheus::metrics::{counter, gauge};
use jiff::Zoned;
use rand::prelude::SliceRandom;
use rootcause::Report;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::time::Duration;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

const SAMPLE_ERRORS: usize = 50;
const METRICS_STATE_PENDING: &str = "Pending";
const METRICS_STATE_COMPLETED: &str = "Completed";

pub async fn migration_task(config: Config, db: Database, shutdown: CancellationToken) {
    let work = async {
        loop {
            // Only check sometimes :)
            tokio::time::sleep(Duration::from_mins(1)).await;

            debug!("Computing pending migrations");
            let pending = match compute_pending_migrations(&config, &db, Zoned::now()).await {
                Err(error) => {
                    error!(%error, "Failed to compute new pending migrations");
                    continue;
                }
                Ok(migrations) => migrations,
            };
            let in_flight = db.get_in_flight().await.unwrap_or_else(|error| {
                error!(%error, "Failed to get in-flight migrations");
                Vec::new()
            });
            metrics_start_run(&config, &pending, &in_flight);
            match execute_migrations(pending, in_flight, &config, &db).await {
                Err(error) => error!(%error, "Failed to execute pending migrations"),
                Ok(errors) => error!(error_count = errors, "Failed to perform migrations",),
            }

            counter!(COUNTER_MIGRATION_RUNS_TOTAL).increment(1);
        }
    };
    select!(
        _ = work => {
            error!("Migration task finished unexpectedly");
        },
        _ = shutdown.cancelled() => {
            info!("Cancelling migration task due to imminent shutdown")
        }
    )
}

pub async fn compute_pending_migrations(
    config: &Config,
    db: &Database,
    now: Zoned,
) -> Result<Vec<PendingMigration>, Report> {
    let mut all_migrations = Vec::new();

    for rule in &config.tiering_rules {
        let pending = db
            .get_pending_migrations_for_rule(rule, &now)
            .await
            .context("failed to apply rule")?;
        all_migrations.extend(pending);
    }

    let mut set = HashSet::new();
    all_migrations.retain(|migration| set.insert(migration.object.clone()));

    if set.is_empty().not() {
        debug!(
            covered_twice=%set.len(),
            "Overlapping rules found, covered {} objects more than once. Prioritizing first rule.",
            set.len()
        )
    }
    for object in set {
        trace!(%object, "Covered object more than once");
    }

    // Remove migrations that don't do anything. This must be done at the end to handle this case:
    // hot : age <= 2d
    // warm: age <= 3d
    // In this case an object in hot matches both, hot and warm. It currently is in hot though,
    // but this migration _must not_ be dropped, otherwise it is migrated to warm.
    // When in warm this happens in reverse and it is moved back up to hot, ping-ponging
    // between the two. Therefore, we retain "bogus" no-op migrations up until we realize them here,
    // to allow the user to write overlapping rules that work as intuitively expected.
    all_migrations.retain(|migration| migration.target_upstream != migration.source_upstream);

    Ok(all_migrations)
}

/// Executes a set of migration actions and returns all accumulated errors.
pub async fn execute_migrations(
    pending: Vec<PendingMigration>,
    in_flight: Vec<InFlightMigration>,
    config: &Config,
    db: &Database,
) -> Result<usize, Report> {
    let pending = remove_in_flight(&in_flight, pending);
    debug!(count = pending.len(), "Executing pending migrations");

    let client = reqwest::Client::new();
    let upstream_to_client = config
        .upstreams
        .values()
        .map(|it| (it.name.clone(), S3Client::for_upstream(client.clone(), it)))
        .collect::<HashMap<_, _>>();

    let mut errors_printed = 0;
    let mut errors = Vec::new();
    let mut total_errors = 0;

    let mut execute = async |in_flight: InFlightMigration| {
        let res = execute_migration(in_flight, &upstream_to_client, db)
            .await
            .context("failed to execute a pending migration")
            .into_report();
        let sample_selected = rand::random_range(0..100) == 1;
        match res {
            Err(e) if errors_printed < SAMPLE_ERRORS && sample_selected => {
                error!(%e, "Failed to execute pending migration");
                errors_printed += 1;
                total_errors += 1;
            }
            Err(e) => {
                debug!(%e, "Failed to execute pending migration");
                errors.push(e.into_dynamic());
                total_errors += 1;
            }
            Ok(migration) => metrics_change_state(
                &migration,
                MigrationState::CopiedToTarget.to_string(),
                METRICS_STATE_COMPLETED.to_string(),
            ),
        }
    };

    for action in in_flight {
        info!(
            from = ?action.pending.source_upstream,
            to = ?action.pending.source_upstream,
            object = %action.pending.object,
            state = %action.state,
            "Retrying in-flight migration"
        );
        metrics_change_state(
            &action.pending,
            action.state.to_string(),
            MigrationState::Started.to_string(),
        );
        execute(action).await;
    }

    for action in pending {
        metrics_change_state(
            &action,
            METRICS_STATE_PENDING.to_string(),
            MigrationState::Started.to_string(),
        );
        execute(InFlightMigration {
            pending: action,
            state: MigrationState::Started,
        })
        .await;
    }

    errors.shuffle(&mut rand::rng());
    while errors_printed < SAMPLE_ERRORS
        && let Some(report) = errors.pop()
    {
        error!(%report, "Failed to execute pending migration");
        errors_printed += 1;
    }

    Ok(total_errors)
}

fn remove_in_flight(
    in_flight: &[InFlightMigration],
    pending: Vec<PendingMigration>,
) -> Vec<PendingMigration> {
    let keys = in_flight
        .iter()
        .map(|it| (&it.pending.source_upstream, &it.pending.object))
        .collect::<HashSet<_>>();

    pending
        .into_iter()
        .filter(|it| !keys.contains(&(&it.source_upstream, &it.object)))
        .collect()
}

async fn execute_migration(
    action: InFlightMigration,
    upstream_to_client: &HashMap<UpstreamId, S3Client>,
    db: &Database,
) -> Result<PendingMigration, Report> {
    let InFlightMigration {
        pending:
            PendingMigration {
                source_upstream: source,
                target_upstream: target,
                object,
            },
        state,
    } = &action;

    debug!(from = ?source, to = ?target, %object, ?state, "Executing pending migration");

    let source_client = upstream_to_client
        .get(source)
        .context("unknown upstream")
        .attach(format!("source upstream: {source}"))
        .attach(format!("object: {object}"))?;
    let target_client = upstream_to_client
        .get(target)
        .context("unknown upstream")
        .attach(format!("target upstream: {target}"))
        .attach(format!("object: {object}"))?;

    let action = match state {
        MigrationState::CopiedToTarget => action,
        MigrationState::Started => upload_object(db, source_client, target_client, action).await?,
    };
    let action = delete_object(db, source_client, action).await?;

    counter!(COUNTER_MIGRATED_OBJECTS_TOTAL).increment(1);
    debug!(
        from = ?action.source_upstream,
        to = ?action.target_upstream,
        object = %action.object,
        "Finished migration"
    );

    Ok(action)
}

async fn upload_object(
    db: &Database,
    source_client: &S3Client,
    target_client: &S3Client,
    migration: InFlightMigration,
) -> Result<InFlightMigration, Report> {
    let source = &migration.pending.source_upstream;
    let target = &migration.pending.target_upstream;
    let object = &migration.pending.object;

    debug!(source = ?source, target = ?target, %object, "Uploading object");
    let (size, data) = source_client
        .get_file(object)
        .await
        .context("failed to download object from source upstream")
        .attach(format!("source upstream: {source}"))
        .attach(format!("object: {object}"))?;

    target_client
        .put_file(object, data, size.unwrap_or(0))
        .await
        .context("failed to upload file")
        .attach(format!("object upstream: {target}"))
        .attach(format!("object: {object}"))?;
    debug!(source = ?source, target = ?target, %object, "Uploaded object");

    // At this point we have copied the file over, so we can adjust the upstream.
    // We also _have_ to adjust it, as we then delete the file and failures during
    // deletion might still leave the object removed from source!
    db.set_upstream(object, target)
        .await
        .context("failed to update upstream in database")
        .attach(format!("object: {}", &object))
        .attach(format!("old upstream: {source}"))
        .attach(format!("new upstream: {target}"))?;

    // If this update fails we do the whole copy again, but that is fine.
    let migration = InFlightMigration {
        pending: migration.pending,
        state: MigrationState::CopiedToTarget,
    };
    update_in_flight(db, &migration).await?;
    debug!(
        source = ?migration.pending.source_upstream,
        target = ?migration.pending.target_upstream,
        object = %migration.pending.object,
        "Updated database after upload"
    );
    metrics_change_state(
        &migration.pending,
        MigrationState::Started.to_string(),
        migration.state.to_string(),
    );

    Ok(migration)
}

async fn delete_object(
    db: &Database,
    source_client: &S3Client,
    action: InFlightMigration,
) -> Result<PendingMigration, Report> {
    let pending = &action.pending;
    debug!(
        source = ?pending.source_upstream,
        target = ?pending.target_upstream,
        object = %pending.object,
        "Deleting object"
    );
    // This will just return false and succeed if the file is already gone
    source_client
        .delete_file(&pending.object)
        .await
        .context("failed to delete object from source upstream")
        .attach(format!("old upstream: {}", &pending.source_upstream))
        .attach(format!("new upstream: {}", &pending.target_upstream))
        .attach(format!("object: {}", &pending.object))?;

    debug!(
        source = ?pending.source_upstream,
        target = ?pending.target_upstream,
        object = %pending.object,
        "Deleted object"
    );

    db.delete_in_flight(&pending.source_upstream, &pending.object)
        .await
        .context("failed to delete pending migration from database")
        .attach(format!("object: {}", &pending.object))
        .attach(format!("source: {}", &pending.source_upstream))
        .attach(format!("target: {}", &pending.target_upstream))?;
    debug!(
        source = ?pending.source_upstream,
        target = ?pending.target_upstream,
        object = %pending.object,
        "Updated database after deleting object"
    );

    Ok(action.pending)
}

async fn update_in_flight(db: &Database, migration: &InFlightMigration) -> Result<(), Report> {
    let pending = &migration.pending;
    db.upsert_in_flight_migration(migration)
        .await
        .context("failed to update pending migration state in database")
        .attach(format!("object: {}", &pending.object))
        .attach(format!("old upstream: {}", pending.source_upstream))
        .attach(format!("new state: {}", migration.state))
        .map_err(Report::into_dynamic)
}

fn metrics_start_run(
    config: &Config,
    pending: &[PendingMigration],
    in_flight: &[InFlightMigration],
) {
    metrics_reset(config);
    for migration in in_flight {
        gauge!(GAUGE_PENDING_ACTIONS,
            "source" => migration.pending.source_upstream.0.clone(),
            "target" => migration.pending.target_upstream.0.clone(),
            "state" => migration.state.to_string()
        )
        .increment(1);
    }
    for migration in pending {
        gauge!(GAUGE_PENDING_ACTIONS,
            "source" => migration.source_upstream.0.clone(),
            "target" => migration.target_upstream.0.clone(),
            "state" => METRICS_STATE_PENDING
        )
        .increment(1);
    }
}

fn metrics_reset(config: &Config) {
    let states = [
        METRICS_STATE_PENDING.to_string(),
        METRICS_STATE_COMPLETED.to_string(),
        MigrationState::CopiedToTarget.to_string(),
        MigrationState::Started.to_string(),
    ];
    for source in config.upstreams.keys() {
        for target in config.upstreams.keys() {
            if source == target {
                continue;
            }
            for state in &states {
                gauge!(GAUGE_PENDING_ACTIONS,
                    "source" => source.0.clone(),
                    "target" => target.0.clone(),
                    "state" => state.clone()
                )
                .set(0.0);
            }
        }
    }
}

fn metrics_change_state(pending: &PendingMigration, from: String, to: String) {
    gauge!(GAUGE_PENDING_ACTIONS,
        "source" => pending.source_upstream.0.clone(),
        "target" => pending.target_upstream.0.clone(),
        "state" => from
    )
    .decrement(1);
    gauge!(GAUGE_PENDING_ACTIONS,
        "source" => pending.source_upstream.0.clone(),
        "target" => pending.target_upstream.0.clone(),
        "state" => to.to_string()
    )
    .increment(1);
}
