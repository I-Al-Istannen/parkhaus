use super::metrics::{MigrationMetrics, MigrationMetricsSnapshot};
use crate::config::Config;
use crate::data::{InFlightMigration, MigrationState, PendingMigration, UpstreamId};
use crate::db::Database;
use crate::s3::client::S3Client;
use crate::server::state::MigrationLocks;
use jiff::Zoned;
use rand::prelude::SliceRandom;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use rootcause::{Report, report};
use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

const SAMPLE_ERRORS: usize = 50;

pub async fn migration_task(
    config: Config,
    db: Database,
    migration_locks: MigrationLocks,
    shutdown: CancellationToken,
) {
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
            match execute_migrations(pending, in_flight, &config, &db, &migration_locks).await {
                Err(error) => error!(%error, "Failed to execute pending migrations"),
                Ok(0) => {}
                Ok(errors) => error!(error_count = errors, "Failed to perform migrations",),
            }

            MigrationMetrics::record_run();

            if let Err(e) = db.cleanup_old_access_times(&Zoned::now()).await {
                error!(%e, "Failed to clean up old access times");
            }
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

    for rule_index in 0..config.tiering_rules.len() {
        let rule = &config.tiering_rules[rule_index];
        let up_until = &config.tiering_rules[..rule_index];
        let start = SystemTime::now();
        let pending = db
            .get_pending_migrations_for_rule(rule, up_until, &rule.to, &now)
            .await
            .context("failed to apply rule")?;
        debug!(
            %rule_index,
            duration=%SystemTime::now().duration_since(start).unwrap_or_default().as_secs_f32(),
            "Applied rule",
        );
        MigrationMetrics::record_rule_migrations(rule_index, &rule.to, pending.len());
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

    Ok(all_migrations)
}

/// Executes a set of migration actions and returns all accumulated errors.
pub async fn execute_migrations(
    pending: Vec<PendingMigration>,
    in_flight: Vec<InFlightMigration>,
    config: &Config,
    db: &Database,
    migration_locks: &MigrationLocks,
) -> Result<usize, Report> {
    let pending = remove_in_flight(&in_flight, pending);
    let mut metrics = MigrationMetrics::snapshot(config, &pending, &in_flight);
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
        let pending = in_flight.pending.clone();
        let res = execute_migration(
            in_flight,
            &mut metrics,
            &upstream_to_client,
            db,
            migration_locks,
        )
        .await
        .context("failed to execute a pending migration")
        .into_report();
        let sample_selected = rand::random_range(0..100) == 1;
        match res {
            Err(e) if errors_printed < SAMPLE_ERRORS && sample_selected => {
                metrics.failed(&pending);
                error!(%e, "Failed to execute pending migration");
                errors_printed += 1;
                total_errors += 1;
            }
            Err(e) => {
                metrics.failed(&pending);
                debug!(%e, "Failed to execute pending migration");
                errors.push(e.into_dynamic());
                total_errors += 1;
            }
            Ok(_) => {}
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
        execute(action).await;
    }

    for action in pending {
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
    metrics: &mut MigrationMetricsSnapshot,
    upstream_to_client: &HashMap<UpstreamId, S3Client>,
    db: &Database,
    migration_locks: &MigrationLocks,
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

    debug!(from = ?source, to = ?target, %object, ?state, "Executing pending migration");
    let _migration_guard = migration_locks.lock_migration(object).await;

    if source == target {
        return Err(report!("Tried to migrate object to the same upstream")
            .attach(format!("object: {object}"))
            .attach(format!("source: {source}"))
            .attach(format!("target: {target}"))
            .attach(format!("state: {state}")));
    }
    if !db.has_object(object).await? {
        return cleanup_deleted_object(db, target_client, action, metrics).await;
    }

    let action = match state {
        MigrationState::CopiedToTarget => action,
        MigrationState::Started => {
            upload_object(db, source_client, target_client, action, metrics).await?
        }
    };
    let action = delete_object(db, source_client, action, metrics).await?;

    MigrationMetrics::record_migrated_object();
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
    metrics: &mut MigrationMetricsSnapshot,
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

    // We mark it as started because we might finish creating the file in the target and then
    // crash. In that case we have two copies, one of them untracked on the target. This is not
    // ideal, so we just redo the migration instead.
    update_in_flight(db, &migration).await?;
    metrics.started(&migration.pending);

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

    let migration = InFlightMigration {
        pending: migration.pending,
        state: MigrationState::CopiedToTarget,
    };
    // If this update fails we do the whole copy again, but that is fine.
    update_in_flight(db, &migration).await?;
    debug!(
        source = ?migration.pending.source_upstream,
        target = ?migration.pending.target_upstream,
        object = %migration.pending.object,
        "Updated database after upload"
    );
    metrics.copied_to_target(&migration.pending);

    Ok(migration)
}

async fn delete_object(
    db: &Database,
    source_client: &S3Client,
    action: InFlightMigration,
    metrics: &mut MigrationMetricsSnapshot,
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

    delete_in_flight(db, pending).await?;
    metrics.completed(pending);
    debug!(
        source = ?pending.source_upstream,
        target = ?pending.target_upstream,
        object = %pending.object,
        "Updated database after deleting object"
    );

    Ok(action.pending)
}

async fn cleanup_deleted_object(
    db: &Database,
    target_client: &S3Client,
    action: InFlightMigration,
    metrics: &mut MigrationMetricsSnapshot,
) -> Result<PendingMigration, Report> {
    let pending = &action.pending;
    info!(state = %action.state, object = %pending.object, "Cleaning up migration for deleted object");

    if matches!(action.state, MigrationState::CopiedToTarget) {
        target_client
            .delete_file(&pending.object)
            .await
            .context("failed to delete object from target upstream")
            .attach(format!("old upstream: {}", &pending.source_upstream))
            .attach(format!("new upstream: {}", &pending.target_upstream))
            .attach(format!("object: {}", &pending.object))?;
    }

    delete_in_flight(db, pending).await?;
    metrics.completed(pending);
    Ok(action.pending)
}

async fn delete_in_flight(db: &Database, pending: &PendingMigration) -> Result<(), Report> {
    db.delete_in_flight(&pending.source_upstream, &pending.object)
        .await
        .context("failed to delete pending migration from database")
        .attach(format!("object: {}", &pending.object))
        .attach(format!("source: {}", &pending.source_upstream))
        .attach(format!("target: {}", &pending.target_upstream))
        .map_err(Report::into_dynamic)
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
