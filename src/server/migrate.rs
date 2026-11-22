use super::metrics::MigrationMetrics;
use crate::config::{Config, Upstream};
use crate::data::{PendingMigration, S3ObjectId};
use crate::db::{Database, DirtyObject};
use crate::s3::client::S3Client;
use crate::server::resolve::{resolve, sweep};
use crate::server::state::KeyLocks;
use jiff::Zoned;
use reqwest::Client;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use rootcause::{Report, bail};
use std::collections::HashSet;
use std::ops::Not;
use std::time::{Duration, SystemTime};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

const SAMPLE_ERRORS: usize = 50;

pub async fn migration_task(
    config: Config,
    db: Database,
    key_locks: KeyLocks,
    shutdown: CancellationToken,
) {
    let http = Client::new();
    let work = async {
        loop {
            // Only check sometimes :)
            tokio::time::sleep(Duration::from_mins(1)).await;

            if let Err(error) = sweep(&config, &db, &http, &key_locks).await {
                error!(%error, "Failed to resolve dirty objects");
            }

            debug!("Computing pending migrations");
            let pending = match compute_pending_migrations(&config, &db, Zoned::now()).await {
                Err(error) => {
                    error!(%error, "Failed to compute new pending migrations");
                    continue;
                }
                Ok(migrations) => migrations,
            };
            match execute_migrations(pending, &config, &db, &key_locks).await {
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

/// Executes the pending migrations and returns the number of failed ones.
pub async fn execute_migrations(
    pending: Vec<PendingMigration>,
    config: &Config,
    db: &Database,
    key_locks: &KeyLocks,
) -> Result<usize, Report> {
    let mut metrics = MigrationMetrics::snapshot(config, &pending);
    debug!(count = pending.len(), "Executing pending migrations");

    let http = Client::new();
    let mut failed = 0;
    let mut sampled_errors = Vec::with_capacity(SAMPLE_ERRORS);
    for migration in &pending {
        match execute_migration(migration, config, db, &http, key_locks).await {
            Ok(()) => metrics.completed(migration),
            Err(e) => {
                debug!(%e, "Failed to execute pending migration");
                metrics.failed(migration);
                failed += 1;
                // Reservoir sampling keeps memory bounded
                if sampled_errors.len() < SAMPLE_ERRORS {
                    sampled_errors.push(e);
                } else if let index = rand::random_range(0..failed)
                    && index < SAMPLE_ERRORS
                {
                    sampled_errors[index] = e;
                }
            }
        }
    }

    for report in sampled_errors {
        error!(%report, "Failed to execute pending migration");
    }
    Ok(failed)
}

/// Copies the object to the target and lets [`resolve`] move the DB entry and delete the source
/// copy. A crash leaves the object dirty, the sweep then finishes or rolls back the migration.
async fn execute_migration(
    migration: &PendingMigration,
    config: &Config,
    db: &Database,
    http: &Client,
    key_locks: &KeyLocks,
) -> Result<(), Report> {
    let PendingMigration {
        object,
        source_upstream: source,
        target_upstream: target,
    } = migration;
    if source == target {
        bail!("Tried to migrate {object} to the upstream it is on ({source})");
    }
    let source_upstream = config
        .upstreams
        .get(source)
        .context("unknown source upstream")
        .attach(format!("upstream: {source}"))?;
    let target_upstream = config
        .upstreams
        .get(target)
        .context("unknown target upstream")
        .attach(format!("upstream: {target}"))?;

    debug!(from = %source, to = %target, %object, "Executing pending migration");
    let _key_guard = key_locks.lock(object).await;
    resolve(config, db, http, object)
        .await
        .context("failed to resolve earlier change")?;
    if db.get_upstream(object).await?.as_ref() != Some(source) {
        debug!(%object, "Object changed since the migration was planned, skipping it");
        return Ok(());
    }

    db.mark_dirty(
        object,
        &DirtyObject {
            target: target.clone(),
            previous: Some(source.clone()),
            migration: true,
        },
    )
    .await?;
    let copied = copy(http, source_upstream, target_upstream, object).await;
    let resolved = resolve(config, db, http, object).await;
    copied
        .context("failed to copy object")
        .attach(format!("from: {source}"))
        .attach(format!("to: {target}"))
        .attach(format!("object: {object}"))?;
    resolved.context("failed to resolve migration")?;

    MigrationMetrics::record_migrated_object();
    debug!(from = %source, to = %target, %object, "Finished migration");
    Ok(())
}

async fn copy(
    http: &Client,
    source: &Upstream,
    target: &Upstream,
    object: &S3ObjectId,
) -> Result<(), Report> {
    let (size, data) = S3Client::for_upstream(http.clone(), source)
        .get_file(object)
        .await
        .context("failed to download object from source upstream")?;
    let size = size.context("source upstream did not report the object size")?;
    S3Client::for_upstream(http.clone(), target)
        .put_file(object, data, size)
        .await
        .context("failed to upload object to target upstream")
        .map_err(Report::into_dynamic)
}
