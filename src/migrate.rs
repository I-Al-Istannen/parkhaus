use crate::config::{Config, Upstream};
use crate::data::{MigrationState, PendingMigration, S3ObjectId, UpstreamId};
use crate::db::Database;
use crate::s3_client::client::S3Client;
use futures_util::StreamExt;
use jiff::{Timestamp, Zoned};
use rand::prelude::IndexedRandom;
use rootcause::Report;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const SAMPLE_ERRORS: usize = 3;

struct SortedUpstreams<'a> {
    upstreams: Vec<&'a Upstream>,
}
impl<'a> SortedUpstreams<'a> {
    pub fn new(now: Zoned, upstreams: impl Iterator<Item = &'a Upstream>) -> Self {
        let mut sorted_upstreams = upstreams.into_iter().collect::<Vec<_>>();
        sorted_upstreams.sort_unstable_by(|a, b| match (a.max_age, b.max_age) {
            (Some(a_age), Some(b_age)) => a_age
                .compare((b_age, &now))
                .expect("failed to compare dates"),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });

        Self {
            upstreams: sorted_upstreams,
        }
    }
}

pub async fn migration_task(config: Config, db: Database, shutdown: CancellationToken) {
    let work = async {
        loop {
            // Only check sometimes :)
            tokio::time::sleep(Duration::from_mins(5)).await;

            debug!("Computing pending migrations");
            if let Err(error) = compute_pending(&config, &db).await {
                error!(%error, "Failed to compute new pending migrations");
            }
            match execute_pending(&config, &db).await {
                Err(error) => error!(%error, "Failed to execute pending migrations"),
                Ok(errors) => {
                    error!(
                        error_count = errors.len(),
                        "Failed to perform {} migrations. Sampling {SAMPLE_ERRORS} random errors",
                        errors.len()
                    );
                    for (index, error) in errors.sample(&mut rand::rng(), SAMPLE_ERRORS).enumerate()
                    {
                        error!(%index, %error, "Error")
                    }
                }
            }
            if let Err(error) = db.delete_finished_pending().await {
                error!(%error, "Failed to delete finished pending migrations");
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

async fn compute_pending(config: &Config, db: &Database) -> Result<(), Report> {
    let pending = get_pending_migrations(config, db)
        .await
        .context("failed to retrieve pending migrations")?;
    db.add_all_pending(&pending)
        .await
        .context("failed to add pending migrations to database")?;

    Ok::<(), Report>(())
}

async fn execute_pending(config: &Config, db: &Database) -> Result<Vec<Report>, Report> {
    let pending = db
        .get_pending_with_state(None)
        .await
        .context("failed to retrieve pending migrations")?;
    let errors = execute_pending_migrations(pending, config, db)
        .await
        .context("failed to execute pending migrations")?;

    Ok(errors)
}

pub async fn get_pending_migrations(
    config: &Config,
    db: &Database,
) -> Result<Vec<PendingMigration>, Report> {
    let now = Zoned::now();
    let sorted_upstreams = SortedUpstreams::new(now.clone(), config.upstreams.values());

    let mut all_actions = Vec::new();

    for upstream in config.upstreams.values() {
        let Some(max_age) = upstream.max_age else {
            continue;
        };
        let actions = db
            .get_objects_in_range(&upstream.name, Timestamp::MIN, (&now - max_age).timestamp())
            .await
            .context("get objects in migration time range")
            .attach(format!("upstream: {}", upstream.name))?
            .into_iter()
            .map(|(object, last_modified)| {
                Ok(PendingMigration {
                    source_upstream: upstream.name.clone(),
                    target_upstream: find_correct_upstream_for_object(
                        &now,
                        &object,
                        last_modified,
                        sorted_upstreams.upstreams.as_slice(),
                    )
                    .context("failed to find target upstream for object")
                    .attach(format!("object: {}/{}", object.bucket, object.key))?
                    .name
                    .clone(),
                    object,
                    state: MigrationState::Pending,
                })
            })
            .collect::<Result<Vec<PendingMigration>, Report>>()
            .context("failed to find correct upstream for some object in migration time range")
            .attach(format!("upstream: {}", upstream.name))?;

        all_actions.extend(actions);
    }

    Ok(all_actions)
}

fn find_correct_upstream_for_object<'u>(
    now: &Zoned,
    object: &'_ S3ObjectId,
    last_modified: Timestamp,
    sorted_upstreams: &'_ [&'u Upstream],
) -> Result<&'u Upstream, Report> {
    sorted_upstreams
        .iter()
        .find(|upstream| !upstream.is_too_old(now, last_modified))
        .copied()
        .context("object is too old for all upstreams")
        .attach(format!("object: {}/{}", object.bucket, object.key))
        .attach(format!("last_modified: {last_modified}"))
        .map_err(Report::into_dynamic)
}

/// Executes a set of migration actions and returns all accumulated errors.
pub async fn execute_pending_migrations(
    pending: Vec<PendingMigration>,
    config: &Config,
    db: &Database,
) -> Result<Vec<Report>, Report> {
    let client = reqwest::Client::new();
    let upstream_to_client = config
        .upstreams
        .values()
        .map(|it| (it.name.clone(), S3Client::for_upstream(client.clone(), it)))
        .collect::<HashMap<_, _>>();

    let errors = futures_util::stream::iter(pending)
        .then(|action| async {
            execute_pending_migration(action, &upstream_to_client, db)
                .await
                .context("failed to execute a pending migration")
                .into_report()
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(Result::err)
        .map(|it| it.into_dynamic())
        .collect::<Vec<_>>();

    Ok(errors)
}

async fn execute_pending_migration(
    action: PendingMigration,
    upstream_to_client: &HashMap<UpstreamId, S3Client>,
    db: &Database,
) -> Result<(), Report> {
    let PendingMigration {
        source_upstream: source,
        target_upstream: target,
        state,
        object,
    } = &action;

    if matches!(state, MigrationState::Finished) {
        return Ok(());
    }

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

    if matches!(state, MigrationState::Pending) {
        upload_object(db, source_client, target_client, object, source, target).await?;
    }

    delete_object(db, source_client, &action).await?;

    Ok(())
}

async fn upload_object(
    db: &Database,
    source_client: &S3Client,
    target_client: &S3Client,
    object: &S3ObjectId,
    source: &UpstreamId,
    target: &UpstreamId,
) -> Result<(), Report> {
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
    update_pending_state(db, source, object, MigrationState::CopiedToTarget).await?;

    Ok(())
}

async fn delete_object(
    db: &Database,
    source_client: &S3Client,
    action: &PendingMigration,
) -> Result<(), Report> {
    // This will just return false and succeed if the file is already gone
    source_client
        .delete_file(&action.object)
        .await
        .context("failed to delete object from source upstream")
        .attach(format!("old upstream: {}", &action.source_upstream))
        .attach(format!("new upstream: {}", &action.target_upstream))
        .attach(format!("object: {}", &action.object))?;
    update_pending_state(
        db,
        &action.source_upstream,
        &action.object,
        MigrationState::Finished,
    )
    .await
}

async fn update_pending_state(
    db: &Database,
    source: &UpstreamId,
    object: &S3ObjectId,
    state: MigrationState,
) -> Result<(), Report> {
    db.set_pending_state(source, object, state)
        .await
        .context("failed to update pending migration state in database")
        .attach(format!("object: {}", &object))
        .attach(format!("old upstream: {source}"))
        .attach(format!("new state: {state}"))
        .map_err(Report::into_dynamic)
}
