use crate::config::{Config, Upstream};
use crate::data::{S3ObjectId, UpstreamId};
use crate::db::Database;
use crate::s3_client::client::S3Client;
use jiff::{Timestamp, Zoned};
use rootcause::Report;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub enum MigrateAction {
    MoveToUpstream {
        source: UpstreamId,
        target: UpstreamId,
        object: S3ObjectId,
    },
}

struct SortedUpstreams<'a> {
    upstreams: Vec<&'a Upstream>,
}
impl<'a> SortedUpstreams<'a> {
    pub fn new(now: Zoned, upstreams: impl Iterator<Item = &'a Upstream>) -> Self {
        let mut sorted_upstreams = upstreams.into_iter().collect::<Vec<_>>();
        sorted_upstreams.sort_unstable_by(|a, b| match (a.max_age, b.max_age) {
            (Some(a_age), Some(b_age)) => a_age
                .compare((b_age, &now))
                .expect("date comparison failed"),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });

        Self {
            upstreams: sorted_upstreams,
        }
    }
}

pub async fn get_pending_migrations(
    config: &Config,
    db: Database,
) -> Result<Vec<MigrateAction>, Report> {
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
                Ok(MigrateAction::MoveToUpstream {
                    source: upstream.name.clone(),
                    target: find_correct_upstream_for_object(
                        &now,
                        &object,
                        last_modified,
                        sorted_upstreams.upstreams.as_slice(),
                    )
                    .context("found no target upstream for object")
                    .attach(format!("object: {}/{}", object.bucket, object.key))?
                    .name
                    .clone(),
                    object,
                })
            })
            .collect::<Result<Vec<MigrateAction>, Report>>()
            .context("found no correct upstream for some object in migration time range")
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

pub async fn execute_migration_actions(
    actions: Vec<MigrateAction>,
    config: &Config,
    db: Database,
) -> Result<(), Report> {
    let client = reqwest::Client::new();
    let upstream_to_client = config
        .upstreams
        .values()
        .map(|it| (it.name.clone(), S3Client::for_upstream(client.clone(), it)))
        .collect::<HashMap<_, _>>();

    for action in actions {
        execute_migration_action(action, &upstream_to_client, db.clone())
            .await
            .context("failed to execute migration action")?;
    }
    Ok(())
}

async fn execute_migration_action(
    action: MigrateAction,
    upstream_to_client: &HashMap<UpstreamId, S3Client>,
    db: Database,
) -> Result<(), Report> {
    match action {
        MigrateAction::MoveToUpstream {
            source,
            target,
            object,
        } => {
            let source_client = upstream_to_client
                .get(&source)
                .context("unknown upstream")
                .attach(format!("source upstream: {source}"))?;
            let target_client = upstream_to_client
                .get(&target)
                .context("unknown upstream")
                .attach(format!("target upstream: {target}"))?;

            let (size, data) = source_client
                .get_file(&object)
                .await
                .context("failed to download object from source upstream")?;

            target_client
                .put_file(&object, data, size.unwrap_or(0))
                .await
                .context("uploading file failed")?;

            db.set_upstream(&object, &target)
                .await
                .context("failed to update upstream in database")
                .attach(format!("object: {}", object))
                .attach(format!("old upstream: {source}"))
                .attach(format!("new upstream: {target}"))?;

            // TODO: Delete
        }
    }

    Ok(())
}
