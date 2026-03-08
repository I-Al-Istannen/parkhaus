use crate::config::{Config, Upstream};
use crate::data::{S3ObjectId, UpstreamId};
use crate::db::Database;
use jiff::{Timestamp, Zoned};
use rootcause::Report;
use rootcause::option_ext::OptionExt;
use rootcause::prelude::ResultExt;
use serde::Serialize;

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
