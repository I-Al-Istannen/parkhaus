//! Compares the upstreams with the DB. With the invariant intact this finds nothing, so every
//! finding points at a bug, data loss upstream or someone bypassing parkhaus.

use crate::config::Config;
use crate::data::{S3ObjectId, UpstreamId, is_addressable_key};
use crate::db::Database;
use crate::s3::client::S3Client;
use crate::server::metrics::GAUGE_RECONCILE_DISCREPANCIES;
use crate::server::state::KeyLocks;
use axum_prometheus::metrics::gauge;
use derive_more::Display;
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, PartialEq, Eq, Display)]
pub enum Discrepancy {
    #[display("{object} exists on {upstream} but is not in the database")]
    Untracked {
        object: S3ObjectId,
        upstream: UpstreamId,
    },
    #[display("{object} exists on {upstream} but is assigned to {assigned}")]
    Stale {
        object: S3ObjectId,
        upstream: UpstreamId,
        assigned: UpstreamId,
    },
    #[display("{object} is assigned to {assigned} but missing there")]
    Missing {
        object: S3ObjectId,
        assigned: UpstreamId,
    },
}

const INTERVAL: Duration = Duration::from_hours(24);
const MAX_LOGGED: usize = 50;

/// Reconciles once a day and exposes the result as a metric.
pub async fn reconcile_task(
    config: Arc<Config>,
    db: Database,
    http: Client,
    locks: KeyLocks,
    shutdown: CancellationToken,
) {
    let work = async {
        loop {
            match reconcile(&config, &db, &http, &locks).await {
                Ok(found) => {
                    gauge!(GAUGE_RECONCILE_DISCREPANCIES).set(found.len() as f64);
                    for discrepancy in found.iter().take(MAX_LOGGED) {
                        warn!(%discrepancy, "Upstreams and database disagree");
                    }
                }
                Err(error) => error!(%error, "Failed to reconcile upstreams with the database"),
            }
            tokio::time::sleep(INTERVAL).await;
        }
    };
    select! {
        _ = work => {}
        _ = shutdown.cancelled() => {}
    }
}

/// Lists all upstreams and checks every object where a listing disagrees with the DB again under
/// its key lock, so changes during the listing are not reported.
pub async fn reconcile(
    config: &Config,
    db: &Database,
    http: &Client,
    locks: &KeyLocks,
) -> Result<Vec<Discrepancy>, Report> {
    let clients: Vec<(&UpstreamId, S3Client)> = config
        .upstreams_in_order()
        .into_iter()
        .map(|it| (&it.name, S3Client::for_upstream(http.clone(), it)))
        .collect();

    let mut buckets = db.get_all_buckets().await?;
    for (_, client) in &clients {
        buckets.extend(client.list_buckets().await?.into_iter().map(|it| it.name));
    }

    let mut candidates = BTreeSet::new();
    for bucket in &buckets {
        debug!(%bucket, "Reconciling bucket");
        let mut listed: BTreeMap<String, Vec<&UpstreamId>> = BTreeMap::new();
        for (name, client) in &clients {
            let objects = client
                .list_objects(bucket)
                .await
                .context("failed to list bucket")
                .attach(format!("bucket: {bucket}"))
                .attach(format!("upstream: {name}"))?;
            for object in objects {
                listed.entry(object.key).or_default().push(*name);
            }
        }

        let assignments: BTreeMap<String, UpstreamId> = db
            .get_assignments_in_bucket(bucket)
            .await?
            .into_iter()
            .collect();
        let disagree = |key: &String| match (listed.get(key), assignments.get(key)) {
            (Some(holders), Some(assigned)) => holders.as_slice() != [assigned],
            (None, None) => false,
            _ => true,
        };
        candidates.extend(
            listed
                .keys()
                .chain(assignments.keys())
                .filter(|key| is_addressable_key(key) && disagree(key))
                .map(|key| S3ObjectId {
                    bucket: bucket.clone(),
                    key: key.clone(),
                }),
        );
    }

    let mut discrepancies = Vec::new();
    for object in candidates {
        discrepancies.extend(verify(db, &clients, locks, &object).await?);
    }
    info!(count = discrepancies.len(), "Finished reconciliation");
    Ok(discrepancies)
}

async fn verify(
    db: &Database,
    clients: &[(&UpstreamId, S3Client)],
    locks: &KeyLocks,
    object: &S3ObjectId,
) -> Result<Vec<Discrepancy>, Report> {
    let _guard = locks.lock(object).await;
    if db.find_dirty(object).await?.is_some() {
        return Ok(Vec::new());
    }
    let assigned = db.get_upstream(object).await?;
    let mut present = Vec::new();
    for (name, client) in clients {
        if client.try_head_file(object).await?.is_some() {
            present.push((*name).clone());
        }
    }

    let mut found = Vec::new();
    for upstream in &present {
        match &assigned {
            None => found.push(Discrepancy::Untracked {
                object: object.clone(),
                upstream: upstream.clone(),
            }),
            Some(assigned) if assigned != upstream => found.push(Discrepancy::Stale {
                object: object.clone(),
                upstream: upstream.clone(),
                assigned: assigned.clone(),
            }),
            Some(_) => {}
        }
    }
    if let Some(assigned) = assigned
        && !present.contains(&assigned)
    {
        found.push(Discrepancy::Missing {
            object: object.clone(),
            assigned,
        });
    }
    Ok(found)
}
