//! Settles where a dirty object lives by asking the upstreams a change was sent to.

use crate::config::{Config, Upstream};
use crate::data::{S3Object, S3ObjectId, UpstreamId};
use crate::db::{Database, DirtyObject, Resolution};
use crate::s3::client::{ObjectInfo, S3Client};
use crate::server::state::KeyLocks;
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use tracing::{info, warn};

/// Resolves a dirty object. The caller must hold the object's key lock.
///
/// The target only holds the key if the change created it there, so a copy on the target wins.
/// Otherwise the previous upstream still holds the object, or it is gone. The DB is updated before
/// the stale copy is deleted, so readers never see neither, and a crash in between leads to the
/// same decision again.
pub async fn resolve(
    config: &Config,
    db: &Database,
    http: &Client,
    obj: &S3ObjectId,
) -> Result<(), Report> {
    let Some(DirtyObject {
        target,
        previous,
        migration,
    }) = db.find_dirty(obj).await?
    else {
        return Ok(());
    };
    let known = db.find_object(obj).await?;

    let (winner, stale) = match head(config, http, &target, obj).await? {
        Some(found) => (Some(found), previous.as_ref()),
        None => match &previous {
            Some(previous) => (head(config, http, previous, obj).await?, None),
            None => (None, None),
        },
    };

    let resolution = match (winner, &known) {
        (Some((upstream, info)), Some(known))
            if upstream.name == known.assigned_upstream
                && info.size == known.size
                && info.last_modified == known.last_modified =>
        {
            Resolution::Keep
        }
        // The copy is new, but the object is not: age and accesses must carry over.
        (Some((upstream, _)), Some(_)) if migration => Resolution::Move(upstream.name.clone()),
        (Some((upstream, info)), _) => Resolution::Record(S3Object {
            id: obj.clone(),
            assigned_upstream: upstream.name.clone(),
            last_modified: info.last_modified,
            size: info.size,
        }),
        // Defensive: a row on an upstream the change did not touch is still accurate.
        (None, Some(known))
            if known.assigned_upstream != target
                && Some(&known.assigned_upstream) != previous.as_ref() =>
        {
            Resolution::Keep
        }
        (None, _) => Resolution::Delete,
    };
    db.apply_resolution(obj, &resolution).await?;

    if let Some(stale) = stale.and_then(|it| config.upstreams.get(it)) {
        info!(%obj, upstream = %stale.name, "deleting previous copy of object");
        S3Client::for_upstream(http.clone(), stale)
            .delete_file(obj)
            .await
            .context("failed to delete previous copy")
            .attach(format!("upstream: {}", stale.name))?;
    }

    db.clear_dirty(obj).await
}

async fn head<'a>(
    config: &'a Config,
    http: &Client,
    name: &UpstreamId,
    obj: &S3ObjectId,
) -> Result<Option<(&'a Upstream, ObjectInfo)>, Report> {
    let Some(upstream) = config.upstreams.get(name) else {
        warn!(%obj, upstream = %name, "dirty object references unknown upstream, ignoring it");
        return Ok(None);
    };
    Ok(S3Client::for_upstream(http.clone(), upstream)
        .try_head_file(obj)
        .await
        .context("failed to HEAD dirty object")
        .attach(format!("upstream: {name}"))?
        .map(|info| (upstream, info)))
}

/// Resolves all dirty objects, e.g. left behind by a crash.
pub async fn sweep(
    config: &Config,
    db: &Database,
    http: &Client,
    locks: &KeyLocks,
) -> Result<usize, Report> {
    let dirty = db.get_dirty_objects().await?;
    let mut failed = 0;
    for obj in &dirty {
        let _guard = locks.lock(obj).await;
        if let Err(e) = resolve(config, db, http, obj).await {
            warn!(%e, %obj, "failed to resolve dirty object");
            failed += 1;
        }
    }
    if !dirty.is_empty() {
        info!(count = dirty.len(), failed, "resolved dirty objects");
    }
    Ok(failed)
}
