#![allow(unused_crate_dependencies)]

use common::S3Instance;
use jiff::{SignedDuration, Timestamp};
use parkhaus::config::{Config, S3Secret, Upstream, UpstreamId};
use parkhaus::data::{S3Object, S3ObjectId};
use parkhaus::db::{Database, DirtyObject, MultipartUpload};
use parkhaus::s3::client::S3Client;
use parkhaus::s3::types::AddressingStyle;
use parkhaus::server::KeyLocks;
use parkhaus::server::reconcile::{Discrepancy, reconcile};
use parkhaus::server::settle::{settle, sweep};
use parkhaus::server::uploads::{ABANDONED_UPLOAD_AGE, reap_abandoned_uploads};
use reqwest::Client;
use rootcause::Report;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::sync::OnceCell;

mod common;

const BUCKET: &str = "settle";

static HOT: OnceCell<(S3Instance, Upstream)> = OnceCell::const_new();
static COLD: OnceCell<(S3Instance, Upstream)> = OnceCell::const_new();

#[ctor::dtor]
fn shutdown_backends() {
    [&HOT, &COLD]
        .into_iter()
        .filter_map(OnceCell::get)
        .for_each(|(it, _)| it.shutdown());
}

async fn start(name: &str, order: usize) -> Result<(S3Instance, Upstream), Report> {
    let backend = S3Instance::start().await?;
    let bucket_id = backend.create_bucket(BUCKET).await?;
    let (key_id, secret) = backend.create_key(&format!("settle-{name}")).await?;
    backend.allow_key_on_bucket(&bucket_id, &key_id).await?;
    let upstream = Upstream {
        name: UpstreamId(name.to_string()),
        order,
        base_url: backend.s3_endpoint().clone(),
        addressing_style: AddressingStyle::Path,
        s3_access_key: key_id,
        s3_secret: S3Secret(secret),
        region: backend.region().to_owned(),
    };
    Ok((backend, upstream))
}

struct Env {
    config: Config,
    db: Database,
    http: Client,
}

impl Env {
    async fn new() -> Result<Self, Report> {
        let hot = &HOT.get_or_try_init(|| start("hot", 1)).await?.1;
        let cold = &COLD.get_or_try_init(|| start("cold", 2)).await?.1;
        Ok(Self {
            config: Config::new(
                "127.0.0.1:0".to_string(),
                None,
                PathBuf::new(),
                vec![hot.clone(), cold.clone()],
                Vec::new(),
            )?,
            db: Database::in_memory().await?,
            http: Client::new(),
        })
    }

    fn client(&self, upstream: &UpstreamId) -> S3Client {
        S3Client::for_upstream(self.http.clone(), &self.config.upstreams[upstream])
    }

    async fn put(&self, upstream: &UpstreamId, id: &S3ObjectId, body: &[u8]) -> Result<(), Report> {
        self.client(upstream)
            .put_file(id, Cursor::new(body.to_vec()), body.len() as u64)
            .await
    }

    async fn exists(&self, upstream: &UpstreamId, id: &S3ObjectId) -> Result<bool, Report> {
        Ok(self.client(upstream).try_head_file(id).await?.is_some())
    }

    async fn record(
        &self,
        upstream: &UpstreamId,
        id: &S3ObjectId,
        size: u64,
    ) -> Result<(), Report> {
        self.db
            .record_creation(&S3Object {
                id: id.clone(),
                assigned_upstream: upstream.clone(),
                last_modified: Timestamp::UNIX_EPOCH,
                size,
            })
            .await
    }

    async fn settle(&self, id: &S3ObjectId) -> Result<(), Report> {
        let guard = KeyLocks::default().lock(id).await;
        settle(&self.config, &self.db, &self.http, &guard).await?;
        assert_eq!(self.db.find_dirty(id).await?, None);
        Ok(())
    }

    async fn assigned(&self, id: &S3ObjectId) -> Result<Option<(UpstreamId, u64)>, Report> {
        Ok(self
            .db
            .find_object(id)
            .await?
            .map(|it| (it.assigned_upstream, it.size)))
    }
}

fn hot() -> UpstreamId {
    UpstreamId("hot".to_string())
}

fn cold() -> UpstreamId {
    UpstreamId("cold".to_string())
}

fn dirty(target: UpstreamId, previous: Option<UpstreamId>) -> DirtyObject {
    DirtyObject {
        target,
        previous,
        migration: false,
    }
}

fn object(key: &str) -> S3ObjectId {
    S3ObjectId {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
    }
}

#[tokio::test]
async fn deleted_object_is_forgotten() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("deleted");
    env.record(&hot(), &id, 1).await?;
    env.db.mark_dirty(&id, &dirty(hot(), None)).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, None);
    Ok(())
}

#[tokio::test]
async fn overwritten_object_is_recorded_again() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("overwritten");
    env.record(&hot(), &id, 1).await?;
    env.put(&hot(), &id, b"hello").await?;
    env.db.mark_dirty(&id, &dirty(hot(), None)).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, Some((hot(), 5)));
    Ok(())
}

#[tokio::test]
async fn unchanged_object_keeps_its_row() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("unchanged");
    env.put(&hot(), &id, b"hello").await?;
    let info = env.client(&hot()).head_file(&id).await?;
    let recorded = S3Object {
        id: id.clone(),
        assigned_upstream: hot(),
        last_modified: info.last_modified,
        size: info.size,
    };
    env.db.record_creation(&recorded).await?;
    env.db.mark_dirty(&id, &dirty(hot(), None)).await?;

    env.settle(&id).await?;

    let row = env.db.find_object(&id).await?.expect("row must stay");
    assert_eq!(row.last_modified, info.last_modified);
    assert_eq!(row.size, 5);
    Ok(())
}

#[tokio::test]
async fn new_object_is_recorded() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("new");
    env.put(&hot(), &id, b"new").await?;
    env.db.mark_dirty(&id, &dirty(hot(), None)).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, Some((hot(), 3)));
    Ok(())
}

#[tokio::test]
async fn failed_creation_leaves_nothing() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("never-created");
    env.db.mark_dirty(&id, &dirty(hot(), None)).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, None);
    Ok(())
}

#[tokio::test]
async fn copy_onto_other_upstream_replaces_assigned_copy() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("copied");
    env.put(&hot(), &id, b"old").await?;
    env.record(&hot(), &id, 3).await?;
    env.put(&cold(), &id, b"newer").await?;
    env.db.mark_dirty(&id, &dirty(cold(), Some(hot()))).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, Some((cold(), 5)));
    assert!(
        !env.exists(&hot(), &id).await?,
        "stale copy must be deleted"
    );
    assert!(env.exists(&cold(), &id).await?);
    Ok(())
}

#[tokio::test]
async fn failed_copy_keeps_assigned_copy() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("copy-failed");
    env.put(&hot(), &id, b"old").await?;
    env.record(&hot(), &id, 3).await?;
    env.db.mark_dirty(&id, &dirty(cold(), Some(hot()))).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, Some((hot(), 3)));
    assert!(env.exists(&hot(), &id).await?);
    Ok(())
}

#[tokio::test]
async fn interrupted_migration_is_finished() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("migrated");
    env.put(&hot(), &id, b"data").await?;
    env.put(&cold(), &id, b"data").await?;
    env.record(&cold(), &id, 4).await?;
    env.db.mark_dirty(&id, &dirty(cold(), Some(hot()))).await?;

    env.settle(&id).await?;

    assert_eq!(env.assigned(&id).await?, Some((cold(), 4)));
    assert!(
        !env.exists(&hot(), &id).await?,
        "source copy must be deleted"
    );
    Ok(())
}

#[tokio::test]
async fn object_moved_elsewhere_is_left_alone() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("elsewhere");
    env.record(&UpstreamId("other".to_string()), &id, 1).await?;
    env.db.mark_dirty(&id, &dirty(cold(), Some(hot()))).await?;

    env.settle(&id).await?;

    assert_eq!(
        env.assigned(&id).await?,
        Some((UpstreamId("other".to_string()), 1))
    );
    Ok(())
}

#[tokio::test]
async fn sweep_settles_every_dirty_object() -> Result<(), Report> {
    let env = Env::new().await?;
    let created = object("sweep-created");
    let deleted = object("sweep-deleted");
    env.put(&cold(), &created, b"abc").await?;
    env.record(&cold(), &deleted, 1).await?;
    env.db.mark_dirty(&created, &dirty(cold(), None)).await?;
    env.db.mark_dirty(&deleted, &dirty(cold(), None)).await?;

    let failed = sweep(&env.config, &env.db, &env.http, &KeyLocks::default()).await?;

    assert_eq!(failed, 0);
    assert!(env.db.get_dirty_objects().await?.is_empty());
    assert_eq!(env.assigned(&created).await?, Some((cold(), 3)));
    assert_eq!(env.assigned(&deleted).await?, None);
    Ok(())
}

#[tokio::test]
async fn migration_keeps_the_object_timestamps() -> Result<(), Report> {
    let env = Env::new().await?;
    let id = object("migrated-times");
    env.put(&hot(), &id, b"hello").await?;
    env.put(&cold(), &id, b"hello").await?;
    env.record(&hot(), &id, 5).await?;
    env.db
        .mark_dirty(
            &id,
            &DirtyObject {
                target: cold(),
                previous: Some(hot()),
                migration: true,
            },
        )
        .await?;

    env.settle(&id).await?;

    let row = env.db.find_object(&id).await?.expect("row must stay");
    assert_eq!(row.assigned_upstream, cold());
    assert_eq!(row.last_modified, Timestamp::UNIX_EPOCH);
    assert!(!env.exists(&hot(), &id).await?);
    Ok(())
}

#[tokio::test]
async fn abandoned_uploads_are_reaped() -> Result<(), Report> {
    let env = Env::new().await?;
    let upload = |id: &str| MultipartUpload {
        upload_id: id.to_string(),
        object: object("upload"),
        upstream: hot(),
    };
    let now = Timestamp::now();
    env.db
        .add_upload(
            &upload("old"),
            now - ABANDONED_UPLOAD_AGE - SignedDuration::from_secs(1),
        )
        .await?;
    env.db.add_upload(&upload("recent"), now).await?;

    let reaped = reap_abandoned_uploads(&env.config, &env.db, &env.http, now).await?;

    assert_eq!(reaped, 1);
    assert_eq!(env.db.get_upload_upstream("old").await?, None);
    assert_eq!(env.db.get_upload_upstream("recent").await?, Some(hot()));
    Ok(())
}

#[tokio::test]
async fn reconcile_reports_every_disagreement() -> Result<(), Report> {
    let env = Env::new().await?;
    let bucket = "reconcile";
    for cell in [&HOT, &COLD] {
        let (backend, upstream) = cell.get().expect("started by Env::new");
        let bucket_id = backend.create_bucket(bucket).await?;
        backend
            .allow_key_on_bucket(&bucket_id, &upstream.s3_access_key)
            .await?;
    }
    let id = |key: &str| S3ObjectId {
        bucket: bucket.to_string(),
        key: key.to_string(),
    };

    env.put(&hot(), &id("fine"), b"a").await?;
    env.record(&hot(), &id("fine"), 1).await?;
    env.put(&cold(), &id("untracked"), b"a").await?;
    env.put(&hot(), &id("stale"), b"a").await?;
    env.put(&cold(), &id("stale"), b"a").await?;
    env.record(&hot(), &id("stale"), 1).await?;
    env.record(&cold(), &id("missing"), 1).await?;
    env.put(&cold(), &id("dirty"), b"a").await?;
    env.db
        .mark_dirty(&id("dirty"), &dirty(cold(), None))
        .await?;

    let mut found: Vec<_> = reconcile(&env.config, &env.db, &env.http, &KeyLocks::default())
        .await?
        .into_iter()
        .filter(|it| match it {
            Discrepancy::Untracked { object, .. }
            | Discrepancy::Stale { object, .. }
            | Discrepancy::Missing { object, .. } => object.bucket == bucket,
        })
        .map(|it| it.to_string())
        .collect();
    found.sort();

    assert_eq!(
        found,
        vec![
            "reconcile/missing is assigned to cold but missing there".to_string(),
            "reconcile/stale exists on cold but is assigned to hot".to_string(),
            "reconcile/untracked exists on cold but is not in the database".to_string(),
        ]
    );
    Ok(())
}
