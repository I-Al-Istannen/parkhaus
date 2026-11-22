mod access_times;
mod dirty;
mod migrate;
mod objects;
mod uploads;

pub use self::dirty::{DirtyObject, Resolution};
pub use self::objects::TieringRuleEnv;
pub use self::uploads::MultipartUpload;
use crate::data::{PendingMigration, S3Object, S3ObjectId, TieringRule, UpstreamId};
use jiff::{Timestamp, Zoned};
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{ConnectOptions, Connection, Pool, Sqlite, query, query_scalar};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::Span;
use tracing_indicatif::span_ext::IndicatifSpanExt;

/// Reads use a pool and see a consistent snapshot thanks to WAL. Writes share a single
/// connection, so they never fail on a locked database.
#[derive(Debug, Clone)]
pub struct Database {
    reads: Pool<Sqlite>,
    writes: Pool<Sqlite>,
    path: Option<PathBuf>,
}

impl Database {
    pub async fn in_memory() -> Result<Self, Report> {
        // See https://github.com/launchbadge/sqlx/issues/2510#issuecomment-2254646669
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(
                SqliteConnectOptions::default()
                    .foreign_keys(true)
                    .read_only(false)
                    .synchronous(SqliteSynchronous::Normal)
                    .pragma("temp_store", "memory")
                    .pragma("mmap_size", "30000000000")
                    .in_memory(true)
                    .with_regexp(),
            )
            .await?;

        Self::migrate(&pool).await?;
        Ok(Self {
            reads: pool.clone(),
            writes: pool,
            path: None,
        })
    }

    pub async fn new(path: &Path) -> Result<Self, Report> {
        let options = SqliteConnectOptions::default()
            .foreign_keys(true)
            .create_if_missing(true)
            .read_only(false)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("temp_store", "memory")
            .pragma("mmap_size", "30000000000")
            .filename(path)
            .with_regexp();
        let writes = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone().optimize_on_close(true, None))
            .await?;
        Self::migrate(&writes).await?;
        let reads = SqlitePool::connect_with(options.read_only(true)).await?;

        Ok(Self {
            reads,
            writes,
            path: Some(path.to_path_buf()),
        })
    }

    async fn migrate(pool: &SqlitePool) -> Result<(), Report> {
        let applied = Self::applied_migrations(pool).await;
        sqlx::migrate!().run(pool).await?;

        if Self::applied_migrations(pool).await != applied {
            // Needs up to twice the size of the database on disk:
            // https://sqlite.org/lang_vacuum.html#how_vacuum_works
            query!("VACUUM")
                .execute(&mut *pool.acquire().await?)
                .await?;
            query("PRAGMA WAL_CHECKPOINT(TRUNCATE)")
                .execute(&mut *pool.acquire().await?)
                .await?;
        }
        Ok(())
    }

    async fn applied_migrations(pool: &SqlitePool) -> i64 {
        query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(pool)
            .await
            .unwrap_or(0)
    }

    async fn read(&self) -> &SqlitePool {
        &self.reads
    }

    async fn write(&self) -> &SqlitePool {
        &self.writes
    }

    pub async fn get_object(&self, obj: &S3ObjectId) -> Result<S3Object, Report> {
        let con = self.read().await;
        objects::get_object(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn find_object(&self, obj: &S3ObjectId) -> Result<Option<S3Object>, Report> {
        let con = self.read().await;
        objects::find_object(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn has_object(&self, obj: &S3ObjectId) -> Result<bool, Report> {
        let con = self.read().await;
        objects::has_object(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn get_upstream(&self, obj: &S3ObjectId) -> Result<Option<UpstreamId>, Report> {
        let con = self.read().await;
        objects::get_upstream(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn delete_object(&self, obj: &S3ObjectId) -> Result<(), Report> {
        let con = self.write().await;
        objects::delete_object(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn record_creation(&self, obj: &S3Object) -> Result<(), Report> {
        let con = self.write().await;
        objects::record_creation(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn record_access(&self, obj: &S3ObjectId, now: &Zoned) -> Result<(), Report> {
        self.record_accesses(&[(obj.clone(), 1)], now).await
    }

    /// Adds `count` accesses to each known object, ignoring unknown ones.
    pub async fn record_accesses(
        &self,
        accesses: &[(S3ObjectId, i64)],
        now: &Zoned,
    ) -> Result<(), Report> {
        let con = self.write().await;
        let mut transaction = con.begin().await.context("begin transaction")?;
        for (obj, count) in accesses {
            access_times::record_access(&mut transaction, obj, *count, now).await?;
        }
        transaction.commit().await.context("commit accesses")?;
        Ok(())
    }

    /// This calls [record_creation] for all objects in batches. It also updates the progress bar
    /// of the current span.
    pub async fn bulk_import_creations(
        &self,
        objects: &[S3Object],
        keep_last_modified: bool,
    ) -> Result<(), Report> {
        let con = self.write().await;
        const BATCH_SIZE: usize = 10_000; // randomly chosen (after some experiments)

        for batch in objects.chunks(BATCH_SIZE) {
            let mut transaction = con.begin().await.context("begin transaction")?;
            for obj in batch {
                if keep_last_modified {
                    objects::record_creation_keep_last_modified(&mut transaction, obj).await?;
                } else {
                    objects::record_creation(&mut transaction, obj).await?;
                }
            }
            transaction.commit().await?;
            Span::current().pb_inc(batch.len() as u64);
        }

        Ok(())
    }

    pub async fn get_objects_not_in_range(
        &self,
        upstream: &UpstreamId,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<(S3ObjectId, Timestamp)>, Report> {
        let con = self.read().await;
        migrate::get_objects_not_in_range(
            &mut *con.acquire().await.context("acquire con")?,
            upstream,
            start,
            end,
        )
        .await
    }

    pub async fn mark_dirty(&self, obj: &S3ObjectId, dirty: &DirtyObject) -> Result<(), Report> {
        let con = self.write().await;
        dirty::mark(
            &mut *con.acquire().await.context("acquire con")?,
            obj,
            dirty,
        )
        .await
    }

    pub async fn find_dirty(&self, obj: &S3ObjectId) -> Result<Option<DirtyObject>, Report> {
        let con = self.read().await;
        dirty::find(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn get_dirty_objects(&self) -> Result<Vec<S3ObjectId>, Report> {
        let con = self.read().await;
        dirty::get_all(&mut *con.acquire().await.context("acquire con")?).await
    }

    /// Applies the resolution but keeps the object dirty.
    pub async fn apply_resolution(
        &self,
        obj: &S3ObjectId,
        resolution: &Resolution,
    ) -> Result<(), Report> {
        let con = self.write().await;
        dirty::apply(
            &mut *con.acquire().await.context("acquire con")?,
            obj,
            resolution,
        )
        .await
    }

    pub async fn clear_dirty(&self, obj: &S3ObjectId) -> Result<(), Report> {
        let con = self.write().await;
        dirty::clear(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    /// Applies the resolution and clears the dirty mark atomically.
    pub async fn resolve_dirty(
        &self,
        obj: &S3ObjectId,
        resolution: &Resolution,
    ) -> Result<(), Report> {
        let con = self.write().await;
        let mut transaction = con.begin().await.context("begin transaction")?;
        dirty::apply(&mut transaction, obj, resolution).await?;
        dirty::clear(&mut transaction, obj).await?;
        transaction.commit().await.context("commit transaction")?;
        Ok(())
    }

    pub async fn add_upload(
        &self,
        upload: &MultipartUpload,
        created: Timestamp,
    ) -> Result<(), Report> {
        let con = self.write().await;
        uploads::add(
            &mut *con.acquire().await.context("acquire con")?,
            upload,
            created,
        )
        .await
    }

    pub async fn get_upload_upstream(&self, upload_id: &str) -> Result<Option<UpstreamId>, Report> {
        let con = self.read().await;
        uploads::get_upstream(&mut *con.acquire().await.context("acquire con")?, upload_id).await
    }

    pub async fn remove_upload(&self, upload_id: &str) -> Result<(), Report> {
        let con = self.write().await;
        uploads::remove(&mut *con.acquire().await.context("acquire con")?, upload_id).await
    }

    pub async fn get_uploads_older_than(
        &self,
        cutoff: Timestamp,
    ) -> Result<Vec<MultipartUpload>, Report> {
        let con = self.read().await;
        uploads::get_older_than(&mut *con.acquire().await.context("acquire con")?, cutoff).await
    }

    pub async fn get_assignments_in_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<(String, UpstreamId)>, Report> {
        let con = self.read().await;
        objects::get_assignments_in_bucket(
            &mut *con.acquire().await.context("acquire con")?,
            bucket,
        )
        .await
    }

    pub async fn get_all_buckets(&self) -> Result<HashSet<String>, Report> {
        let con = self.read().await;
        objects::get_all_buckets(&mut *con.acquire().await.context("acquire con")?).await
    }

    pub async fn get_pending_migrations_for_rule(
        &self,
        rule: &TieringRule,
        exclude: &[TieringRule],
        rule_to_upstream: &UpstreamId,
        now: &Zoned,
    ) -> Result<Vec<PendingMigration>, Report> {
        let con = self.read().await;
        objects::get_pending_migrations_for_rule(
            &mut *con.acquire().await.context("acquire con")?,
            rule,
            exclude,
            rule_to_upstream,
            now,
        )
        .await
    }

    pub async fn get_num_of_objects_without_size(&self) -> Result<HashMap<String, usize>, Report> {
        let con = self.read().await;
        objects::get_num_of_objects_without_size(&mut *con.acquire().await.context("acquire con")?)
            .await
    }

    pub async fn cleanup_old_access_times(&self, now: &Zoned) -> Result<(), Report> {
        let con = self.write().await;
        access_times::cleanup_old(&mut *con.acquire().await.context("acquire con")?, now).await
    }

    pub async fn close(self) -> Result<(), Report> {
        // Close the existing connection to ensure it does not block cleanup
        self.reads.close().await;
        self.writes.close().await;

        if let Some(path) = self.path.as_ref() {
            // Clean up WAL files. This should happen above but sqlx does not correctly close sqlite
            // See: https://github.com/launchbadge/sqlx/issues/2249
            SqliteConnectOptions::default()
                .journal_mode(SqliteJournalMode::Wal)
                .filename(path)
                .connect()
                .await?
                .close()
                .await?;
        }

        Ok(())
    }
}
