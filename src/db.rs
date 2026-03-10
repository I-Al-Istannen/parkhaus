mod migrate;
mod objects;

use crate::data::{S3Object, S3ObjectId, UpstreamId};
use jiff::Timestamp;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteSynchronous};
use sqlx::{ConnectOptions, Connection, Pool, Sqlite, query};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone)]
pub struct Database {
    con: Arc<RwLock<Pool<Sqlite>>>,
    path: PathBuf,
}

impl Database {
    pub async fn new(path: &Path) -> Result<Self, Report> {
        let pool = SqlitePool::connect_with(
            SqliteConnectOptions::default()
                .foreign_keys(true)
                .create_if_missing(true)
                .read_only(false)
                .journal_mode(SqliteJournalMode::Wal)
                .optimize_on_close(true, None)
                .synchronous(SqliteSynchronous::Normal)
                .pragma("temp_store", "memory")
                .pragma("mmap_size", "30000000000")
                .filename(path),
        )
        .await?;

        sqlx::migrate!().run(&pool).await?;

        // This might duplicate the database according to the docs:
        // https://sqlite.org/lang_vacuum.html#how_vacuum_works
        query!("VACUUM")
            .execute(&mut *pool.acquire().await?)
            .await?;
        // Therefore, we now also checkpoint it
        query("PRAGMA WAL_CHECKPOINT(TRUNCATE)")
            .execute(&mut *pool.acquire().await?)
            .await?;

        Ok(Self {
            con: Arc::new(RwLock::new(pool)),
            path: path.to_owned(),
        })
    }

    async fn read(&self) -> RwLockReadGuard<'_, SqlitePool> {
        self.con.read().await
    }

    async fn write(&self) -> RwLockWriteGuard<'_, SqlitePool> {
        self.con.write().await
    }

    pub async fn get_object(&self, obj: &S3ObjectId) -> Result<S3Object, Report> {
        let con = self.read().await;
        objects::get_object(&mut *con.acquire().await.context("acquire con")?, obj).await
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

    pub async fn get_objects_in_range(
        &self,
        upstream: &UpstreamId,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<(S3ObjectId, Timestamp)>, Report> {
        let con = self.read().await;
        migrate::get_objects_in_range(
            &mut *con.acquire().await.context("acquire con")?,
            upstream,
            start,
            end,
        )
        .await
    }

    pub async fn set_upstream(
        &self,
        obj: &S3ObjectId,
        upstream: &UpstreamId,
    ) -> Result<(), Report> {
        let con = self.write().await;
        objects::set_upstream(
            &mut *con.acquire().await.context("acquire con")?,
            obj,
            upstream,
        )
        .await
    }

    pub async fn close(self) -> Result<(), Report> {
        // Close the existing connection to ensure it does not block cleanup
        self.write().await.close().await;

        // Clean up WAL files. This should happen above but sqlx does not correctly close sqlite
        // See: https://github.com/launchbadge/sqlx/issues/2249
        SqliteConnectOptions::default()
            .journal_mode(SqliteJournalMode::Wal)
            .filename(&self.path)
            .connect()
            .await?
            .close()
            .await?;

        Ok(())
    }
}
