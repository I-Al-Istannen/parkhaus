mod objects;

use crate::data::{S3Object, S3ObjectId};
use rootcause::prelude::ResultExt;
use rootcause::Report;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteSynchronous};
use sqlx::{query, Pool, Sqlite};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Clone)]
pub struct Database {
    con: Arc<RwLock<Pool<Sqlite>>>,
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

    pub async fn delete_object(&self, obj: &S3ObjectId) -> Result<(), Report> {
        let con = self.write().await;
        objects::delete_object(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn record_creation(&self, obj: &S3Object) -> Result<(), Report> {
        let con = self.write().await;
        objects::record_creation(&mut *con.acquire().await.context("acquire con")?, obj).await
    }

    pub async fn close(&self) -> Result<(), Report> {
        // Checkpoint the database to collapse the WAL file and truncate it. This ensures we only
        // have a single db file after shutdown.
        let pool = self.write().await;
        query("PRAGMA WAL_CHECKPOINT(FULL)")
            .execute(&mut *pool.acquire().await?)
            .await?;
        Ok(())
    }
}
