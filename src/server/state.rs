use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use axum::http::Method;
use reqwest::Client;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::config::Config;
use crate::data::S3ObjectId;
use crate::db::Database;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Database,
    pub http: Client,
    pub migration_locks: MigrationLocks,
}

pub struct MutationLockGuard {
    _guard: Option<OwnedRwLockReadGuard<()>>,
}

#[derive(Clone, Default)]
pub struct MigrationLocks {
    inner: Arc<Mutex<HashMap<S3ObjectId, Weak<RwLock<()>>>>>,
}

impl MigrationLocks {
    fn lock(&self, object: &S3ObjectId) -> Arc<RwLock<()>> {
        let mut locks = self.inner.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(object).and_then(Weak::upgrade) {
            return lock;
        }

        let lock = Arc::new(RwLock::new(()));
        locks.insert(object.clone(), Arc::downgrade(&lock));
        lock
    }

    pub async fn wait_for_migration(
        &self,
        method: &Method,
        object: &S3ObjectId,
    ) -> MutationLockGuard {
        if matches!(*method, Method::POST | Method::PUT | Method::DELETE) {
            MutationLockGuard {
                _guard: Some(self.lock(object).read_owned().await),
            }
        } else {
            MutationLockGuard { _guard: None }
        }
    }

    pub async fn lock_migration(&self, object: &S3ObjectId) -> OwnedRwLockWriteGuard<()> {
        self.lock(object).write_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::MigrationLocks;
    use crate::data::S3ObjectId;
    use axum::http::Method;
    use std::time::Duration;

    fn object(key: &str) -> S3ObjectId {
        S3ObjectId {
            bucket: "bucket".to_owned(),
            key: key.to_owned(),
        }
    }

    #[tokio::test]
    async fn mutating_requests_wait_for_migration_lock() {
        let locks = MigrationLocks::default();
        let migration_guard = locks.lock_migration(&object("foo")).await;

        let lock_attempt = tokio::time::timeout(
            Duration::from_millis(50),
            locks.wait_for_migration(&Method::PUT, &object("foo")),
        )
        .await;
        assert!(
            lock_attempt.is_err(),
            "mutating request should wait for migration"
        );

        drop(migration_guard);

        tokio::time::timeout(
            Duration::from_millis(50),
            locks.wait_for_migration(&Method::PUT, &object("foo")),
        )
        .await
        .expect("mutating request should proceed after migration lock is released");
    }

    #[tokio::test]
    async fn reads_do_not_wait_for_migration_lock() {
        let locks = MigrationLocks::default();
        let _migration_guard = locks.lock_migration(&object("foo")).await;

        tokio::time::timeout(
            Duration::from_millis(50),
            locks.wait_for_migration(&Method::GET, &object("foo")),
        )
        .await
        .expect("reads should not wait for migration");
    }

    #[tokio::test]
    async fn expired_lock_entries_are_pruned() {
        let locks = MigrationLocks::default();
        {
            let _guard = locks.wait_for_migration(&Method::PUT, &object("foo")).await;
            assert_eq!(locks.inner.lock().unwrap().len(), 1);
        }

        let _guard = locks.wait_for_migration(&Method::PUT, &object("bar")).await;
        let keys = locks
            .inner
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(keys, vec![object("bar")]);
    }
}
