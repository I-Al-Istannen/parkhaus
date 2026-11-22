use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use reqwest::Client;
use tokio::sync::OwnedMutexGuard;

use crate::config::Config;
use crate::data::S3ObjectId;
use crate::db::Database;
use crate::server::listing::ListingStore;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Database,
    pub http: Client,
    pub key_locks: KeyLocks,
    pub listings: ListingStore,
}

/// Serializes everything that changes where an object lives: client mutations, the resolution of
/// dirty objects and migrations. Reads do not lock.
#[derive(Clone, Default)]
pub struct KeyLocks {
    inner: Arc<Mutex<HashMap<S3ObjectId, Weak<tokio::sync::Mutex<()>>>>>,
}

impl KeyLocks {
    pub async fn lock(&self, object: &S3ObjectId) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.inner.lock().unwrap();
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(object).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    locks.insert(object.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }

    /// Locks in iteration order, callers must pass a sorted set to not deadlock each other.
    pub async fn lock_all(
        &self,
        objects: impl IntoIterator<Item = &S3ObjectId>,
    ) -> Vec<OwnedMutexGuard<()>> {
        let mut guards = Vec::new();
        for object in objects {
            guards.push(self.lock(object).await);
        }
        guards
    }
}

#[cfg(test)]
mod tests {
    use super::KeyLocks;
    use crate::data::S3ObjectId;
    use std::time::Duration;

    fn object(key: &str) -> S3ObjectId {
        S3ObjectId {
            bucket: "bucket".to_owned(),
            key: key.to_owned(),
        }
    }

    #[tokio::test]
    async fn locks_are_exclusive_per_key() {
        let locks = KeyLocks::default();
        let guard = locks.lock(&object("foo")).await;

        let attempt =
            tokio::time::timeout(Duration::from_millis(50), locks.lock(&object("foo"))).await;
        assert!(attempt.is_err(), "second lock on the same key should wait");

        tokio::time::timeout(Duration::from_millis(50), locks.lock(&object("bar")))
            .await
            .expect("other keys are independent");

        drop(guard);
        tokio::time::timeout(Duration::from_millis(50), locks.lock(&object("foo")))
            .await
            .expect("lock should be free after release");
    }

    #[tokio::test]
    async fn expired_lock_entries_are_pruned() {
        let locks = KeyLocks::default();
        drop(locks.lock(&object("foo")).await);

        let _guard = locks.lock(&object("bar")).await;
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
