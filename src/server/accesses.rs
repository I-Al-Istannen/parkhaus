use crate::data::S3ObjectId;
use crate::db::Database;
use crate::server::metrics::COUNTER_ACCESSES_DROPPED_TOTAL;
use axum_prometheus::metrics::counter;
use jiff::Zoned;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

const QUEUE_SIZE: usize = 65_536;
const BATCH_SIZE: usize = 4_096;

/// Queues accesses for a single writer, which batches them into few transactions. When the
/// writer falls behind, accesses are dropped instead of piling up.
#[derive(Clone)]
pub struct AccessRecorder {
    queue: mpsc::Sender<S3ObjectId>,
}

impl AccessRecorder {
    /// Spawns the writer, which stops once every recorder is dropped.
    pub fn spawn(db: Database) -> Self {
        let (queue, accesses) = mpsc::channel(QUEUE_SIZE);
        tokio::spawn(write_accesses(db, accesses));
        Self { queue }
    }

    pub fn record(&self, object: S3ObjectId) {
        if let Err(TrySendError::Full(_)) = self.queue.try_send(object) {
            counter!(COUNTER_ACCESSES_DROPPED_TOTAL).increment(1);
        }
    }
}

async fn write_accesses(db: Database, mut accesses: mpsc::Receiver<S3ObjectId>) {
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    while accesses.recv_many(&mut batch, BATCH_SIZE).await > 0 {
        let mut counts: HashMap<S3ObjectId, i64> = HashMap::new();
        for object in batch.drain(..) {
            *counts.entry(object).or_default() += 1;
        }
        let counts: Vec<_> = counts.into_iter().collect();
        if let Err(error) = db.record_accesses(&counts, &Zoned::now()).await {
            warn!(%error, count = counts.len(), "Failed to record accesses");
        }
    }
}
