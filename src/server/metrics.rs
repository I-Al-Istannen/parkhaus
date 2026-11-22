use crate::config::{Config, UpstreamId};
use crate::data::PendingMigration;
use axum_prometheus::metrics::{counter, describe_counter, describe_gauge, gauge};
use std::collections::HashMap;

pub const GAUGE_PENDING_ACTIONS: &str = "pending_actions";
pub const GAUGE_RECONCILE_DISCREPANCIES: &str = "reconcile_discrepancies";
pub const COUNTER_MIGRATION_RUNS_TOTAL: &str = "migration_runs_total";
pub const COUNTER_MIGRATION_RULE_MIGRATIONS_TOTAL: &str = "migration_rule_migrations_total";
pub const COUNTER_MIGRATED_OBJECTS_TOTAL: &str = "migrated_objects_total";
pub const COUNTER_UPSTREAM_FORWARDS_TOTAL: &str = "upstream_forwards_total";
pub const COUNTER_UPSTREAM_FALLBACKS_TOTAL: &str = "upstream_fallbacks_total";
pub const COUNTER_OBJECT_CREATIONS_TOTAL: &str = "object_creations_total";
pub const COUNTER_OBJECT_DELETIONS_TOTAL: &str = "object_deletions_total";
pub const COUNTER_ACCESSES_DROPPED_TOTAL: &str = "accesses_dropped_total";

type MigrationMetricKey = (UpstreamId, UpstreamId, MigrationMetricState);

pub(super) struct MigrationMetrics;

/// Gauges of the current migration run per source, target and state.
pub(super) struct MigrationMetricsSnapshot {
    counts: HashMap<MigrationMetricKey, usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MigrationMetricState {
    Pending,
    Completed,
    Failed,
}

impl MigrationMetricState {
    fn all() -> [Self; 3] {
        [Self::Pending, Self::Completed, Self::Failed]
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
        }
    }
}

impl MigrationMetrics {
    pub(super) fn snapshot(
        config: &Config,
        pending: &[PendingMigration],
    ) -> MigrationMetricsSnapshot {
        let mut counts = HashMap::new();
        for source in config.upstreams.keys() {
            for target in config.upstreams.keys().filter(|it| *it != source) {
                for state in MigrationMetricState::all() {
                    counts.insert((source.clone(), target.clone(), state), 0);
                }
            }
        }
        for migration in pending {
            *counts
                .entry(key(migration, MigrationMetricState::Pending))
                .or_default() += 1;
        }
        let snapshot = MigrationMetricsSnapshot { counts };
        for key in snapshot.counts.keys() {
            snapshot.emit(key);
        }
        snapshot
    }

    pub(super) fn record_run() {
        counter!(COUNTER_MIGRATION_RUNS_TOTAL).increment(1);
    }

    pub(super) fn record_migrated_object() {
        counter!(COUNTER_MIGRATED_OBJECTS_TOTAL).increment(1);
    }

    pub(super) fn record_rule_migrations(rule_index: usize, target: &UpstreamId, count: usize) {
        counter!(COUNTER_MIGRATION_RULE_MIGRATIONS_TOTAL,
            "rule_index" => rule_index.to_string(),
            "target" => target.0.clone()
        )
        .increment(count as u64);
    }
}

fn key(migration: &PendingMigration, state: MigrationMetricState) -> MigrationMetricKey {
    (
        migration.source_upstream.clone(),
        migration.target_upstream.clone(),
        state,
    )
}

impl MigrationMetricsSnapshot {
    pub(super) fn completed(&mut self, migration: &PendingMigration) {
        self.finish(migration, MigrationMetricState::Completed);
    }

    pub(super) fn failed(&mut self, migration: &PendingMigration) {
        self.finish(migration, MigrationMetricState::Failed);
    }

    fn finish(&mut self, migration: &PendingMigration, state: MigrationMetricState) {
        let pending = key(migration, MigrationMetricState::Pending);
        if let Some(count) = self.counts.get_mut(&pending) {
            *count = count.saturating_sub(1);
        }
        self.emit(&pending);
        let finished = key(migration, state);
        *self.counts.entry(finished.clone()).or_default() += 1;
        self.emit(&finished);
    }

    fn emit(&self, key: &MigrationMetricKey) {
        let (source, target, state) = key;
        gauge!(GAUGE_PENDING_ACTIONS,
            "source" => source.0.clone(),
            "target" => target.0.clone(),
            "state" => state.as_label()
        )
        .set(self.counts.get(key).copied().unwrap_or_default() as f64);
    }
}

pub fn initialize_metrics() {
    describe_gauge!(
        GAUGE_PENDING_ACTIONS,
        "Number of pending actions in the system"
    );
    describe_counter!(COUNTER_MIGRATED_OBJECTS_TOTAL, "Number of migrated objects");
    describe_counter!(
        COUNTER_MIGRATION_RUNS_TOTAL,
        "Total number of migration runs"
    );
    describe_counter!(
        COUNTER_MIGRATION_RULE_MIGRATIONS_TOTAL,
        "Total number of pending migrations created by tiering rules"
    );
    describe_counter!(
        COUNTER_UPSTREAM_FORWARDS_TOTAL,
        "Total number of upstream forward attempts"
    );
    describe_counter!(
        COUNTER_UPSTREAM_FALLBACKS_TOTAL,
        "Total number of upstream fallback responses"
    );
    describe_counter!(
        COUNTER_OBJECT_CREATIONS_TOTAL,
        "Total number of object creation requests"
    );
    describe_gauge!(
        GAUGE_RECONCILE_DISCREPANCIES,
        "Objects whose location in the database disagrees with the upstreams at the last reconciliation"
    );
    describe_counter!(
        COUNTER_ACCESSES_DROPPED_TOTAL,
        "Accesses not recorded because the database fell behind"
    );
    describe_counter!(
        COUNTER_OBJECT_DELETIONS_TOTAL,
        "Total number of object deletion requests"
    );
}
