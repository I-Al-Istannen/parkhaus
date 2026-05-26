use crate::config::{Config, UpstreamId};
use crate::data::{InFlightMigration, MigrationState, PendingMigration, S3ObjectId};
use axum_prometheus::metrics::{counter, describe_counter, describe_gauge, gauge};
use std::collections::HashMap;

pub const GAUGE_PENDING_ACTIONS: &str = "pending_actions";
pub const COUNTER_MIGRATION_RUNS_TOTAL: &str = "migration_runs_total";
pub const COUNTER_MIGRATED_OBJECTS_TOTAL: &str = "migrated_objects_total";
pub const COUNTER_UPSTREAM_FORWARDS_TOTAL: &str = "upstream_forwards_total";
pub const COUNTER_UPSTREAM_FALLBACKS_TOTAL: &str = "upstream_fallbacks_total";
pub const COUNTER_OBJECT_CREATIONS_TOTAL: &str = "object_creations_total";
pub const COUNTER_OBJECT_DELETIONS_TOTAL: &str = "object_deletions_total";
pub const COUNTER_OBJECT_IMPORTED_ON_THE_FLY: &str = "object_import_on_the_fly";

type MigrationMetricKey = (UpstreamId, UpstreamId, S3ObjectId);

pub(super) struct MigrationMetrics;

pub(super) struct MigrationMetricsSnapshot {
    upstreams: Vec<UpstreamId>,
    states: HashMap<MigrationMetricKey, MigrationMetricState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationMetricState {
    Pending,
    Started,
    CopiedToTarget,
    Completed,
    Failed,
}

impl MigrationMetricState {
    fn all() -> [Self; 5] {
        [
            Self::Pending,
            Self::Started,
            Self::CopiedToTarget,
            Self::Completed,
            Self::Failed,
        ]
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Started => "Started",
            Self::CopiedToTarget => "CopiedToTarget",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
        }
    }
}

impl From<MigrationState> for MigrationMetricState {
    fn from(value: MigrationState) -> Self {
        match value {
            MigrationState::Started => Self::Started,
            MigrationState::CopiedToTarget => Self::CopiedToTarget,
        }
    }
}

impl MigrationMetrics {
    pub(super) fn snapshot(
        config: &Config,
        pending: &[PendingMigration],
        in_flight: &[InFlightMigration],
    ) -> MigrationMetricsSnapshot {
        let mut snapshot = MigrationMetricsSnapshot {
            upstreams: config.upstreams.keys().cloned().collect(),
            states: HashMap::new(),
        };

        for migration in pending {
            snapshot.set_state(migration, MigrationMetricState::Pending);
        }
        for migration in in_flight {
            snapshot.set_state(&migration.pending, migration.state.into());
        }

        snapshot.emit();
        snapshot
    }

    pub(super) fn record_run() {
        counter!(COUNTER_MIGRATION_RUNS_TOTAL).increment(1);
    }

    pub(super) fn record_migrated_object() {
        counter!(COUNTER_MIGRATED_OBJECTS_TOTAL).increment(1);
    }
}

impl MigrationMetricsSnapshot {
    pub(super) fn started(&mut self, pending: &PendingMigration) {
        self.set_state(pending, MigrationMetricState::Started);
    }

    pub(super) fn copied_to_target(&mut self, pending: &PendingMigration) {
        self.set_state(pending, MigrationMetricState::CopiedToTarget);
    }

    pub(super) fn completed(&mut self, pending: &PendingMigration) {
        self.set_state(pending, MigrationMetricState::Completed);
    }

    pub(super) fn failed(&mut self, pending: &PendingMigration) {
        self.set_state(pending, MigrationMetricState::Failed);
    }

    fn set_state(&mut self, pending: &PendingMigration, state: MigrationMetricState) {
        self.states.insert(
            (
                pending.source_upstream.clone(),
                pending.target_upstream.clone(),
                pending.object.clone(),
            ),
            state,
        );
        self.emit();
    }

    fn emit(&self) {
        for source in &self.upstreams {
            for target in &self.upstreams {
                if source == target {
                    continue;
                }
                for state in MigrationMetricState::all() {
                    gauge!(GAUGE_PENDING_ACTIONS,
                        "source" => source.0.clone(),
                        "target" => target.0.clone(),
                        "state" => state.as_label()
                    )
                    .set(self.total_for(source, target, state) as f64);
                }
            }
        }
    }

    fn total_for(
        &self,
        source: &UpstreamId,
        target: &UpstreamId,
        state: MigrationMetricState,
    ) -> usize {
        self.states
            .iter()
            .filter(|((current_source, current_target, _), current_state)| {
                current_source == source && current_target == target && **current_state == state
            })
            .count()
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
    describe_counter!(
        COUNTER_OBJECT_DELETIONS_TOTAL,
        "Total number of object deletion requests"
    );
}
