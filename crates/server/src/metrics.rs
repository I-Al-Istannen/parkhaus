use crate::config::Config;
use crate::data::MigrationState;
use axum_prometheus::metrics::{describe_counter, describe_gauge, gauge};

pub const GAUGE_PENDING_ACTIONS: &str = "pending_actions";
pub const COUNTER_MIGRATION_RUNS_TOTAL: &str = "migration_runs_total";
pub const COUNTER_MIGRATED_OBJECTS_TOTAL: &str = "migrated_objects_total";
pub const COUNTER_UPSTREAM_FORWARDS_TOTAL: &str = "upstream_forwards_total";
pub const COUNTER_UPSTREAM_FALLBACKS_TOTAL: &str = "upstream_fallbacks_total";
pub const COUNTER_OBJECT_CREATIONS_TOTAL: &str = "object_creations_total";
pub const COUNTER_OBJECT_DELETIONS_TOTAL: &str = "object_deletions_total";

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

pub fn reset_pending_migrations_gauge(config: &Config) {
    for source in config.upstreams.keys() {
        for target in config.upstreams.keys() {
            if source == target {
                continue;
            }
            for state in MigrationState::all() {
                gauge!(GAUGE_PENDING_ACTIONS,
                    "source" => source.0.clone(),
                    "target" => target.0.clone(),
                    "state" => state.to_string()
                )
                .set(0.0);
            }
        }
    }
}
