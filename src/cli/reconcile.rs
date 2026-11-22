use super::logging::env_filter;
use crate::config::Config;
use crate::db::Database;
use crate::server::KeyLocks;
use crate::server::reconcile::reconcile;
use rootcause::{Report, bail};
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Reports differences between the upstreams and the database. Must not run while `serve` does.
pub async fn run(config: Arc<Config>, db: Database) -> Result<(), Report> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .compact(),
        )
        .with(env_filter())
        .init();

    let http = crate::s3::client::http_client()?;
    let discrepancies = reconcile(&config, &db, &http, &KeyLocks::default()).await?;
    for discrepancy in &discrepancies {
        println!("{discrepancy}");
    }
    if !discrepancies.is_empty() {
        bail!("found {} discrepancies", discrepancies.len());
    }
    Ok(())
}
