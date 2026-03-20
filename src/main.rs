mod config;
mod data;
mod db;
mod endpoints;
mod error;
mod import;
mod metrics;
mod migrate;
mod s3_client;
#[cfg(test)]
mod testing;
mod toml_utils;

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;
use crate::data::MigrationState;
use crate::db::Database;
use crate::import::import;
use crate::metrics::{GAUGE_PENDING_ACTIONS, initialize_metrics};
use crate::migrate::migration_task;
use axum::Router;
use axum::routing::{any, get};
use axum_prometheus::metrics::gauge;
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use axum_prometheus::{GenericMetricLayer, Handle, PrometheusMetricLayer};
use clap::{Parser, Subcommand};
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use tokio::net::TcpListener;
use tokio::{join, signal};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

type AppResult<T> = Result<T, Report>;

#[derive(Parser, Debug)]
#[command(author, version, about, arg_required_else_help = true)]
struct Cli {
    #[arg(long, short)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

/// A lightweight and transparent S3 proxy server implementing object tiering.
#[derive(Subcommand, Debug)]
enum Command {
    /// Start the proxy server.
    Serve,
    /// Import objects from all configured upstreams into the local database.
    Import {
        /// Optional timestamp to use as the last modified time for all imported objects.
        /// If not provided, the last modified time from S3 will be used.
        #[arg(long)]
        import_time: Option<jiff::Timestamp>,
    },
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Database,
    pub http: Client,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Application error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> AppResult<()> {
    let cli = Cli::parse();
    let config = Arc::new(config::load(&cli.config)?);
    let db = Database::new(&config.db_path)
        .await
        .context("failed to initialize database")
        .attach(format!("path: {}", &config.db_path.display()))?;

    let res = match cli.command {
        Command::Serve => serve(config, db.clone()).await,
        Command::Import { import_time } => import(config, db.clone(), import_time).await,
    };

    if let Err(e) = db.close().await {
        eprintln!("Failed to close database: {e}");
    }
    res
}

async fn serve(config: Arc<Config>, db: Database) -> AppResult<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .compact(),
        )
        .init();

    let shutdown_token = CancellationToken::new();
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    let main_server = start_main_server(
        config.clone(),
        db.clone(),
        prometheus_layer,
        shutdown_token.clone(),
    );
    let metrics_server =
        start_metric_server(config.clone(), db, metric_handle, shutdown_token.clone());

    initialize_metrics();

    let (a, b, _) = join!(
        main_server,
        metrics_server,
        shutdown_signal(shutdown_token.clone())
    );
    a?;
    b
}

async fn start_main_server(
    config: Arc<Config>,
    db: Database,
    prometheus_layer: GenericMetricLayer<'static, PrometheusHandle, Handle>,
    shutdown_token: CancellationToken,
) -> Result<(), Report> {
    // The poor man's task scheduler!
    tokio::spawn(migration_task(
        (*config).clone(),
        db.clone(),
        shutdown_token.clone(),
    ));

    let app_state = AppState {
        config: Arc::clone(&config),
        db,
        http: Client::builder()
            .build()
            .context("failed to build HTTP client")?,
    };

    let app = Router::new()
        .route("/", any(endpoints::proxy_request))
        .route("/{*path}", any(endpoints::proxy_request))
        .with_state(app_state)
        .layer(prometheus_layer);

    let listener = TcpListener::bind(&config.listen)
        .await
        .context("failed to bind listen socket")?;
    info!(listen = %config.listen, "proxy listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_token.cancelled_owned())
        .await
        .context("axum server failed")
        .map_err(Report::into_dynamic)
}

async fn start_metric_server(
    config: Arc<Config>,
    database: Database,
    metric_handle: PrometheusHandle,
    shutdown_token: CancellationToken,
) -> Result<(), Report> {
    let Some(listen) = &config.metrics_listen else {
        info!("metrics server disabled (no 'metrics_listen' configured)");
        return Ok(());
    };
    let listener = TcpListener::bind(listen)
        .await
        .context("failed to bind listen socket")?;
    info!(listen = %listen, "metrics server listening");

    axum::serve(
        listener,
        Router::new().route(
            "/metrics",
            get(|| async move {
                // Initialize all to zero so they have a value even if there are none pending
                metrics::reset_pending_migrations_gauge(&config);

                for state in MigrationState::all() {
                    if let Ok(pending) = database.get_pending_per_upstream(Some(*state)).await {
                        for (source, target, actions) in pending {
                            gauge!(GAUGE_PENDING_ACTIONS,
                                "source" => source.0.clone(),
                                "target" => target.0.clone(),
                                "state" => state.to_string()
                            )
                            .set(actions as f64);
                        }
                    }
                }
                metric_handle.render()
            }),
        ),
    )
    .with_graceful_shutdown(shutdown_token.cancelled_owned())
    .await
    .context("axum metrics server failed")
    .map_err(Report::into_dynamic)
}

async fn shutdown_signal(token: CancellationToken) {
    if signal::ctrl_c().await.is_ok() {
        info!("received ctrl+c, shutting down");
        token.cancel();
    }
}
