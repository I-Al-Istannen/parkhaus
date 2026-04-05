use std::sync::Arc;

use crate::cli::logging::env_filter;
use crate::config::Config;
use crate::db::Database;
use crate::server::endpoints;
use crate::server::metrics::initialize_metrics;
use crate::server::migrate::migration_task;
use crate::server::state::AppState;
use axum::Router;
use axum::routing::{any, get};
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use axum_prometheus::{GenericMetricLayer, Handle, PrometheusMetricLayer};
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use tokio::net::TcpListener;
use tokio::{join, signal};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub async fn run(config: Arc<Config>, db: Database) -> Result<(), Report> {
    tracing_subscriber::registry()
        .with(env_filter())
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .compact(),
        )
        .init();

    if db.get_num_of_objects_without_size().await? > 0 {
        let count = db.get_num_of_objects_without_size().await?;
        warn!(
            %count,
            "Database contains objects without size. You likely want to re-run import \
            (with --keep-modify-time)"
        )
    }

    let shutdown_token = CancellationToken::new();
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
    let main_server = start_main_server(
        config.clone(),
        db.clone(),
        prometheus_layer,
        shutdown_token.clone(),
    );
    let metrics_server = start_metric_server(config.clone(), metric_handle, shutdown_token.clone());

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
        Router::new().route("/metrics", get(|| async move { metric_handle.render() })),
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
