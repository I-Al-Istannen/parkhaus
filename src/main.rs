mod config;
mod data;
mod db;
mod endpoints;
mod error;
mod import;
mod migrate;
mod s3_client;
#[cfg(test)]
mod testing;
mod toml_utils;

use std::path::PathBuf;
use std::sync::Arc;

use crate::db::Database;
use crate::import::import;
use crate::migrate::migration_task;
use axum::Router;
use axum::routing::any;
use clap::{Parser, Subcommand};
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use tokio::net::TcpListener;
use tokio::signal;
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
    pub config: Arc<config::Config>,
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

async fn serve(config: Arc<config::Config>, db: Database) -> AppResult<()> {
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
        .with_state(app_state);

    let listener = TcpListener::bind(&config.listen)
        .await
        .context("failed to bind listen socket")?;
    info!(listen = %config.listen, "proxy listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_token.clone()))
        .await
        .context("axum server failed")?;
    Ok(())
}

async fn shutdown_signal(token: CancellationToken) {
    if signal::ctrl_c().await.is_ok() {
        info!("received ctrl+c, shutting down");
        token.cancel();
    }
}
