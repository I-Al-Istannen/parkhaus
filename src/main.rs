mod config;
mod data;
mod db;
mod error;
mod proxy;
mod toml_utils;

use std::path::PathBuf;
use std::sync::Arc;

use crate::db::Database;
use axum::Router;
use axum::routing::any;
use clap::{Parser, Subcommand};
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};

type AppResult<T> = Result<T, Report>;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Serve,
    Import,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<config::Config>,
    pub db: Database,
    pub http: Client,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    if let Err(error) = run().await {
        error!(%error, "application failed");
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

    match cli.command {
        Command::Serve => serve(config, db).await,
        Command::Import => todo!(),
    }
}

async fn serve(config: Arc<config::Config>, db: Database) -> AppResult<()> {
    let app_state = AppState {
        config: Arc::clone(&config),
        db,
        http: Client::builder()
            .build()
            .context("failed to build HTTP client")?,
    };

    let app = Router::new()
        .route("/", any(proxy::handle_request))
        .route("/{*path}", any(proxy::handle_request))
        .with_state(app_state);

    let listener = TcpListener::bind(&config.listen)
        .await
        .context("failed to bind listen socket")?;
    info!(listen = %config.listen, "proxy listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum server failed")?;
    Ok(())
}

async fn shutdown_signal() {
    if signal::ctrl_c().await.is_ok() {
        info!("received ctrl+c, shutting down");
    }
}
