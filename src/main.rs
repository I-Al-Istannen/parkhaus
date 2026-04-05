#![allow(unused_crate_dependencies)]

use clap::{Parser, Subcommand};
use parkhaus::cli::import::ImportOptions;
use parkhaus::cli::logging;
use parkhaus::db::Database;
use parkhaus::{cli, config};
use rootcause::Report;
use rootcause::prelude::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;

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
    Import(ImportOptions),
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Application error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Report> {
    logging::setup_rootcause_hooks().context("failed to install error handler hooks")?;

    let cli = Cli::parse();
    let config = Arc::new(config::load(&cli.config)?);
    let db = Database::new(&config.db_path)
        .await
        .context("failed to initialize database")
        .attach(format!("path: {}", &config.db_path.display()))?;

    let result = match cli.command {
        Command::Serve => cli::serve::run(config, db.clone()).await,
        Command::Import(options) => cli::import::run(config, db.clone(), options).await,
    };

    if let Err(error) = db.close().await {
        eprintln!("Failed to close database: {error}");
    }

    result
}
