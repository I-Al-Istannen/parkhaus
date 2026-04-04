pub mod cli;
pub mod config;
pub mod data;
pub mod db;
pub mod error;
pub mod s3;
pub mod server;

// cargos integration test setup really is something (not good)
use clap as _;
#[cfg(test)]
use ctor as _;
#[cfg(test)]
use serde_json as _;
#[cfg(test)]
use testcontainers as _;
