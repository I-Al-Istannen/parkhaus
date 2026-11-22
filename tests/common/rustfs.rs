//! Testcontainers module for [RustFS](https://rustfs.com), an S3-compatible
//! object storage written in Rust.
//!
//! RustFS has no simple admin API for creating scoped keys, so every "key" is
//! the root credential pair and buckets are created via the S3 API.
use std::borrow::Cow;
use std::sync::Mutex;
use std::time::Duration;

use parkhaus::s3::client::S3Client;
use parkhaus::s3::types::AddressingStyle;
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt as _;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::{ContainerAsync, Image};
use url::Url;

pub const S3_API_PORT: ContainerPort = ContainerPort::Tcp(9000);
pub const DEFAULT_REGION: &str = "us-east-1";
pub const ACCESS_KEY: &str = "rustfs-test-access";
pub const SECRET_KEY: &str = "rustfs-test-secret-1234567890";

const IMAGE_NAME: &str = "rustfs/rustfs";
const IMAGE_TAG: &str = "1.0.1";

#[derive(Debug, Clone, Default)]
pub struct RustFs;

impl Image for RustFs {
    fn name(&self) -> &str {
        IMAGE_NAME
    }

    fn tag(&self) -> &str {
        IMAGE_TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::http(
            HttpWaitStrategy::new("/health/ready")
                .with_port(S3_API_PORT)
                .with_poll_interval(Duration::from_millis(200))
                .with_expected_status_code(200u16),
        )]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        [
            ("RUSTFS_ACCESS_KEY", ACCESS_KEY),
            ("RUSTFS_SECRET_KEY", SECRET_KEY),
            ("RUSTFS_SERVER_DOMAINS", "localhost"),
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[S3_API_PORT]
    }
}

pub struct RustFsInstance {
    container: Mutex<Option<ContainerAsync<RustFs>>>,
    s3_endpoint: Url,
}

impl RustFsInstance {
    pub async fn start() -> Result<Self, Report> {
        use testcontainers::runners::AsyncRunner;
        let container = RustFs
            .start()
            .await
            .context("failed to start RustFS container")?;
        let s3_port = container
            .get_host_port_ipv4(S3_API_PORT)
            .await
            .context("failed to resolve S3 port")?;
        let s3_endpoint: Url = format!("http://localhost:{s3_port}")
            .parse()
            .context("parse s3 endpoint")?;
        Ok(Self {
            container: Mutex::new(Some(container)),
            s3_endpoint,
        })
    }

    pub fn s3_endpoint(&self) -> &Url {
        &self.s3_endpoint
    }

    pub fn region(&self) -> &str {
        DEFAULT_REGION
    }

    pub fn take_container(&self) -> Option<ContainerAsync<RustFs>> {
        self.container.lock().unwrap().take()
    }

    /// Creates the bucket and returns its name, which doubles as its ID.
    ///
    /// Uses a fresh HTTP client: every test has its own runtime, and pooled
    /// connections die with the runtime that opened them.
    pub async fn create_bucket(&self, name: &str) -> Result<String, Report> {
        S3Client::new(
            Client::new(),
            self.s3_endpoint.clone(),
            DEFAULT_REGION,
            ACCESS_KEY,
            SECRET_KEY,
            AddressingStyle::Path,
        )
        .create_bucket(name)
        .await?;
        Ok(name.to_owned())
    }

    pub fn create_key(&self) -> (String, String) {
        (ACCESS_KEY.to_owned(), SECRET_KEY.to_owned())
    }
}
