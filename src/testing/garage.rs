//! Testcontainers module for [Garage](https://garagehq.deuxfleurs.fr/), a
//! lightweight, distributed S3-compatible object storage service.
//!
//! # Quick start
//!
//! Use [`GarageInstance::start`] to spin up a fully initialized single-node
//! Garage cluster ready to serve S3 requests:
//!
//! ```rust,no_run
//! # #[tokio::test]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use crate::testing::garage::GarageInstance;
//!
//! let mut garage = GarageInstance::start().await?;
//!
//! let bucket = garage.create_bucket("my-bucket").await?;
//! let (key_id, secret) = garage.create_key("my-key").await?;
//! garage.allow_key_on_bucket(&bucket, &key_id).await?;
//!
//! println!("S3 endpoint: {}", garage.s3_endpoint());
//! println!("Region:      {}", garage.region());
//! println!("Key ID:      {key_id}");
//! println!("Secret:      {secret}");
//! # Ok(())
//! # }
//! ```
use std::borrow::Cow;
use std::time::Duration;

use std::sync::Mutex;

use reqwest::Client;
use rootcause::prelude::ResultExt as _;
use rootcause::{Report, bail, report};
use serde_json::{Value, json};
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ContainerPort, CopyToContainer, WaitFor};
use testcontainers::{ContainerAsync, Image};
use url::Url;

/// S3 API port.
pub const S3_API_PORT: ContainerPort = ContainerPort::Tcp(3900);
/// Admin API port.
pub const ADMIN_PORT: ContainerPort = ContainerPort::Tcp(3903);
/// S3 region name Garage is configured with.
pub const DEFAULT_REGION: &str = "garage";
/// Admin bearer token baked into the test config.
pub const ADMIN_TOKEN: &str = "test-admin-token";

const IMAGE_NAME: &str = "dxflrs/garage";
const IMAGE_TAG: &str = "v2.2.0";

/// `garage.toml` embedded into the container via [`Image::copy_to_sources`].
///
/// Notable choices:
/// - SQLite metadata engine — no extra dependencies.
/// - `replication_factor = 1` — single-node cluster.
/// - Fixed `rpc_secret` — only matters inside the container.
/// - Admin API exposed on `[::]:3903` with a static `admin_token`.
const GARAGE_CONFIG: &[u8] = br#"
metadata_dir = "/tmp/meta"
data_dir     = "/tmp/data"
db_engine    = "sqlite"

replication_factor = 1

rpc_bind_addr   = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[s3_api]
s3_region    = "garage"
api_bind_addr = "[::]:3900"
root_domain   = ".s3.garage.localhost"

[s3_web]
bind_addr    = "[::]:3902"
root_domain  = ".web.garage.localhost"
index        = "index.html"

[admin]
api_bind_addr = "[::]:3903"
admin_token   = "test-admin-token"
"#;

// ── Image ────────────────────────────────────────────────────────────────────

/// Testcontainers [`Image`] for Garage.
///
/// The default instance injects a minimal single-node configuration and waits
/// until Garage's admin `/health` endpoint returns `200 OK`.
///
/// In most cases you want [`GarageInstance::start`] instead of using this
/// struct directly, because a freshly-started Garage node has no cluster
/// layout yet and cannot serve S3 requests until one is assigned.
#[derive(Debug, Clone)]
pub struct Garage {
    config: CopyToContainer,
}

impl Default for Garage {
    fn default() -> Self {
        Self {
            config: CopyToContainer::new(GARAGE_CONFIG.to_vec(), "/etc/garage.toml"),
        }
    }
}

impl Image for Garage {
    fn name(&self) -> &str {
        IMAGE_NAME
    }

    fn tag(&self) -> &str {
        IMAGE_TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        // Wait for 503: Garage is running but the cluster layout has not been
        // applied yet, so quorum is unavailable.  We will apply the layout in
        // `GarageInstance::init` and then poll until we get 200.
        vec![WaitFor::http(
            HttpWaitStrategy::new("/health")
                .with_port(ADMIN_PORT)
                .with_poll_interval(Duration::from_millis(200))
                .with_expected_status_code(503u16),
        )]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        // Silence Garage's colour output so logs stay readable in test output.
        [("NO_COLOR", "1")]
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        std::iter::once(&self.config)
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[S3_API_PORT, ADMIN_PORT]
    }
}

// ── GarageInstance ───────────────────────────────────────────────────────────

/// A started, fully-initialized single-node Garage cluster.
///
/// Obtain via [`GarageInstance::start`] (convenience) or
/// [`GarageInstance::init`] (when you need to customize the container first).
pub struct GarageInstance {
    container: Mutex<Option<ContainerAsync<Garage>>>,
    s3_endpoint: Url,
    admin_endpoint: Url,
}

impl GarageInstance {
    /// Start a Garage container and initialize its cluster layout.
    ///
    /// This is the primary entry point for most tests.
    pub async fn start() -> Result<Self, Report> {
        use testcontainers::runners::AsyncRunner;
        let container = Garage::default()
            .start()
            .await
            .context("failed to start Garage container")?;
        Self::init(container).await
    }

    /// Wrap an already-started container and initialize the cluster layout.
    ///
    /// Use this when you started the container yourself (e.g. to share it
    /// across tests with `OnceCell`).
    pub async fn init(container: ContainerAsync<Garage>) -> Result<Self, Report> {
        let host = container
            .get_host()
            .await
            .context("failed to resolve Garage host")?;
        let s3_port = container
            .get_host_port_ipv4(S3_API_PORT)
            .await
            .context("failed to resolve S3 port")?;
        let admin_port = container
            .get_host_port_ipv4(ADMIN_PORT)
            .await
            .context("failed to resolve admin port")?;

        let s3_endpoint: Url = format!("http://{host}:{s3_port}")
            .parse()
            .context("parse s3 endpoint")?;
        let admin_endpoint: Url = format!("http://{host}:{admin_port}")
            .parse()
            .context("parse admin endpoint")?;

        let instance = Self {
            container: Mutex::new(Some(container)),
            s3_endpoint,
            admin_endpoint,
        };
        instance.assign_layout().await?;
        instance.wait_healthy().await?;
        Ok(instance)
    }

    // ── admin helpers ────────────────────────────────────────────────────────

    async fn admin_get(&self, op: &str) -> Result<Value, Report> {
        let url = self
            .admin_endpoint
            .join(&format!("v2/{op}"))
            .context("build URL")?;
        let resp = Client::new()
            .get(url)
            .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
            .send()
            .await
            .context_with(|| format!("GET {op}"))?;

        let status = resp.status();
        let body: String = resp.text().await.context("read body")?;
        if !status.is_success() {
            bail!("Garage admin GET {op} failed ({status}): {body}");
        }
        serde_json::from_str(&body)
            .context(format!("parse response for GET {op}"))
            .map_err(Report::into_dynamic)
    }

    async fn admin_post(&self, op: &str, payload: Value) -> Result<Value, Report> {
        let url = self
            .admin_endpoint
            .join(&format!("v2/{op}"))
            .context("build URL")?;
        let body = serde_json::to_string(&payload).context("serialize request")?;
        let resp = Client::new()
            .post(url)
            .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .context_with(|| format!("POST {op}"))?;

        let status = resp.status();
        let body: String = resp.text().await.context("read body")?;
        if !status.is_success() {
            bail!("Garage admin POST {op} failed ({status}): {body}");
        }
        serde_json::from_str(&body)
            .context(format!("parse response for POST {op}"))
            .map_err(Report::into_dynamic)
    }

    /// Poll `/health` until a 200 response is received (cluster is healthy).
    async fn wait_healthy(&self) -> Result<(), Report> {
        let url = self
            .admin_endpoint
            .join("/health")
            .context("build health URL")?;
        for _ in 0..50u8 {
            if let Ok(resp) = Client::new().get(url.clone()).send().await
                && resp.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        bail!("Garage cluster did not become healthy within 10 seconds after layout assignment")
    }

    /// Assign a zone+capacity to the single node and apply the layout.
    async fn assign_layout(&self) -> Result<(), Report> {
        let status = self.admin_get("GetClusterStatus").await?;
        let node_id = status["nodes"]
            .as_array()
            .and_then(|n| n.first())
            .and_then(|n| n["id"].as_str())
            .ok_or_else(|| report!("no nodes in GetClusterStatus response"))?
            .to_owned();

        self.admin_post(
            "UpdateClusterLayout",
            json!({
                "roles": [{
                    "id": node_id,
                    "zone": "dc1",
                    "capacity": 1_000_000_000u64,
                    "tags": []
                }]
            }),
        )
        .await
        .context("UpdateClusterLayout")?;

        self.admin_post("ApplyClusterLayout", json!({ "version": 1 }))
            .await
            .context("ApplyClusterLayout")?;

        Ok(())
    }

    // ── public API ───────────────────────────────────────────────────────────

    /// The S3-compatible API endpoint.
    pub fn s3_endpoint(&self) -> &Url {
        &self.s3_endpoint
    }

    /// The admin API endpoint.
    #[allow(unused)]
    pub fn admin_endpoint(&self) -> &Url {
        &self.admin_endpoint
    }

    /// The S3 region name (`"garage"` by default).
    pub fn region(&self) -> &str {
        DEFAULT_REGION
    }

    /// The admin bearer token for this instance.
    #[allow(unused)]
    pub fn admin_token(&self) -> &str {
        ADMIN_TOKEN
    }

    /// Take the underlying container, stopping it on drop.
    ///
    /// Returns `None` if the container has already been taken.
    pub fn take_container(&self) -> Option<ContainerAsync<Garage>> {
        self.container.lock().unwrap().take()
    }

    /// Create a new S3 bucket with the given global alias and return its
    /// internal bucket ID.
    pub async fn create_bucket(&self, name: &str) -> Result<String, Report> {
        let resp = self
            .admin_post("CreateBucket", json!({ "globalAlias": name }))
            .await
            .context_with(|| format!("CreateBucket {name:?}"))?;

        resp["id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| report!("missing `id` in CreateBucket response"))
    }

    /// Create a new API key and return `(access_key_id, secret_access_key)`.
    pub async fn create_key(&self, name: &str) -> Result<(String, String), Report> {
        let resp = self
            .admin_post("CreateKey", json!({ "name": name }))
            .await
            .context_with(|| format!("CreateKey {name:?}"))?;

        let key_id = resp["accessKeyId"]
            .as_str()
            .ok_or_else(|| report!("missing `accessKeyId` in CreateKey response"))?
            .to_owned();

        // The secret is returned on creation; fall back to a dedicated fetch if
        // a future Garage version changes this behaviour.
        let secret: String = if let Some(s) = resp["secretAccessKey"].as_str() {
            s.to_owned()
        } else {
            self.fetch_key_secret(&key_id).await?
        };

        Ok((key_id, secret))
    }

    /// Fetch the secret access key for an existing key ID.
    async fn fetch_key_secret(&self, key_id: &str) -> Result<String, Report> {
        let url = self
            .admin_endpoint
            .join(&format!("v2/GetKeyInfo?id={key_id}&showSecretKey=true"))
            .context("build URL")?;
        let resp = Client::new()
            .get(url)
            .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
            .send()
            .await
            .context("GetKeyInfo")?;

        let status = resp.status();
        let body: String = resp.text().await.context("read body")?;
        if !status.is_success() {
            bail!("GetKeyInfo failed ({status}): {body}");
        }

        let parsed: Value = serde_json::from_str(&body).context("parse GetKeyInfo response")?;
        parsed["secretAccessKey"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| report!("missing `secretAccessKey` in GetKeyInfo response"))
    }

    /// Grant read + write + owner permissions for `key_id` on `bucket_id`.
    pub async fn allow_key_on_bucket(&self, bucket_id: &str, key_id: &str) -> Result<(), Report> {
        self.admin_post(
            "AllowBucketKey",
            json!({
                "bucketId": bucket_id,
                "accessKeyId": key_id,
                "permissions": { "read": true, "write": true, "owner": true }
            }),
        )
        .await
        .context_with(|| format!("AllowBucketKey bucket={bucket_id} key={key_id}"))?;

        Ok(())
    }

    pub fn drop_in_new_runtime(container: ContainerAsync<Garage>) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("teardown runtime")
            .block_on(async move {
                drop(container);
                tokio::task::yield_now().await;
            });
    }
}
