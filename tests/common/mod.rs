pub mod garage;
pub mod rustfs;

use garage::GarageInstance;
use rootcause::{Report, bail};
use rustfs::RustFsInstance;
use url::Url;

/// Environment variable selecting the S3 backend the tests run against.
pub const BACKEND_ENV: &str = "PARKHAUS_TEST_BACKEND";

pub enum S3Instance {
    Garage(GarageInstance),
    RustFs(RustFsInstance),
}

impl S3Instance {
    /// Starts the backend selected by [`BACKEND_ENV`] (`garage` by default).
    pub async fn start() -> Result<Self, Report> {
        match std::env::var(BACKEND_ENV).ok().as_deref() {
            None | Some("garage") => Ok(Self::Garage(GarageInstance::start().await?)),
            Some("rustfs") => Ok(Self::RustFs(RustFsInstance::start().await?)),
            Some(other) => bail!("unknown {BACKEND_ENV} '{other}', expected 'garage' or 'rustfs'"),
        }
    }

    pub fn s3_endpoint(&self) -> &Url {
        match self {
            Self::Garage(g) => g.s3_endpoint(),
            Self::RustFs(r) => r.s3_endpoint(),
        }
    }

    pub fn region(&self) -> &str {
        match self {
            Self::Garage(g) => g.region(),
            Self::RustFs(r) => r.region(),
        }
    }

    /// Creates a bucket and returns its backend-specific ID.
    pub async fn create_bucket(&self, name: &str) -> Result<String, Report> {
        match self {
            Self::Garage(g) => g.create_bucket(name).await,
            Self::RustFs(r) => r.create_bucket(name).await,
        }
    }

    /// Returns `(access_key_id, secret_access_key)`. Backends without scoped
    /// keys hand out their root credentials.
    pub async fn create_key(&self, name: &str) -> Result<(String, String), Report> {
        match self {
            Self::Garage(g) => g.create_key(name).await,
            Self::RustFs(r) => Ok(r.create_key()),
        }
    }

    pub async fn allow_key_on_bucket(&self, bucket_id: &str, key_id: &str) -> Result<(), Report> {
        match self {
            Self::Garage(g) => g.allow_key_on_bucket(bucket_id, key_id).await,
            Self::RustFs(_) => Ok(()),
        }
    }

    /// Stops the container. Usable from a `#[ctor::dtor]`, where no runtime
    /// is available anymore.
    pub fn shutdown(&self) {
        match self {
            Self::Garage(g) => drop_in_new_runtime(g.take_container()),
            Self::RustFs(r) => drop_in_new_runtime(r.take_container()),
        }
    }
}

fn drop_in_new_runtime<T>(value: T) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("teardown runtime")
        .block_on(async move {
            drop(value);
            tokio::task::yield_now().await;
        });
}
