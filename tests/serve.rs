#![allow(unused_crate_dependencies)]

use parkhaus::config::{Config, S3Secret, Upstream, UpstreamId};
use parkhaus::db::Database;
use parkhaus::s3::types::AddressingStyle;
use rootcause::Report;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn bind_failure_stops_the_server() -> Result<(), Report> {
    let occupied = TcpListener::bind("127.0.0.1:0")?;
    let config = Config::new(
        occupied.local_addr()?.to_string(),
        None,
        PathBuf::default(),
        vec![Upstream {
            name: UpstreamId("hot".to_owned()),
            order: 1,
            base_url: "http://127.0.0.1:9".parse()?,
            addressing_style: AddressingStyle::Path,
            s3_access_key: "test".to_owned(),
            s3_secret: S3Secret("test".to_owned()),
            region: "test".to_owned(),
        }],
        vec![],
    )?;

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        parkhaus::cli::serve::run(Arc::new(config), Database::in_memory().await?),
    )
    .await;
    assert!(matches!(result, Ok(Err(_))), "{result:?}");
    Ok(())
}
