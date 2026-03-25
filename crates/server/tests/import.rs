#![allow(unused_crate_dependencies)]

use crate::common::garage::GarageInstance;
use jiff::Timestamp;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use server::config::{AddressingStyle, S3Secret, Upstream, UpstreamId};
use server::data::S3ObjectId;
use server::db::Database;
use server::import::import_upstream;
use server::s3_client::client::S3Client;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::time::Duration;
use tokio::sync::OnceCell;

mod common;

static GARAGE: OnceCell<GarageInstance> = OnceCell::const_new();

#[ctor::dtor]
fn shutdown_garage() {
    if let Some(container) = GARAGE.get().and_then(GarageInstance::take_container) {
        GarageInstance::drop_in_new_runtime(container);
    }
}

async fn garage() -> Result<&'static GarageInstance, Report> {
    GARAGE.get_or_try_init(GarageInstance::start).await
}

#[tokio::test]
async fn test_import_buckets() -> Result<(), Report> {
    let garage = garage().await?;
    let reqwest_client = reqwest::Client::new();

    let (key_id, key_secret) = garage.create_key("access-key").await?;
    let upstream = Upstream {
        name: UpstreamId("garage".to_string()),
        order: 0,
        base_url: garage.s3_endpoint().clone(),
        region: garage.region().to_string(),
        addressing_style: AddressingStyle::Path,
        max_age: None,
        s3_access_key: key_id.clone(),
        s3_secret: S3Secret(key_secret.clone()),
    };
    let s3 = S3Client::new(
        reqwest_client.clone(),
        garage.s3_endpoint().clone(),
        garage.region(),
        &key_id,
        &key_secret,
        AddressingStyle::Path,
    );

    let bucket_names = (0..5).map(|i| format!("bucket-{i}")).collect::<Vec<_>>();
    for bucket_name in &bucket_names {
        let bucket_id = garage.create_bucket(bucket_name).await?;
        garage.allow_key_on_bucket(&bucket_id, &key_id).await?;
    }

    let mut expected_objects: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for bucket_name in &bucket_names {
        let mut objects = Vec::new();

        for i in 0..rand::random_range(1..1000) {
            let payload = format!("payload-{bucket_name}-{i}").into_bytes();
            let payload_len = payload.len();
            let id = S3ObjectId {
                bucket: bucket_name.to_string(),
                key: format!("obj-{bucket_name}-{i}"),
            };
            s3.put_file(&id, Cursor::new(payload), payload_len as u64)
                .await?;

            objects.push(id.key);
        }

        expected_objects.insert(bucket_name.clone(), objects);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Act
    let remote_time_db = Database::in_memory().await?;
    import_upstream(reqwest_client.clone(), &remote_time_db, &upstream, None).await?;
    let custom_time_db = Database::in_memory().await?;
    let target_import_time = Timestamp::from_second(0xB105_F00D)?;
    import_upstream(
        reqwest_client,
        &custom_time_db,
        &upstream,
        Some(target_import_time),
    )
    .await?;

    // Assert
    let last_time = Timestamp::MIN;
    for bucket_name in &bucket_names {
        let expected = expected_objects.get(bucket_name).unwrap();
        for key in expected {
            let id = S3ObjectId {
                bucket: bucket_name.clone(),
                key: key.clone(),
            };
            let obj = remote_time_db.get_object(&id).await.context(format!(
                "failed to get object {bucket_name}/{key} from database"
            ))?;
            assert_eq!(obj.assigned_upstream, upstream.name);
            assert!(
                obj.last_modified >= last_time,
                "last_modified should use the remote timestamp"
            );
            assert!(
                obj.last_modified < Timestamp::now(),
                "last_modified must be in the past"
            );

            assert_eq!(
                custom_time_db.get_object(&id).await?.last_modified,
                target_import_time
            );
        }
    }

    Ok(())
}
