mod logging;

use crate::config::{Config, Upstream};
use crate::data::{S3Object, S3ObjectId};
use crate::db::Database;
use crate::import::logging::{bar_progress_style, logger_config};
use crate::s3_client::client::{BucketInfo, S3Client};
use reqwest::Client;
use rootcause::Report;
use rootcause::hooks::Hooks;
use rootcause::hooks::builtin_hooks::report_formatter::DefaultReportFormatter;
use rootcause::prelude::ResultExt;
use rootcause_tracing::RootcauseLayer;
use std::sync::Arc;
use tracing::{Instrument, Span, info, info_span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub async fn import(
    config: Arc<Config>,
    db: Database,
    import_time: Option<jiff::Timestamp>,
) -> Result<(), Report> {
    let indicatif_layer =
        logging::get_indicatif_layer().context("failed to build indicatif layer")?;

    tracing_subscriber::registry()
        .with(logger_config(indicatif_layer.get_stderr_writer()))
        .with(RootcauseLayer)
        .with(indicatif_layer)
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    Hooks::new()
        .report_formatter(DefaultReportFormatter::UNICODE_COLORS)
        .install()
        .context("failed to install hooks")?;

    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    for (name, upstream) in &config.upstreams {
        info!(%name, "Importing from upstream");
        import_upstream(client.clone(), &db, upstream, import_time)
            .await
            .context(format!("failed to import from upstream {name}"))?;
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(name = %upstream.name), name = "Importing buckets of upstream")]
async fn import_upstream(
    client: Client,
    db: &Database,
    upstream: &Upstream,
    import_time: Option<jiff::Timestamp>,
) -> Result<(), Report> {
    let s3 = S3Client::new(
        client,
        upstream.base_url.clone(),
        "garage",
        &upstream.s3_access_key,
        &upstream.s3_secret.0,
    );

    let buckets = s3.list_buckets().await?;
    let buckets_span = Span::current();
    buckets_span.pb_set_style(&bar_progress_style()?);
    buckets_span.pb_set_length(buckets.len() as u64);

    info!(%upstream.name, "Importing {} buckets", buckets.len());

    for bucket in &buckets {
        let bucket_span = info_span!("Processing", bucket = bucket.name);
        bucket_span.pb_set_style(&bar_progress_style()?);

        record_objects_of_bucket(db, upstream, &s3, bucket, import_time)
            .instrument(bucket_span.clone())
            .await?;

        info!(%bucket.name, "  Finished");
        buckets_span.pb_inc(1);
    }

    info!(%upstream.name, "Finished upstream");

    Ok(())
}

async fn record_objects_of_bucket(
    db: &Database,
    upstream: &Upstream,
    s3: &S3Client,
    bucket: &BucketInfo,
    import_time: Option<jiff::Timestamp>,
) -> Result<(), Report> {
    let objects = s3
        .list_objects(&bucket.name)
        .instrument(info_span!("Listing objects"))
        .await
        .context(format!("failed to list objects in bucket {}", bucket.name))?;
    Span::current().pb_set_length(objects.len() as u64);

    let objects = objects
        .into_iter()
        .map(|obj| S3Object {
            id: S3ObjectId {
                bucket: bucket.name.clone(),
                key: obj.key.clone(),
            },
            assigned_upstream: upstream.name.clone(),
            last_modified: import_time.unwrap_or(obj.last_modified),
        })
        .collect::<Vec<_>>();

    db.bulk_import_creations(&objects)
        .await
        .context("failed to record object creations in database")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::{AddressingStyle, S3Secret, Upstream};
    use crate::data::{S3ObjectId, UpstreamId};
    use crate::db::Database;
    use crate::import::import_upstream;
    use crate::s3_client::client::S3Client;
    use crate::testing::garage::GarageInstance;
    use jiff::Timestamp;
    use rootcause::Report;
    use rootcause::prelude::ResultExt;
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::time::Duration;
    use tokio::sync::OnceCell;

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
}
