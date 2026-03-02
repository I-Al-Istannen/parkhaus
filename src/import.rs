mod logging;
pub mod s3;

use crate::config::{Config, Upstream};
use crate::data::{S3Object, S3ObjectId};
use crate::db::Database;
use crate::import::logging::{bar_progress_style, logger_config};
use crate::import::s3::{BucketInfo, S3Client};
use reqwest::Client;
use rootcause::Report;
use rootcause::hooks::Hooks;
use rootcause::hooks::builtin_hooks::report_formatter::DefaultReportFormatter;
use rootcause::prelude::ResultExt;
use rootcause_tracing::{RootcauseLayer, SpanCollector};
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
        logging::get_indicatif_layer().context("Could not build indicatif layer")?;

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
        .report_creation_hook(SpanCollector::new())
        .install()
        .context("Failed to install hooks")?;

    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    for (name, upstream) in &config.upstreams {
        info!(%name, "Importing from upstream");
        import_upstream(client.clone(), &db, upstream.clone(), import_time)
            .await
            .context(format!("failed to import from upstream {name}"))?;
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(name = %upstream.name), name = "Importing buckets of upstream")]
async fn import_upstream(
    client: Client,
    db: &Database,
    upstream: Upstream,
    import_time: Option<jiff::Timestamp>,
) -> Result<(), Report> {
    let s3 = S3Client::new(
        client,
        upstream.base_url.clone(),
        "garage",
        &upstream.s3_access_key,
        &upstream.s3_secret.0,
    );

    let buckets = s3.list_buckets().await.context("listing buckets")?;
    let buckets_span = Span::current();
    buckets_span.pb_set_style(&bar_progress_style()?);
    buckets_span.pb_set_length(buckets.len() as u64);

    info!(%upstream.name, "Importing {} buckets", buckets.len());

    for bucket in &buckets {
        let bucket_span = info_span!("Processing", bucket = bucket.name);
        bucket_span.pb_set_style(&bar_progress_style()?);

        record_objects_of_bucket(db, &upstream, &s3, bucket, import_time)
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
        .context(format!("listing objects in bucket {}", bucket.name))?;
    Span::current().pb_set_length(objects.len() as u64);

    for obj in &objects {
        db.record_creation(&S3Object {
            id: S3ObjectId {
                bucket: bucket.name.clone(),
                key: obj.key.clone(),
            },
            assigned_upstream: upstream.name.clone(),
            last_modified: import_time.unwrap_or(obj.last_modified),
        })
        .await
        .context("recording object creation in database")?;

        Span::current().pb_inc(1);
    }

    Ok(())
}
