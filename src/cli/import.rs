use super::logging::{TierLogFormatter, bar_progress_style, env_filter, get_indicatif_layer};
use crate::config::{Config, Upstream};
use crate::data::{S3Object, S3ObjectId};
use crate::db::Database;
use crate::s3::client::{BucketInfo, S3Client};
use clap::Args;
use regex::Regex;
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use rootcause_tracing::RootcauseLayer;
use std::sync::Arc;
use tracing::{Instrument, Span, info, info_span, instrument};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Args, Debug, Default)]
pub struct ImportOptions {
    /// Optional timestamp to use as the last modified time for all imported objects.
    /// If not provided, the last modified time from S3 will be used.
    #[arg(long)]
    pub modify_time: Option<jiff::Timestamp>,
    /// The bucket regex to match. Defaults to all buckets if not given.
    #[arg(long, short)]
    pub bucket_regex: Option<Regex>,
    /// The upstream regex to match. Defaults to all upstreams if not given.
    #[arg(long, short)]
    pub upstream_regex: Option<Regex>,
    /// Keep the existing last modified times from the parkhaus db and do not update them based
    /// on remote state.
    #[arg(long)]
    pub keep_modify_time: bool,
}

pub async fn run(config: Arc<Config>, db: Database, options: ImportOptions) -> Result<(), Report> {
    let indicatif_layer = get_indicatif_layer().context("failed to build indicatif layer")?;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(TierLogFormatter)
                .with_writer(indicatif_layer.get_stderr_writer()),
        )
        .with(RootcauseLayer)
        .with(indicatif_layer)
        .with(env_filter())
        .init();

    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    for (name, upstream) in &config.upstreams {
        if let Some(regex) = &options.upstream_regex
            && !regex.is_match(&name.0)
        {
            info!(%name, "Skipping upstream due to regex filter");
            continue;
        }
        info!(%name, "Importing from upstream");
        import_upstream(client.clone(), &db, upstream, &options)
            .await
            .context(format!("failed to import from upstream {name}"))?;
    }

    Ok(())
}

#[instrument(skip_all, fields(name = %upstream.name), name = "Importing buckets of upstream")]
pub async fn import_upstream(
    client: Client,
    db: &Database,
    upstream: &Upstream,
    options: &ImportOptions,
) -> Result<(), Report> {
    let s3 = S3Client::for_upstream(client, upstream);

    let mut buckets = s3.list_buckets().await?;
    if let Some(regex) = &options.bucket_regex {
        info!(%upstream.name, "Found {} buckets in upstream", buckets.len());
        buckets.retain(|it| regex.is_match(&it.name));
        info!(%upstream.name, "Kept {} buckets after filter", buckets.len());
    }

    let buckets_span = Span::current();
    buckets_span.pb_set_style(&bar_progress_style()?);
    buckets_span.pb_set_length(buckets.len() as u64);

    info!(%upstream.name, "Importing {} buckets", buckets.len());

    for bucket in &buckets {
        let bucket_span = info_span!("Processing", bucket = bucket.name);
        bucket_span.pb_set_style(&bar_progress_style()?);

        record_objects_of_bucket(db, upstream, &s3, bucket, options)
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
    options: &ImportOptions,
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
            last_modified: options.modify_time.unwrap_or(obj.last_modified),
            size: obj.size,
        })
        .collect::<Vec<_>>();

    db.bulk_import_creations(&objects, options.keep_modify_time)
        .await
        .context("failed to record object creations in database")?;

    Ok(())
}
