//! Cleanup of multipart uploads clients never completed. Upstream lifecycle rules would do this,
//! but parkhaus requires them to be off.

use crate::config::Config;
use crate::db::Database;
use crate::s3::client::S3Client;
use jiff::{SignedDuration, Timestamp};
use reqwest::Client;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use tracing::{info, warn};

pub const ABANDONED_UPLOAD_AGE: SignedDuration = SignedDuration::from_hours(7 * 24);

/// Aborts uploads created before `now - ABANDONED_UPLOAD_AGE` and returns how many.
pub async fn reap_abandoned_uploads(
    config: &Config,
    db: &Database,
    http: &Client,
    now: Timestamp,
) -> Result<usize, Report> {
    let cutoff = now - ABANDONED_UPLOAD_AGE;
    let uploads = db.get_uploads_older_than(cutoff).await?;
    let mut aborted = 0;
    for upload in &uploads {
        if let Some(upstream) = config.upstreams.get(&upload.upstream) {
            let result = S3Client::for_upstream(http.clone(), upstream)
                .abort_multipart_upload(&upload.object, &upload.upload_id)
                .await
                .context("failed to abort multipart upload");
            if let Err(e) = result {
                warn!(%e, ?upload, "failed to abort abandoned multipart upload");
                continue;
            }
        }
        db.remove_upload(&upload.upload_id).await?;
        aborted += 1;
    }
    if aborted > 0 {
        info!(aborted, "aborted abandoned multipart uploads");
    }
    Ok(aborted)
}
