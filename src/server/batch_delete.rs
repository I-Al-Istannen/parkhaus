//! DeleteObjects sent to every upstream, as the signed body cannot be split by upstream.
//!
//! The client's request is forwarded unchanged, so its signature stays valid. An object is deleted
//! if no upstream reports an error other than `NoSuchKey` for it, which some upstreams (e.g.
//! Garage) report for keys they do not hold.

use crate::config::{Upstream, UpstreamId};
use crate::data::S3ObjectId;
use crate::db::{DirtyObject, Resolution};
use crate::error::TierError;
use crate::server::classify::not_supported;
use crate::server::endpoints;
use crate::server::metrics::COUNTER_OBJECT_DELETIONS_TOTAL;
use crate::server::resolve::resolve;
use crate::server::state::AppState;
use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum_prometheus::metrics::counter;
use futures_util::future::join_all;
use reqwest::StatusCode;
use rootcause::prelude::ResultExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use tracing::warn;

/// 1000 keys of at most 1024 bytes each, plus markup.
const MAX_BODY_SIZE: usize = 4 * 1024 * 1024;
const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Deserialize)]
struct DeleteRequest {
    #[serde(rename = "Object", default)]
    objects: Vec<ObjectVersion>,
}

#[derive(Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ObjectVersion {
    key: String,
    version_id: Option<String>,
}

impl ObjectVersion {
    /// Upstreams may name the deleted version even if the request did not.
    fn matches(&self, key: &str, version_id: Option<&str>) -> bool {
        self.key == key
            && self
                .version_id
                .as_deref()
                .is_none_or(|it| Some(it) == version_id)
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename = "DeleteResult")]
struct DeleteResult {
    #[serde(rename = "@xmlns", default, skip_deserializing)]
    xmlns: &'static str,
    #[serde(rename = "Deleted", default)]
    deleted: Vec<Deleted>,
    #[serde(rename = "Error", default)]
    errors: Vec<DeleteError>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Deleted {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete_marker: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete_marker_version_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteError {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    code: String,
    message: String,
}

impl DeleteError {
    fn is_missing(&self) -> bool {
        self.code == "NoSuchKey"
    }
}

impl DeleteResult {
    fn error_for(&self, object: &ObjectVersion, missing: bool) -> Option<&DeleteError> {
        self.errors.iter().find(|it| {
            it.is_missing() == missing && object.matches(&it.key, it.version_id.as_deref())
        })
    }

    /// Quiet responses only list errors, so anything without a real error was deleted.
    fn reports_deleted(&self, key: &str) -> bool {
        !self
            .errors
            .iter()
            .any(|it| !it.is_missing() && it.key == key)
    }

    fn deleted_for(&self, object: &ObjectVersion) -> Option<&Deleted> {
        self.deleted
            .iter()
            .find(|it| object.matches(&it.key, it.version_id.as_deref()))
    }

    /// Prefers real errors over deletions. An object is only missing if it is missing everywhere,
    /// as quiet responses omit deletions.
    fn merge(results: &[Self], objects: &[ObjectVersion]) -> Self {
        let mut merged = Self {
            xmlns: S3_XMLNS,
            ..Self::default()
        };
        for object in objects {
            if let Some(error) = results.iter().find_map(|it| it.error_for(object, false)) {
                merged.errors.push(error.clone());
            } else if let Some(deleted) = results.iter().find_map(|it| it.deleted_for(object)) {
                merged.deleted.push(deleted.clone());
            } else if let Some(error) = results.first().and_then(|it| it.error_for(object, true))
                && results
                    .iter()
                    .all(|it| it.error_for(object, true).is_some())
            {
                merged.errors.push(error.clone());
            }
        }
        merged
    }
}

pub(super) async fn proxy_delete_objects(
    state: &AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, TierError> {
    let (parts, body) = req.into_parts();
    let body = to_bytes(body, MAX_BODY_SIZE)
        .await
        .context("failed to read DeleteObjects request")
        .attach(StatusCode::BAD_REQUEST)?;
    let request: DeleteRequest = quick_xml::de::from_reader(&body[..])
        .context("invalid DeleteObjects request")
        .attach(StatusCode::BAD_REQUEST)?;

    let mut seen = HashSet::new();
    let versions: Vec<_> = request
        .objects
        .into_iter()
        .map(|it| ObjectVersion {
            version_id: it.version_id.filter(|it| !it.is_empty()),
            ..it
        })
        .filter(|it| seen.insert(it.clone()))
        .collect();
    if versions.iter().any(|it| it.version_id.is_some()) {
        return Err(not_supported("deleting object versions"));
    }
    let objects: BTreeSet<_> = versions
        .iter()
        .map(|it| S3ObjectId {
            bucket: bucket.to_string(),
            key: it.key.clone(),
        })
        .collect();

    // Detached, so a client disconnect cannot cancel the bookkeeping.
    let state = state.clone();
    let bucket = bucket.to_string();
    let results = tokio::spawn(async move {
        let _guards = state.key_locks.lock_all(&objects).await;
        let assigned = mark_dirty(&state, &objects).await?;

        let upstreams = state.config.upstreams_in_order();
        let results = join_all(upstreams.iter().map(|upstream| {
            let req = Request::from_parts(parts.clone(), Body::from(body.clone()));
            delete_on(&state, &bucket, upstream, req)
        }))
        .await;

        // Responses are not `Sync`, so they must not be borrowed across awaits
        let outcomes = assigned
            .into_iter()
            .map(|(object, upstream)| {
                let deleted = upstreams.iter().zip(&results).any(|(it, result)| {
                    it.name == upstream
                        && matches!(result, Ok(Ok(result)) if result.reports_deleted(&object.key))
                });
                (object, upstream, deleted)
            })
            .collect::<Vec<_>>();
        resolve_deletions(&state, outcomes).await;
        Ok::<_, TierError>(results)
    })
    .await
    .context("DeleteObjects task failed")??;

    let mut successful = Vec::new();
    for result in results {
        match result? {
            Ok(result) => successful.push(result),
            Err(response) => return Ok(response.into_response()),
        }
    }

    let body = quick_xml::se::to_string(&DeleteResult::merge(&successful, &versions))
        .context("failed to serialize DeleteObjects response")?;
    let mut response = Response::new(Body::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string() + &body,
    ));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(response)
}

/// Resolves leftovers and marks every known object dirty on its upstream.
async fn mark_dirty(
    state: &AppState,
    objects: &BTreeSet<S3ObjectId>,
) -> Result<Vec<(S3ObjectId, UpstreamId)>, TierError> {
    let mut assigned = Vec::new();
    for object in objects {
        resolve(&state.config, &state.db, &state.http, object)
            .await
            .context("failed to resolve earlier mutation")?;
        let Some(upstream) = state
            .db
            .get_upstream(object)
            .await
            .context("failed to get upstream for object")?
        else {
            continue;
        };
        state
            .db
            .mark_dirty(
                object,
                &DirtyObject {
                    target: upstream.clone(),
                    previous: None,
                    migration: false,
                },
            )
            .await
            .context("failed to mark object dirty")?;
        assigned.push((object.clone(), upstream));
    }
    Ok(assigned)
}

/// Objects their upstream reported as deleted are forgotten right away, everything else is checked
/// with a HEAD.
async fn resolve_deletions(state: &AppState, outcomes: Vec<(S3ObjectId, UpstreamId, bool)>) {
    for (object, upstream, deleted) in &outcomes {
        let resolved = if *deleted {
            counter!(COUNTER_OBJECT_DELETIONS_TOTAL, "upstream" => upstream.0.clone()).increment(1);
            state.db.resolve_dirty(object, &Resolution::Delete).await
        } else {
            resolve(&state.config, &state.db, &state.http, object).await
        };
        if let Err(e) = resolved {
            warn!(%e, %object, "failed to resolve deletion, leaving it to the sweep");
        }
    }
}

type UpstreamResult = Result<Result<DeleteResult, Response>, TierError>;

/// Returns the upstream's error response as is.
async fn delete_on(
    state: &AppState,
    bucket: &str,
    upstream: &Upstream,
    req: Request,
) -> UpstreamResult {
    let mut url = upstream.format_url(bucket, None)?;
    url.url.set_query(req.uri().query());
    let response = endpoints::forward_request(state, upstream, url, req, async {}).await?;
    if !response.status().is_success() {
        return Ok(Err(response));
    }
    let body = to_bytes(response.into_body(), MAX_BODY_SIZE)
        .await
        .context("failed to read DeleteObjects response")?;
    Ok(Ok(quick_xml::de::from_reader(&body[..])
        .context("invalid DeleteObjects response")
        .attach(format!("upstream: {}", upstream.name))?))
}

#[cfg(test)]
mod tests {
    use super::{DeleteResult, ObjectVersion};

    fn version(key: &str, version_id: Option<&str>) -> ObjectVersion {
        ObjectVersion {
            key: key.to_string(),
            version_id: version_id.map(str::to_string),
        }
    }

    fn result(entries: &str) -> DeleteResult {
        quick_xml::de::from_str(&format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{entries}</DeleteResult>"
        ))
        .unwrap()
    }

    #[test]
    fn merges_delete_results() {
        let hot = result(
            "<Deleted><Key>a&amp;b</Key><VersionId>v1</VersionId></Deleted>\
             <Error><Key>c</Key><Code>NoSuchKey</Code><Message>no</Message></Error>\
             <Deleted><Key>d</Key></Deleted>",
        );
        let cold = result(
            "<Error><Key>a&amp;b</Key><Code>NoSuchKey</Code><Message>no</Message></Error>\
             <Error><Key>c</Key><Code>NoSuchKey</Code><Message>no</Message></Error>\
             <Error><Key>d</Key><Code>AccessDenied</Code><Message>no</Message></Error>",
        );
        let objects = [version("a&b", None), version("c", None), version("d", None)];

        let merged = DeleteResult::merge(&[hot, cold], &objects);
        let xml = quick_xml::se::to_string(&merged).unwrap();

        assert_eq!(
            xml,
            "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Deleted><Key>a&amp;b</Key><VersionId>v1</VersionId></Deleted>\
             <Error><Key>c</Key><Code>NoSuchKey</Code><Message>no</Message></Error>\
             <Error><Key>d</Key><Code>AccessDenied</Code><Message>no</Message></Error>\
             </DeleteResult>"
        );
    }

    #[test]
    fn merges_quiet_delete_results() {
        let hot = result("<Error><Key>a</Key><Code>NoSuchKey</Code><Message>no</Message></Error>");
        let cold = result("");
        let merged = DeleteResult::merge(&[hot, cold], &[version("a", None)]);

        assert!(merged.deleted.is_empty());
        assert!(merged.errors.is_empty());
    }

    #[test]
    fn matches_requested_versions() {
        assert!(version("a", None).matches("a", Some("v1")));
        assert!(version("a", Some("v1")).matches("a", Some("v1")));
        assert!(!version("a", Some("v1")).matches("a", Some("v2")));
        assert!(!version("a", Some("v1")).matches("a", None));
        assert!(!version("a", None).matches("b", None));
    }
}
