//! DeleteObjects sent to every upstream, as unindexed objects can live on any of them.
//!
//! The client's request is forwarded unchanged, so its signature stays valid. An object is deleted
//! if no upstream reports an error other than `NoSuchKey` for it, which some upstreams (e.g.
//! Garage) report for keys held by other upstreams.

use crate::config::{Upstream, UpstreamId};
use crate::data::S3ObjectId;
use crate::error::TierError;
use crate::server::endpoints;
use crate::server::listing::query_param;
use crate::server::metrics::COUNTER_OBJECT_DELETIONS_TOTAL;
use crate::server::state::AppState;
use axum::body::{Body, to_bytes};
use axum::extract::Request;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Method};
use axum::response::{IntoResponse, Response};
use axum_prometheus::metrics::counter;
use futures_util::future::join_all;
use reqwest::StatusCode;
use rootcause::Report;
use rootcause::prelude::ResultExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use tracing::warn;

/// 1000 keys of at most 1024 bytes each, plus markup.
const MAX_BODY_SIZE: usize = 4 * 1024 * 1024;
const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub(super) fn is_delete_objects(req: &Request) -> bool {
    *req.method() == Method::POST && query_param(req, "delete").is_some()
}

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
    let objects: BTreeSet<_> = versions
        .iter()
        .map(|it| S3ObjectId {
            bucket: bucket.to_string(),
            key: it.key.clone(),
        })
        .collect();

    // A migration running concurrently could copy an object to its target after we deleted it
    // there. Readers queue behind waiting migrations, so batches lock in key order (and each key
    // once) to not deadlock each other.
    let mut guards = Vec::with_capacity(objects.len());
    for object in &objects {
        guards.push(
            state
                .migration_locks
                .wait_for_migration(&Method::POST, object)
                .await,
        );
    }

    let upstreams = state.config.upstreams_in_order();
    let results = join_all(upstreams.iter().map(|upstream| {
        let req = Request::from_parts(parts.clone(), Body::from(body.clone()));
        async move {
            match delete_on(state, bucket, upstream, req).await {
                Ok(result) => result,
                Err(error) => Err(error.into_response()),
            }
        }
    }))
    .await;

    // Responses are not `Sync`, so only the successful results are kept across awaits
    let succeeded: Vec<_> = upstreams
        .iter()
        .zip(&results)
        .filter_map(|(upstream, result)| Some((&upstream.name, result.as_ref().ok()?)))
        .collect();
    if let Err(e) = record_deletions(state, &objects, &succeeded).await {
        warn!(%e, "failed to record batch deletion");
    }
    drop(guards);

    let results = match results.into_iter().collect::<Result<Vec<_>, _>>() {
        Ok(results) => results,
        Err(response) => return Ok(response),
    };

    let body = quick_xml::se::to_string(&DeleteResult::merge(&results, &versions))
        .context("failed to serialize DeleteObjects response")?;
    let mut response = Response::new(Body::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string() + &body,
    ));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    Ok(response)
}

/// Returns the upstream's error response as is.
async fn delete_on(
    state: &AppState,
    bucket: &str,
    upstream: &Upstream,
    req: Request,
) -> Result<Result<DeleteResult, Response>, TierError> {
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

/// Forgets objects that are gone from their assigned upstream, even if others failed.
async fn record_deletions(
    state: &AppState,
    objects: &BTreeSet<S3ObjectId>,
    succeeded: &[(&UpstreamId, &DeleteResult)],
) -> Result<(), Report> {
    for object in objects {
        let Some(assigned) = state
            .db
            .get_upstream(object)
            .await
            .context("failed to get upstream for object")?
        else {
            continue;
        };
        let gone = succeeded.iter().any(|(upstream, result)| {
            **upstream == assigned
                && !result
                    .errors
                    .iter()
                    .any(|it| !it.is_missing() && it.key == object.key)
        });
        if !gone {
            continue;
        }
        counter!(COUNTER_OBJECT_DELETIONS_TOTAL, "upstream" => assigned.0.clone()).increment(1);
        state
            .db
            .delete_object(object)
            .await
            .context("failed to record deletion")
            .attach(format!("object: {object:?}"))?;
    }
    Ok(())
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
