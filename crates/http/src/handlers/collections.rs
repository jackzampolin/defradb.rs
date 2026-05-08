//! Collection REST endpoint handlers.
//!
//! These handlers extract identity from the Authorization header and pass it
//! to the REST operations layer for ACP (Access Control Policy) enforcement.
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Response for listing collections.
#[derive(Debug, Clone, Serialize)]
pub struct CollectionsResponse {
    pub collections: Vec<String>,
}

/// List all collection names.
///
/// GET /api/v0/collections
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn list_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<CollectionsResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.list_collections().await {
        Ok(collections) => Ok(Json(CollectionsResponse { collections })),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to list collections");
            Err(e.into())
        }
    }
}

/// Go-compatible request body for patching a collection schema.
#[derive(Debug, serde::Deserialize)]
pub struct PatchCollectionRequest {
    #[serde(rename = "Patch", alias = "patch")]
    pub patch: serde_json::Value,
    #[serde(rename = "Migration", default)]
    pub migration: Option<serde_json::Value>,
    #[serde(
        rename = "SetAsDefaultVersion",
        alias = "set_as_default_version",
        default
    )]
    pub set_as_default_version: Option<bool>,
}

/// Patch a collection schema.
///
/// PATCH /api/v0/collections
///
/// Applies a JSON Patch (RFC 6902) to a collection schema, creating a
/// new schema version.
///
/// Requires `CollectionUpdate` permission when NAC is enabled.
pub async fn patch_collection(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<PatchCollectionRequest>,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    // The Patch field may be a JSON string containing patch ops, or a JSON array directly
    let patch_str = if body.patch.is_string() {
        body.patch.as_str().unwrap().to_string()
    } else {
        serde_json::to_string(&body.patch)
            .map_err(|e| HttpError::BadRequest(format!("invalid patch: {}", e)))?
    };

    // Parse the patch ops to extract the collection name from the first op's path
    let patch_ops: serde_json::Value = serde_json::from_str(&patch_str)
        .map_err(|e| HttpError::BadRequest(format!("invalid patch JSON: {}", e)))?;

    let name = extract_collection_name_from_patch(&patch_ops)?;

    collection_mgmt
        .patch_collection(&name, &patch_str)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(StatusCode::OK)
}

/// Extract the collection name from the first patch operation's path.
/// Go patches use paths like "/Users/Fields/-" where "Users" is the collection name.
fn extract_collection_name_from_patch(patch_ops: &serde_json::Value) -> Result<String, HttpError> {
    let ops = patch_ops
        .as_array()
        .ok_or_else(|| HttpError::BadRequest("patch must be a JSON array".into()))?;

    let first_op = ops
        .first()
        .ok_or_else(|| HttpError::BadRequest("patch array is empty".into()))?;

    let path = first_op
        .get("path")
        .and_then(|p| p.as_str())
        .ok_or_else(|| HttpError::BadRequest("first patch op missing 'path' field".into()))?;

    // Path format: "/<CollectionName>/Fields/-" or "/<CollectionName>/..."
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let name = segments
        .first()
        .ok_or_else(|| HttpError::BadRequest("patch path has no collection name".into()))?;

    Ok(name.to_string())
}

/// Set the active collection version.
///
/// POST /api/v0/collections/default
///
/// Accepts a plain text body containing the version ID to activate.
/// Activates the specified version and deactivates other versions
/// of the same collection.
///
/// Requires `CollectionUpdate` permission when NAC is enabled.
pub async fn set_active(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: axum::body::Bytes,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let version_id = String::from_utf8(body.to_vec())
        .map_err(|_| HttpError::BadRequest("invalid UTF-8".into()))?;

    let collection_mgmt = state.require_collection_mgmt()?;

    collection_mgmt
        .set_active_version(version_id.trim())
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(StatusCode::OK)
}

/// Truncate all documents in a collection.
///
/// DELETE /api/v0/collections/{name}/truncate
///
/// Deletes all documents, heads, blocks, and index entries while
/// preserving the collection schema.
///
/// Requires `DocumentDelete` permission when NAC is enabled.
pub async fn truncate_collection(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(name): Path<String>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionTruncate).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    collection_mgmt
        .truncate_collection(&name)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
}

/// Describe a collection by name.
///
/// GET /api/v0/collections/{name}/describe
///
/// Returns the CollectionVersion JSON for the named collection, or 404.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn describe_collection(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    match collection_mgmt.get_collection_by_name(&name).await {
        Ok(Some(cv)) => {
            let val = serde_json::to_value(&cv)
                .map_err(|e| HttpError::Internal(format!("serialization error: {}", e)))?;
            Ok(Json(val))
        }
        Ok(None) => Err(HttpError::NotFound(format!(
            "collection '{}' not found",
            name
        ))),
        Err(e) => Err(HttpError::BadRequest(e)),
    }
}

/// Check if a collection exists.
///
/// GET /api/v0/collections/{name}/exists
///
/// Returns `{"exists": true/false}`.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn collection_exists(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    let exists = collection_mgmt
        .has_collection(&name)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(serde_json::json!({ "exists": exists })))
}

/// Find a collection by its collection ID.
///
/// GET /api/v0/collections/by-id/{id}
///
/// Returns the CollectionVersion JSON or null.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn find_collection_by_id(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    match collection_mgmt.find_collection_by_id(&id).await {
        Ok(Some(cv)) => {
            let val = serde_json::to_value(&cv)
                .map_err(|e| HttpError::Internal(format!("serialization error: {}", e)))?;
            Ok(Json(val))
        }
        Ok(None) => Ok(Json(serde_json::Value::Null)),
        Err(e) => Err(HttpError::BadRequest(e)),
    }
}

/// Get a collection by version ID, searching both cache and storage.
///
/// GET /api/v0/collections/by-version/{id}
///
/// Returns the CollectionVersion JSON or null.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn get_collection_by_version_id(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    match collection_mgmt.get_collection_by_version_id(&id).await {
        Ok(Some(cv)) => {
            let val = serde_json::to_value(&cv)
                .map_err(|e| HttpError::Internal(format!("serialization error: {}", e)))?;
            Ok(Json(val))
        }
        Ok(None) => Ok(Json(serde_json::Value::Null)),
        Err(e) => Err(HttpError::BadRequest(e)),
    }
}

/// Delete multiple collection versions.
///
/// DELETE /api/v0/collections/versions
///
/// Body: JSON array of version ID strings.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn delete_collection_versions(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(version_ids): Json<Vec<String>>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    collection_mgmt
        .delete_collection_versions(version_ids)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(serde_json::json!({})))
}

/// Get all collection versions (active + inactive).
///
/// GET /api/v0/collections/versions
///
/// Returns a JSON array of all CollectionVersion objects from the system store.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn get_all_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<schema::CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    let collections = collection_mgmt
        .get_all_collections()
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(collections))
}

/// Delete a collection by name.
///
/// DELETE /api/v0/collections/{name}
///
/// Removes the collection and all its versions.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn delete_collection(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    collection_mgmt
        .delete_collection(&name)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(serde_json::json!({})))
}

#[cfg(test)]
#[path = "collections_tests.rs"]
mod tests;
