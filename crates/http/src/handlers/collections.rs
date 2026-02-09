//! Collection REST endpoint handlers.
//!
//! These handlers extract identity from the Authorization header and pass it
//! to the REST operations layer for ACP (Access Control Policy) enforcement.
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{
    extract::{Path, State},
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

/// Response for document IDs in a collection.
#[derive(Debug, Clone, Serialize)]
pub struct DocIdsResponse {
    pub doc_ids: Vec<String>,
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

/// Get all document IDs in a collection.
///
/// GET /api/v0/collections/{name}
///
/// Identity is extracted from the Authorization header and used to filter
/// documents based on read permissions (protected documents will only be
/// visible if the identity has read access).
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn get_collection_doc_ids(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(name): Path<String>,
) -> Result<Json<DocIdsResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.get_collection_doc_ids(&name, identity.did()).await {
        Ok(doc_ids) => Ok(Json(DocIdsResponse { doc_ids })),
        Err(e) => {
            tracing::warn!(collection = %name, error = %e, "Failed to get collection doc IDs");
            Err(e.into())
        }
    }
}

/// Request body for patching a collection schema.
#[derive(Debug, serde::Deserialize)]
pub struct PatchCollectionRequest {
    /// Collection name (or version ID) to patch.
    #[serde(rename = "Name", alias = "name")]
    pub name: String,
    /// JSON Patch (RFC 6902) as a JSON string or array.
    #[serde(rename = "Patch", alias = "patch")]
    pub patch: serde_json::Value,
}

/// Request body for setting the active collection version.
#[derive(Debug, serde::Deserialize)]
pub struct SetActiveRequest {
    /// The version ID to activate.
    #[serde(rename = "VersionID", alias = "version_id")]
    pub version_id: String,
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
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    let patch_str = if body.patch.is_string() {
        body.patch.as_str().unwrap().to_string()
    } else {
        serde_json::to_string(&body.patch)
            .map_err(|e| HttpError::BadRequest(format!("invalid patch: {}", e)))?
    };

    let result = collection_mgmt
        .patch_collection(&body.name, &patch_str)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(result))
}

/// Set the active collection version.
///
/// POST /api/v0/collections/set-active
///
/// Activates the specified version and deactivates other versions
/// of the same collection.
///
/// Requires `CollectionUpdate` permission when NAC is enabled.
pub async fn set_active(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<SetActiveRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let collection_mgmt = state.require_collection_mgmt()?;

    collection_mgmt
        .set_active_version(&body.version_id)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity_extractor::ExtractIdentity;
    use crate::mock::{FailingMockRestOperations, MockQueryExecutor, MockRestOperations};
    use crate::router::AppStateBuilder;
    use query::executor::QueryExecutor;
    use query::rest::RestOperations;
    use std::sync::Arc;

    fn create_state() -> AppState {
        AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
            .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
            .build()
    }

    fn create_state_without_rest() -> AppState {
        AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>).build()
    }

    fn create_failing_state() -> AppState {
        AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
            .with_rest(Arc::new(FailingMockRestOperations::new("test error")))
            .build()
    }

    #[tokio::test]
    async fn test_list_collections() {
        let state = create_state();
        let identity = ExtractIdentity::anonymous();
        let result = list_collections(State(state), identity).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.collections.contains(&"Users".to_string()));
        assert!(response.collections.contains(&"Books".to_string()));
    }

    #[tokio::test]
    async fn test_list_collections_no_rest() {
        let state = create_state_without_rest();
        let identity = ExtractIdentity::anonymous();
        let result = list_collections(State(state), identity).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_collections_error() {
        let state = create_failing_state();
        let identity = ExtractIdentity::anonymous();
        let result = list_collections(State(state), identity).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_collection_doc_ids() {
        let state = create_state();
        let identity = ExtractIdentity::anonymous();
        let result =
            get_collection_doc_ids(State(state), identity, Path("Users".to_string())).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.doc_ids.len(), 2);
        assert!(response.doc_ids.contains(&"bae-123".to_string()));
        assert!(response.doc_ids.contains(&"bae-456".to_string()));
    }

    #[tokio::test]
    async fn test_get_collection_doc_ids_not_found() {
        let state = create_state();
        let identity = ExtractIdentity::anonymous();
        let result =
            get_collection_doc_ids(State(state), identity, Path("NonExistent".to_string())).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("NonExistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_get_collection_doc_ids_no_rest() {
        let state = create_state_without_rest();
        let identity = ExtractIdentity::anonymous();
        let result =
            get_collection_doc_ids(State(state), identity, Path("Users".to_string())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_empty_collection() {
        let state = create_state();
        let identity = ExtractIdentity::anonymous();
        let result =
            get_collection_doc_ids(State(state), identity, Path("Books".to_string())).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.doc_ids.is_empty());
    }
}
