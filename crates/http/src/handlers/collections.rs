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

/// Patch a collection schema.
///
/// PATCH /api/v0/collections
pub async fn patch_collection() -> Result<Json<()>, HttpError> {
    Err(HttpError::NotImplemented(
        "collection patch is not yet implemented".into(),
    ))
}

/// Set the active collection version.
///
/// POST /api/v0/collections/set-active
pub async fn set_active() -> Result<Json<()>, HttpError> {
    Err(HttpError::NotImplemented(
        "collection set-active is not yet implemented".into(),
    ))
}

/// Truncate all documents in a collection.
///
/// DELETE /api/v0/collections/{name}/truncate
pub async fn truncate_collection() -> Result<Json<()>, HttpError> {
    Err(HttpError::NotImplemented(
        "collection truncate is not yet implemented".into(),
    ))
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
