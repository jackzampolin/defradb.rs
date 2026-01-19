//! Collection REST endpoint handlers.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Serialize;

use crate::error::HttpError;
use crate::router::AppState;

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
pub async fn list_collections(
    State(state): State<AppState>,
) -> Result<Json<CollectionsResponse>, HttpError> {
    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.list_collections().await {
        Ok(collections) => Ok(Json(CollectionsResponse { collections })),
        Err(e) => {
            tracing::error!(error = %e, "Failed to list collections");
            Err(HttpError::Internal(e.to_string()))
        }
    }
}

/// Get all document IDs in a collection.
///
/// GET /api/v0/collections/{name}
pub async fn get_collection_doc_ids(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<DocIdsResponse>, HttpError> {
    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.get_collection_doc_ids(&name).await {
        Ok(doc_ids) => Ok(Json(DocIdsResponse { doc_ids })),
        Err(e) => {
            tracing::warn!(collection = %name, error = %e, "Failed to get collection doc IDs");
            match e {
                query::rest::RestError::CollectionNotFound(_) => Err(HttpError::NotFound(format!(
                    "Collection '{}' not found",
                    name
                ))),
                _ => Err(HttpError::Internal(e.to_string())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{FailingMockRestOperations, MockQueryExecutor, MockRestOperations};
    use query::executor::QueryExecutor;
    use query::rest::RestOperations;
    use std::sync::Arc;

    fn create_state() -> AppState {
        AppState {
            executor: Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>,
            rest: Some(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>),
        }
    }

    fn create_state_without_rest() -> AppState {
        AppState {
            executor: Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>,
            rest: None,
        }
    }

    fn create_failing_state() -> AppState {
        AppState {
            executor: Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>,
            rest: Some(Arc::new(FailingMockRestOperations::new("test error"))),
        }
    }

    #[tokio::test]
    async fn test_list_collections() {
        let state = create_state();
        let result = list_collections(State(state)).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.collections.contains(&"Users".to_string()));
        assert!(response.collections.contains(&"Books".to_string()));
    }

    #[tokio::test]
    async fn test_list_collections_no_rest() {
        let state = create_state_without_rest();
        let result = list_collections(State(state)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_collections_error() {
        let state = create_failing_state();
        let result = list_collections(State(state)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_collection_doc_ids() {
        let state = create_state();
        let result = get_collection_doc_ids(State(state), Path("Users".to_string())).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.doc_ids.len(), 2);
        assert!(response.doc_ids.contains(&"bae-123".to_string()));
        assert!(response.doc_ids.contains(&"bae-456".to_string()));
    }

    #[tokio::test]
    async fn test_get_collection_doc_ids_not_found() {
        let state = create_state();
        let result = get_collection_doc_ids(State(state), Path("NonExistent".to_string())).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("NonExistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_get_collection_doc_ids_no_rest() {
        let state = create_state_without_rest();
        let result = get_collection_doc_ids(State(state), Path("Users".to_string())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_empty_collection() {
        let state = create_state();
        let result = get_collection_doc_ids(State(state), Path("Books".to_string())).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.doc_ids.is_empty());
    }
}
