//! Document REST endpoint handlers.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::error::HttpError;
use crate::router::AppState;

/// Response for delete operations.
#[derive(Debug, Clone, Serialize)]
pub struct DeleteResponse {
    pub deleted: bool,
}

/// Get a single document by ID.
///
/// GET /api/v0/collections/{name}/{docID}
pub async fn get_document(
    State(state): State<AppState>,
    Path((collection, doc_id)): Path<(String, String)>,
) -> Result<Json<JsonValue>, HttpError> {
    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.get_document(&collection, &doc_id).await {
        Ok(Some(doc)) => Ok(Json(doc)),
        Ok(None) => Err(HttpError::NotFound(format!(
            "Document '{}' not found in collection '{}'",
            doc_id, collection
        ))),
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                doc_id = %doc_id,
                error = %e,
                "Failed to get document"
            );
            match e {
                query::rest::RestError::CollectionNotFound(_) => Err(HttpError::NotFound(format!(
                    "Collection '{}' not found",
                    collection
                ))),
                query::rest::RestError::InvalidDocId(_) => Err(HttpError::BadRequest(format!(
                    "Invalid document ID: {}",
                    doc_id
                ))),
                query::rest::RestError::InvalidInput(msg) => Err(HttpError::BadRequest(msg)),
                query::rest::RestError::PermissionDenied(msg) => Err(HttpError::Forbidden(msg)),
                query::rest::RestError::Internal(msg) => Err(HttpError::Internal(msg)),
                query::rest::RestError::DocumentNotFound(_) => Err(HttpError::NotFound(format!(
                    "Document '{}' not found in collection '{}'",
                    doc_id, collection
                ))),
            }
        }
    }
}

/// Create document(s) in a collection.
///
/// POST /api/v0/collections/{name}
///
/// Accepts either a single document object or an array of documents.
pub async fn create_document(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(body): Json<JsonValue>,
) -> Result<Json<JsonValue>, HttpError> {
    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    let result = if body.is_array() {
        let docs: Vec<JsonValue> = body
            .as_array()
            .ok_or_else(|| HttpError::BadRequest("Expected array of documents".into()))?
            .clone();
        rest.create_documents(&collection, docs).await
    } else {
        rest.create_document(&collection, body)
            .await
            .map(|doc| vec![doc])
    };

    match result {
        Ok(docs) => {
            tracing::info!(
                collection = %collection,
                count = docs.len(),
                "Documents created"
            );
            // Return single doc if single input, array if array input
            let response = if docs.len() == 1 {
                docs.into_iter()
                    .next()
                    .expect("docs.len() == 1 but iterator was empty")
            } else {
                JsonValue::Array(docs)
            };
            Ok(Json(response))
        }
        Err(e) => {
            tracing::warn!(collection = %collection, error = %e, "Failed to create document");
            match e {
                query::rest::RestError::CollectionNotFound(_) => Err(HttpError::NotFound(format!(
                    "Collection '{}' not found",
                    collection
                ))),
                query::rest::RestError::InvalidInput(msg) => Err(HttpError::BadRequest(msg)),
                query::rest::RestError::InvalidDocId(msg) => Err(HttpError::BadRequest(format!(
                    "Invalid document ID: {}",
                    msg
                ))),
                query::rest::RestError::PermissionDenied(msg) => Err(HttpError::Forbidden(msg)),
                query::rest::RestError::Internal(msg) => Err(HttpError::Internal(msg)),
                query::rest::RestError::DocumentNotFound(_) => Err(HttpError::NotFound(format!(
                    "Document not found in collection '{}'",
                    collection
                ))),
            }
        }
    }
}

/// Update a single document.
///
/// PATCH /api/v0/collections/{name}/{docID}
pub async fn update_document(
    State(state): State<AppState>,
    Path((collection, doc_id)): Path<(String, String)>,
    Json(patch): Json<JsonValue>,
) -> Result<Json<JsonValue>, HttpError> {
    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.update_document(&collection, &doc_id, patch).await {
        Ok(doc) => {
            tracing::info!(
                collection = %collection,
                doc_id = %doc_id,
                "Document updated"
            );
            Ok(Json(doc))
        }
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                doc_id = %doc_id,
                error = %e,
                "Failed to update document"
            );
            match e {
                query::rest::RestError::CollectionNotFound(_) => Err(HttpError::NotFound(format!(
                    "Collection '{}' not found",
                    collection
                ))),
                query::rest::RestError::DocumentNotFound(_) => Err(HttpError::NotFound(format!(
                    "Document '{}' not found in collection '{}'",
                    doc_id, collection
                ))),
                query::rest::RestError::InvalidDocId(_) => Err(HttpError::BadRequest(format!(
                    "Invalid document ID: {}",
                    doc_id
                ))),
                query::rest::RestError::InvalidInput(msg) => Err(HttpError::BadRequest(msg)),
                query::rest::RestError::PermissionDenied(msg) => Err(HttpError::Forbidden(msg)),
                query::rest::RestError::Internal(msg) => Err(HttpError::Internal(msg)),
            }
        }
    }
}

/// Delete a single document.
///
/// DELETE /api/v0/collections/{name}/{docID}
pub async fn delete_document(
    State(state): State<AppState>,
    Path((collection, doc_id)): Path<(String, String)>,
) -> Result<Json<DeleteResponse>, HttpError> {
    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest.delete_document(&collection, &doc_id).await {
        Ok(deleted) => {
            if deleted {
                tracing::info!(
                    collection = %collection,
                    doc_id = %doc_id,
                    "Document deleted"
                );
            }
            Ok(Json(DeleteResponse { deleted }))
        }
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                doc_id = %doc_id,
                error = %e,
                "Failed to delete document"
            );
            match e {
                query::rest::RestError::CollectionNotFound(_) => Err(HttpError::NotFound(format!(
                    "Collection '{}' not found",
                    collection
                ))),
                query::rest::RestError::InvalidDocId(_) => Err(HttpError::BadRequest(format!(
                    "Invalid document ID: {}",
                    doc_id
                ))),
                query::rest::RestError::InvalidInput(msg) => Err(HttpError::BadRequest(msg)),
                query::rest::RestError::PermissionDenied(msg) => Err(HttpError::Forbidden(msg)),
                query::rest::RestError::Internal(msg) => Err(HttpError::Internal(msg)),
                query::rest::RestError::DocumentNotFound(_) => Err(HttpError::NotFound(format!(
                    "Document '{}' not found in collection '{}'",
                    doc_id, collection
                ))),
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
    use serde_json::json;
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
    async fn test_get_document() {
        let state = create_state();
        let result = get_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_ok());
        let doc = result.unwrap();
        assert_eq!(doc.get("_docID").unwrap(), "bae-123");
        assert_eq!(doc.get("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_get_document_not_found() {
        let state = create_state();
        let result = get_document(
            State(state),
            Path(("Users".to_string(), "bae-nonexistent".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("bae-nonexistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_get_document_collection_not_found() {
        let state = create_state();
        let result = get_document(
            State(state),
            Path(("NonExistent".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("NonExistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_get_document_no_rest() {
        let state = create_state_without_rest();
        let result = get_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_single_document() {
        let state = create_state();
        let result = create_document(
            State(state),
            Path("Users".to_string()),
            Json(json!({"name": "Charlie", "age": 35})),
        )
        .await;
        assert!(result.is_ok());
        let Json(doc) = result.unwrap();
        assert!(doc.get("_docID").is_some());
        assert_eq!(doc.get("name").unwrap(), "Charlie");
    }

    #[tokio::test]
    async fn test_create_multiple_documents() {
        let state = create_state();
        let result = create_document(
            State(state),
            Path("Users".to_string()),
            Json(json!([
                {"name": "Dave", "age": 40},
                {"name": "Eve", "age": 28}
            ])),
        )
        .await;
        assert!(result.is_ok());
        let Json(docs) = result.unwrap();
        let docs = docs.as_array().unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[tokio::test]
    async fn test_create_document_collection_not_found() {
        let state = create_state();
        let result = create_document(
            State(state),
            Path("NonExistent".to_string()),
            Json(json!({"name": "Charlie"})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("NonExistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_create_document_no_rest() {
        let state = create_state_without_rest();
        let result = create_document(
            State(state),
            Path("Users".to_string()),
            Json(json!({"name": "Charlie"})),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_document() {
        let state = create_state();
        let result = update_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_ok());
        let doc = result.unwrap();
        assert_eq!(doc.get("_docID").unwrap(), "bae-123");
        assert_eq!(doc.get("name").unwrap(), "Alice");
        assert_eq!(doc.get("age").unwrap(), 31);
    }

    #[tokio::test]
    async fn test_update_document_not_found() {
        let state = create_state();
        let result = update_document(
            State(state),
            Path(("Users".to_string(), "bae-nonexistent".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("bae-nonexistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_update_document_collection_not_found() {
        let state = create_state();
        let result = update_document(
            State(state),
            Path(("NonExistent".to_string(), "bae-123".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("NonExistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_update_document_no_rest() {
        let state = create_state_without_rest();
        let result = update_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_document() {
        let state = create_state();
        let result = delete_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.deleted);
    }

    #[tokio::test]
    async fn test_delete_document_not_found() {
        let state = create_state();
        let result = delete_document(
            State(state),
            Path(("Users".to_string(), "bae-nonexistent".to_string())),
        )
        .await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(!response.deleted);
    }

    #[tokio::test]
    async fn test_delete_document_collection_not_found() {
        let state = create_state();
        let result = delete_document(
            State(state),
            Path(("NonExistent".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::NotFound(msg) => assert!(msg.contains("NonExistent")),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_delete_document_no_rest() {
        let state = create_state_without_rest();
        let result = delete_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_failing_rest_operations() {
        let state = create_failing_state();

        let result = get_document(
            State(state.clone()),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());

        let result = create_document(
            State(state.clone()),
            Path("Users".to_string()),
            Json(json!({"name": "Test"})),
        )
        .await;
        assert!(result.is_err());

        let result = update_document(
            State(state.clone()),
            Path(("Users".to_string(), "bae-123".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_err());

        let result = delete_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Empty array tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_empty_document_array() {
        let state = create_state();
        let result =
            create_document(State(state), Path("Users".to_string()), Json(json!([]))).await;
        assert!(result.is_ok());
        let Json(docs) = result.unwrap();
        assert!(docs.is_array());
        assert!(docs.as_array().unwrap().is_empty());
    }

    // =========================================================================
    // InvalidDocId error path tests
    // =========================================================================

    fn create_invalid_doc_id_state() -> AppState {
        AppState {
            executor: Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>,
            rest: Some(Arc::new(FailingMockRestOperations::with_invalid_doc_id(
                "bad-id",
            ))),
        }
    }

    #[tokio::test]
    async fn test_get_document_invalid_doc_id() {
        let state = create_invalid_doc_id_state();
        let result = get_document(
            State(state),
            Path(("Users".to_string(), "bad-id".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::BadRequest(msg) => assert!(msg.contains("Invalid document ID")),
            other => panic!("Expected BadRequest error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_update_document_invalid_doc_id() {
        let state = create_invalid_doc_id_state();
        let result = update_document(
            State(state),
            Path(("Users".to_string(), "bad-id".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::BadRequest(msg) => assert!(msg.contains("Invalid document ID")),
            other => panic!("Expected BadRequest error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_delete_document_invalid_doc_id() {
        let state = create_invalid_doc_id_state();
        let result = delete_document(
            State(state),
            Path(("Users".to_string(), "bad-id".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::BadRequest(msg) => assert!(msg.contains("Invalid document ID")),
            other => panic!("Expected BadRequest error, got {:?}", other),
        }
    }

    // =========================================================================
    // InvalidInput error path tests
    // =========================================================================

    fn create_invalid_input_state() -> AppState {
        AppState {
            executor: Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>,
            rest: Some(Arc::new(FailingMockRestOperations::with_invalid_input(
                "type mismatch: expected String, got Int",
            ))),
        }
    }

    #[tokio::test]
    async fn test_create_document_invalid_input() {
        let state = create_invalid_input_state();
        let result = create_document(
            State(state),
            Path("Users".to_string()),
            Json(json!({"name": 123})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::BadRequest(msg) => assert!(msg.contains("type mismatch")),
            other => panic!("Expected BadRequest error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_update_document_invalid_input() {
        let state = create_invalid_input_state();
        let result = update_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
            Json(json!({"age": "not-a-number"})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::BadRequest(msg) => assert!(msg.contains("type mismatch")),
            other => panic!("Expected BadRequest error, got {:?}", other),
        }
    }

    // =========================================================================
    // PermissionDenied error path tests
    // =========================================================================

    fn create_permission_denied_state() -> AppState {
        AppState {
            executor: Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>,
            rest: Some(Arc::new(FailingMockRestOperations::with_permission_denied(
                "access denied for user",
            ))),
        }
    }

    #[tokio::test]
    async fn test_get_document_permission_denied() {
        let state = create_permission_denied_state();
        let result = get_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::Forbidden(msg) => assert!(msg.contains("access denied")),
            other => panic!("Expected Forbidden error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_create_document_permission_denied() {
        let state = create_permission_denied_state();
        let result = create_document(
            State(state),
            Path("Users".to_string()),
            Json(json!({"name": "Test"})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::Forbidden(msg) => assert!(msg.contains("access denied")),
            other => panic!("Expected Forbidden error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_update_document_permission_denied() {
        let state = create_permission_denied_state();
        let result = update_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
            Json(json!({"age": 31})),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::Forbidden(msg) => assert!(msg.contains("access denied")),
            other => panic!("Expected Forbidden error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_delete_document_permission_denied() {
        let state = create_permission_denied_state();
        let result = delete_document(
            State(state),
            Path(("Users".to_string(), "bae-123".to_string())),
        )
        .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HttpError::Forbidden(msg) => assert!(msg.contains("access denied")),
            other => panic!("Expected Forbidden error, got {:?}", other),
        }
    }
}
