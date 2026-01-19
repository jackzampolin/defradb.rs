//! Document handler tests.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::json;

use defra_http::mock::{FailingMockRestOperations, MockQueryExecutor, MockRestOperations};
use defra_http::{handlers, AppState, HttpError};
use query::executor::QueryExecutor;
use query::rest::RestOperations;

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
    let result = handlers::get_document(
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
    let result = handlers::get_document(
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
    let result = handlers::get_document(
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
    let result = handlers::get_document(
        State(state),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_single_document() {
    let state = create_state();
    let result = handlers::create_document(
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
    let result = handlers::create_document(
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
    let result = handlers::create_document(
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
    let result = handlers::create_document(
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
    let result = handlers::update_document(
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
    let result = handlers::update_document(
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
    let result = handlers::update_document(
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
    let result = handlers::update_document(
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
    let result = handlers::delete_document(
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
    let result = handlers::delete_document(
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
    let result = handlers::delete_document(
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
    let result = handlers::delete_document(
        State(state),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_failing_rest_operations() {
    let state = create_failing_state();

    let result = handlers::get_document(
        State(state.clone()),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_err());

    let result = handlers::create_document(
        State(state.clone()),
        Path("Users".to_string()),
        Json(json!({"name": "Test"})),
    )
    .await;
    assert!(result.is_err());

    let result = handlers::update_document(
        State(state.clone()),
        Path(("Users".to_string(), "bae-123".to_string())),
        Json(json!({"age": 31})),
    )
    .await;
    assert!(result.is_err());

    let result = handlers::delete_document(
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
        handlers::create_document(State(state), Path("Users".to_string()), Json(json!([]))).await;
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
    let result = handlers::get_document(
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
    let result = handlers::update_document(
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
    let result = handlers::delete_document(
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
    let result = handlers::create_document(
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
    let result = handlers::update_document(
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
    let result = handlers::get_document(
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
    let result = handlers::create_document(
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
    let result = handlers::update_document(
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
    let result = handlers::delete_document(
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
