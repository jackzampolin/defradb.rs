//! Document handler tests.
//!
//! Note: Create, update, and delete handlers now return empty bodies (StatusCode::OK)
//! to match Go DefraDB behavior. These tests verify that operations succeed by
//! checking the status code rather than response content.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::json;

use defra_http::identity_extractor::ExtractIdentity;
use defra_http::mock::{FailingMockRestOperations, MockQueryExecutor, MockRestOperations};
use defra_http::{handlers, AppState, AppStateBuilder, HttpError};
use query::executor::QueryExecutor;
use query::rest::RestOperations;

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

fn anonymous() -> ExtractIdentity {
    ExtractIdentity::anonymous()
}

#[tokio::test]
async fn test_get_document() {
    let state = create_state();
    let result = handlers::get_document(
        State(state),
        anonymous(),
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
        anonymous(),
        Path(("Users".to_string(), "bae-nonexistent".to_string())),
    )
    .await;
    assert!(result.is_err());
    // Go DefraDB returns 400 Bad Request for document not found (combines with permission error)
    match result.unwrap_err() {
        HttpError::BadRequest(msg) => assert!(msg.contains("bae-nonexistent")),
        _ => panic!("Expected BadRequest error (Go-compatible behavior for document not found)"),
    }
}

#[tokio::test]
async fn test_get_document_collection_not_found() {
    let state = create_state();
    let result = handlers::get_document(
        State(state),
        anonymous(),
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
        anonymous(),
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
        anonymous(),
        Path("Users".to_string()),
        Json(json!({"name": "Charlie", "age": 35})),
    )
    .await;
    assert!(result.is_ok());
    // Returns empty body (StatusCode::OK) to match Go DefraDB behavior
    assert_eq!(result.unwrap(), StatusCode::OK);
}

#[tokio::test]
async fn test_create_multiple_documents() {
    let state = create_state();
    let result = handlers::create_document(
        State(state),
        anonymous(),
        Path("Users".to_string()),
        Json(json!([
            {"name": "Dave", "age": 40},
            {"name": "Eve", "age": 28}
        ])),
    )
    .await;
    assert!(result.is_ok());
    // Returns empty body (StatusCode::OK) to match Go DefraDB behavior
    assert_eq!(result.unwrap(), StatusCode::OK);
}

#[tokio::test]
async fn test_create_document_collection_not_found() {
    let state = create_state();
    let result = handlers::create_document(
        State(state),
        anonymous(),
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
        anonymous(),
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
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
        Json(json!({"age": 31})),
    )
    .await;
    assert!(result.is_ok());
    // Returns empty body (StatusCode::OK) to match Go DefraDB behavior
    assert_eq!(result.unwrap(), StatusCode::OK);
}

#[tokio::test]
async fn test_update_document_not_found() {
    let state = create_state();
    let result = handlers::update_document(
        State(state),
        anonymous(),
        Path(("Users".to_string(), "bae-nonexistent".to_string())),
        Json(json!({"age": 31})),
    )
    .await;
    assert!(result.is_err());
    // Go DefraDB returns 400 Bad Request for document not found (combines with permission error)
    match result.unwrap_err() {
        HttpError::BadRequest(msg) => assert!(msg.contains("bae-nonexistent")),
        _ => panic!("Expected BadRequest error (Go-compatible behavior for document not found)"),
    }
}

#[tokio::test]
async fn test_update_document_collection_not_found() {
    let state = create_state();
    let result = handlers::update_document(
        State(state),
        anonymous(),
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
        anonymous(),
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
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_ok());
    // Returns empty body (StatusCode::OK) to match Go DefraDB behavior
    assert_eq!(result.unwrap(), StatusCode::OK);
}

#[tokio::test]
async fn test_delete_document_not_found() {
    let state = create_state();
    let result = handlers::delete_document(
        State(state),
        anonymous(),
        Path(("Users".to_string(), "bae-nonexistent".to_string())),
    )
    .await;
    // Delete of non-existent document still returns OK (Go behavior)
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), StatusCode::OK);
}

#[tokio::test]
async fn test_delete_document_collection_not_found() {
    let state = create_state();
    let result = handlers::delete_document(
        State(state),
        anonymous(),
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
        anonymous(),
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
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_err());

    let result = handlers::create_document(
        State(state.clone()),
        anonymous(),
        Path("Users".to_string()),
        Json(json!({"name": "Test"})),
    )
    .await;
    assert!(result.is_err());

    let result = handlers::update_document(
        State(state.clone()),
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
        Json(json!({"age": 31})),
    )
    .await;
    assert!(result.is_err());

    let result = handlers::delete_document(
        State(state),
        anonymous(),
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
    let result = handlers::create_document(
        State(state),
        anonymous(),
        Path("Users".to_string()),
        Json(json!([])),
    )
    .await;
    assert!(result.is_ok());
    // Returns empty body (StatusCode::OK) to match Go DefraDB behavior
    assert_eq!(result.unwrap(), StatusCode::OK);
}

// =========================================================================
// InvalidDocId error path tests
// =========================================================================

fn create_invalid_doc_id_state() -> AppState {
    AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
        .with_rest(Arc::new(FailingMockRestOperations::with_invalid_doc_id(
            "bad-id",
        )))
        .build()
}

#[tokio::test]
async fn test_get_document_invalid_doc_id() {
    let state = create_invalid_doc_id_state();
    let result = handlers::get_document(
        State(state),
        anonymous(),
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
        anonymous(),
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
        anonymous(),
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
    AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
        .with_rest(Arc::new(FailingMockRestOperations::with_invalid_input(
            "type mismatch: expected String, got Int",
        )))
        .build()
}

#[tokio::test]
async fn test_create_document_invalid_input() {
    let state = create_invalid_input_state();
    let result = handlers::create_document(
        State(state),
        anonymous(),
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
        anonymous(),
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
    AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
        .with_rest(Arc::new(FailingMockRestOperations::with_permission_denied(
            "access denied for user",
        )))
        .build()
}

#[tokio::test]
async fn test_get_document_permission_denied() {
    let state = create_permission_denied_state();
    let result = handlers::get_document(
        State(state),
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        HttpError::Unauthorized(msg) => assert!(msg.contains("access denied")),
        other => panic!("Expected Unauthorized error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_create_document_permission_denied() {
    let state = create_permission_denied_state();
    let result = handlers::create_document(
        State(state),
        anonymous(),
        Path("Users".to_string()),
        Json(json!({"name": "Test"})),
    )
    .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        HttpError::Unauthorized(msg) => assert!(msg.contains("access denied")),
        other => panic!("Expected Unauthorized error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_update_document_permission_denied() {
    let state = create_permission_denied_state();
    let result = handlers::update_document(
        State(state),
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
        Json(json!({"age": 31})),
    )
    .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        HttpError::Unauthorized(msg) => assert!(msg.contains("access denied")),
        other => panic!("Expected Unauthorized error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_delete_document_permission_denied() {
    let state = create_permission_denied_state();
    let result = handlers::delete_document(
        State(state),
        anonymous(),
        Path(("Users".to_string(), "bae-123".to_string())),
    )
    .await;
    assert!(result.is_err());
    match result.unwrap_err() {
        HttpError::Unauthorized(msg) => assert!(msg.contains("access denied")),
        other => panic!("Expected Unauthorized error, got {:?}", other),
    }
}
