//! Router integration tests.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

use defra_http::mock::{MockQueryExecutor, MockRestOperations};
use defra_http::{create_router, create_router_with_rest};

#[tokio::test]
async fn test_health_check_route() {
    let executor = Arc::new(MockQueryExecutor::new());
    let router = create_router(executor);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/health-check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_version_route() {
    let executor = Arc::new(MockQueryExecutor::new());
    let router = create_router(executor);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_collections_route_without_rest() {
    let executor = Arc::new(MockQueryExecutor::new());
    let router = create_router(executor);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 500 because REST operations are not configured
    // (REST uses Internal error, unlike P2P/ACP/Index/Backup which use ServiceUnavailable)
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_collections_route_with_rest() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_document_route() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/Users/bae-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_create_document_route() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/Users")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "Charlie", "age": 35}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_update_document_route() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v0/collections/Users/bae-123")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"age": 31}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_delete_document_route() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/collections/Users/bae-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_document_not_found() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/Users/bae-nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_collection_not_found() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/NonExistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// =========================================================================
// Response body validation tests
// =========================================================================

#[tokio::test]
async fn test_create_document_response_body() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/Users")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "Charlie", "age": 35}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Validate response body structure
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(doc.get("_docID").is_some(), "Response should have _docID");
    assert_eq!(doc.get("name").unwrap(), "Charlie");
    assert_eq!(doc.get("age").unwrap(), 35);
}

#[tokio::test]
async fn test_get_document_response_body() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/Users/bae-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Validate response body structure
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc.get("_docID").unwrap(), "bae-123");
    assert_eq!(doc.get("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_list_collections_response_body() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Validate response body structure
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let collections = data.get("collections").unwrap().as_array().unwrap();
    assert!(collections.iter().any(|c| c == "Users"));
    assert!(collections.iter().any(|c| c == "Books"));
}

// =========================================================================
// Malformed JSON input tests
// =========================================================================

#[tokio::test]
async fn test_create_document_malformed_json() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/Users")
                .header("content-type", "application/json")
                .body(Body::from("{invalid json}"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Axum returns 422 Unprocessable Entity for JSON parse errors
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::UNPROCESSABLE_ENTITY,
        "Expected 400 or 422 for malformed JSON, got {}",
        response.status()
    );
}

#[tokio::test]
async fn test_update_document_malformed_json() {
    let executor = Arc::new(MockQueryExecutor::new());
    let rest = Arc::new(MockRestOperations::new());
    let router = create_router_with_rest(executor, rest);

    let response = router
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v0/collections/Users/bae-123")
                .header("content-type", "application/json")
                .body(Body::from("{not valid json"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Axum returns 422 Unprocessable Entity for JSON parse errors
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::UNPROCESSABLE_ENTITY,
        "Expected 400 or 422 for malformed JSON, got {}",
        response.status()
    );
}
