use super::*;
use crate::identity_extractor::ExtractIdentity;
use crate::mock::{
    FailingMockRestOperations, MockCollectionManagementOperations, MockQueryExecutor,
    MockRestOperations,
};
use crate::router::{AppStateBuilder, CollectionManagementOperations};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use query::executor::QueryExecutor;
use query::rest::RestOperations;
use std::sync::Arc;
use tower::ServiceExt;

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

fn router_with_collection_mgmt() -> axum::Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
        .with_collection_mgmt(Arc::new(MockCollectionManagementOperations::new())
            as Arc<dyn CollectionManagementOperations>)
        .build();
    crate::router::create_router_with_state(state)
}

#[tokio::test]
async fn delete_collections_by_query_returns_ok_for_single_name() {
    let router = router_with_collection_mgmt();
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/v0/collections?name=Users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn delete_collections_by_query_returns_ok_for_csv_names() {
    let router = router_with_collection_mgmt();
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/v0/collections?name=Users,Books&active-only=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn delete_collections_by_query_rejects_missing_name_param() {
    let router = router_with_collection_mgmt();
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/v0/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn delete_collections_by_query_rejects_invalid_active_only() {
    let router = router_with_collection_mgmt();
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/v0/collections?name=Users&active-only=not-a-bool")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
