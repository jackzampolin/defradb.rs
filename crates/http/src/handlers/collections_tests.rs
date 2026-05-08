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
