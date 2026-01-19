//! Mock executor and REST operations tests.

use serde_json::json;

use defra_http::mock::{FailingMockRestOperations, MockQueryExecutor, MockRestOperations};
use query::executor::{QueryExecutor, QueryRequest};
use query::rest::RestOperations;

#[tokio::test]
async fn test_mock_users_query() {
    let executor = MockQueryExecutor::new();
    let request = QueryRequest::new("{ users { name } }");

    let response = executor.execute(request).await;
    assert!(!response.has_errors());

    let data = response.data.unwrap();
    let users = data.get("users").unwrap();
    assert!(users.is_array());
}

#[tokio::test]
async fn test_mock_schema() {
    let executor = MockQueryExecutor::new();
    let schema = executor.schema().await.unwrap();
    assert!(schema.contains("type User"));
    assert!(schema.contains("type Query"));
}

#[tokio::test]
async fn test_mock_introspection() {
    let executor = MockQueryExecutor::new();
    let request = QueryRequest::new("{ __schema { types { name } } }");

    let response = executor.execute(request).await;
    assert!(!response.has_errors());
}

// ========================================================================
// REST Operations tests
// ========================================================================

#[tokio::test]
async fn test_mock_rest_list_collections() {
    let rest = MockRestOperations::new();
    let collections = rest.list_collections().await.unwrap();
    assert!(collections.contains(&"Users".to_string()));
    assert!(collections.contains(&"Books".to_string()));
}

#[tokio::test]
async fn test_mock_rest_get_collection_doc_ids() {
    let rest = MockRestOperations::new();
    let ids = rest.get_collection_doc_ids("Users").await.unwrap();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"bae-123".to_string()));
    assert!(ids.contains(&"bae-456".to_string()));
}

#[tokio::test]
async fn test_mock_rest_get_collection_not_found() {
    let rest = MockRestOperations::new();
    let result = rest.get_collection_doc_ids("NonExistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mock_rest_get_document() {
    let rest = MockRestOperations::new();
    let doc = rest.get_document("Users", "bae-123").await.unwrap();
    assert!(doc.is_some());
    let doc = doc.unwrap();
    assert_eq!(doc.get("_docID").unwrap(), "bae-123");
    assert_eq!(doc.get("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_mock_rest_get_document_not_found() {
    let rest = MockRestOperations::new();
    let doc = rest.get_document("Users", "bae-nonexistent").await.unwrap();
    assert!(doc.is_none());
}

#[tokio::test]
async fn test_mock_rest_create_document() {
    let rest = MockRestOperations::new();
    let doc = rest
        .create_document("Users", json!({"name": "Charlie", "age": 35}))
        .await
        .unwrap();
    assert!(doc.get("_docID").is_some());
    assert_eq!(doc.get("name").unwrap(), "Charlie");
}

#[tokio::test]
async fn test_mock_rest_create_documents() {
    let rest = MockRestOperations::new();
    let docs = rest
        .create_documents(
            "Users",
            vec![
                json!({"name": "Dave", "age": 40}),
                json!({"name": "Eve", "age": 28}),
            ],
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 2);
    assert!(docs[0].get("_docID").is_some());
    assert!(docs[1].get("_docID").is_some());
}

#[tokio::test]
async fn test_mock_rest_update_document() {
    let rest = MockRestOperations::new();
    let doc = rest
        .update_document("Users", "bae-123", json!({"age": 31}))
        .await
        .unwrap();
    assert_eq!(doc.get("_docID").unwrap(), "bae-123");
    assert_eq!(doc.get("name").unwrap(), "Alice");
    assert_eq!(doc.get("age").unwrap(), 31);
}

#[tokio::test]
async fn test_mock_rest_update_document_not_found() {
    let rest = MockRestOperations::new();
    let result = rest
        .update_document("Users", "bae-nonexistent", json!({"age": 31}))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_mock_rest_delete_document() {
    let rest = MockRestOperations::new();
    let deleted = rest.delete_document("Users", "bae-123").await.unwrap();
    assert!(deleted);

    // Verify it's gone
    let doc = rest.get_document("Users", "bae-123").await.unwrap();
    assert!(doc.is_none());
}

#[tokio::test]
async fn test_mock_rest_delete_document_not_found() {
    let rest = MockRestOperations::new();
    let deleted = rest
        .delete_document("Users", "bae-nonexistent")
        .await
        .unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn test_failing_mock_rest() {
    let rest = FailingMockRestOperations::new("test error");
    assert!(rest.list_collections().await.is_err());
    assert!(rest.get_collection_doc_ids("Users").await.is_err());
    assert!(rest.get_document("Users", "bae-123").await.is_err());
    assert!(rest.create_document("Users", json!({})).await.is_err());
    assert!(rest.create_documents("Users", vec![]).await.is_err());
    assert!(rest
        .update_document("Users", "bae-123", json!({}))
        .await
        .is_err());
    assert!(rest.delete_document("Users", "bae-123").await.is_err());
}
