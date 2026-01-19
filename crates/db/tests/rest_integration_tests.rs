//! Integration tests for RestOperationsImpl with real storage.
//!
//! These tests verify the full REST CRUD flow using real database components:
//! - AutoCommitFetcher for non-transactional reads
//! - AutoCommitMutator for auto-commit mutations
//! - DbTransactionRegistry for transaction support
//! - QueryRunner to execute GraphQL operations
//! - RestOperationsImpl to translate REST to GraphQL

use std::sync::Arc;

use db::{AutoCommitFetcher, AutoCommitMutator, DbTransactionRegistry, DB};
use query::rest::{RestError, RestOperations, RestOperationsImpl};
use query::runner::QueryRunner;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use serde_json::json;
use storage::backends::MemoryStore;

/// Create a test schema with a Users collection.
fn test_schema() -> Vec<CollectionVersion> {
    vec![CollectionVersion::new(
        "Users",     // name
        "1",         // version_id
        "col-users", // collection_id
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )]
}

/// Create a test database with the Users collection.
async fn test_db() -> Arc<DB<MemoryStore>> {
    let db = Arc::new(DB::new(MemoryStore::new()));
    for schema in test_schema() {
        db.create_collection(schema).await.unwrap();
    }
    db
}

/// Create RestOperationsImpl with real storage components.
async fn create_rest_ops(
) -> RestOperationsImpl<AutoCommitFetcher<MemoryStore>, DbTransactionRegistry<MemoryStore>> {
    let db = test_db().await;
    let fetcher = AutoCommitFetcher::new(Arc::clone(&db));
    let mutator = Arc::new(AutoCommitMutator::new(Arc::clone(&db)));
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // Get collection schemas (matching CLI pattern)
    let collection_names = db.list_collections().unwrap();
    let mut collections: Vec<CollectionVersion> = Vec::new();
    for name in &collection_names {
        if let Ok(Some(c)) = db.get_collection(name) {
            collections.push(c.schema().clone());
        }
    }

    let runner =
        Arc::new(QueryRunner::with_registry(fetcher, collections, registry).with_mutator(mutator));

    RestOperationsImpl::new(runner)
}

#[tokio::test]
async fn test_list_collections() {
    let rest = create_rest_ops().await;
    let collections = rest.list_collections().await.unwrap();
    assert!(collections.contains(&"Users".to_string()));
}

#[tokio::test]
async fn test_get_collection_doc_ids_empty() {
    let rest = create_rest_ops().await;
    let doc_ids = rest.get_collection_doc_ids("Users").await.unwrap();
    assert!(doc_ids.is_empty());
}

#[tokio::test]
async fn test_get_collection_not_found() {
    let rest = create_rest_ops().await;
    let result = rest.get_collection_doc_ids("NonExistent").await;
    assert!(matches!(result, Err(RestError::CollectionNotFound(_))));
}

#[tokio::test]
async fn test_create_and_get_document() {
    let rest = create_rest_ops().await;

    // Create a document
    let created = rest
        .create_document("Users", json!({"name": "Alice", "age": 30}))
        .await
        .unwrap();

    // Verify it has a _docID
    let doc_id = created.get("_docID").unwrap().as_str().unwrap();
    assert!(doc_id.starts_with("bae-"));

    // Get the document
    let fetched = rest.get_document("Users", doc_id).await.unwrap();
    assert!(fetched.is_some());
    let doc = fetched.unwrap();
    assert_eq!(doc.get("name").unwrap(), "Alice");
    assert_eq!(doc.get("age").unwrap(), 30);
}

#[tokio::test]
async fn test_create_and_list_doc_ids() {
    let rest = create_rest_ops().await;

    // Create two documents
    let doc1 = rest
        .create_document("Users", json!({"name": "Alice", "age": 30}))
        .await
        .unwrap();
    let doc2 = rest
        .create_document("Users", json!({"name": "Bob", "age": 25}))
        .await
        .unwrap();

    let doc_id1 = doc1.get("_docID").unwrap().as_str().unwrap();
    let doc_id2 = doc2.get("_docID").unwrap().as_str().unwrap();

    // List doc IDs
    let doc_ids = rest.get_collection_doc_ids("Users").await.unwrap();
    assert_eq!(doc_ids.len(), 2);
    assert!(doc_ids.contains(&doc_id1.to_string()));
    assert!(doc_ids.contains(&doc_id2.to_string()));
}

#[tokio::test]
async fn test_create_multiple_documents() {
    let rest = create_rest_ops().await;

    let docs = rest
        .create_documents(
            "Users",
            vec![
                json!({"name": "Charlie", "age": 35}),
                json!({"name": "Diana", "age": 28}),
            ],
        )
        .await
        .unwrap();

    assert_eq!(docs.len(), 2);
    assert!(docs[0].get("_docID").is_some());
    assert!(docs[1].get("_docID").is_some());
    assert_eq!(docs[0].get("name").unwrap(), "Charlie");
    assert_eq!(docs[1].get("name").unwrap(), "Diana");
}

#[tokio::test]
async fn test_update_document() {
    let rest = create_rest_ops().await;

    // Create a document
    let created = rest
        .create_document("Users", json!({"name": "Eve", "age": 40}))
        .await
        .unwrap();
    let doc_id = created.get("_docID").unwrap().as_str().unwrap();

    // Update the document
    let updated = rest
        .update_document("Users", doc_id, json!({"age": 41}))
        .await
        .unwrap();

    // Verify the update
    assert_eq!(updated.get("name").unwrap(), "Eve");
    assert_eq!(updated.get("age").unwrap(), 41);

    // Fetch and verify
    let fetched = rest.get_document("Users", doc_id).await.unwrap().unwrap();
    assert_eq!(fetched.get("age").unwrap(), 41);
}

#[tokio::test]
async fn test_update_nonexistent_document() {
    let rest = create_rest_ops().await;

    let result = rest
        .update_document(
            "Users",
            "bae-00000000-0000-0000-0000-000000000000",
            json!({"age": 50}),
        )
        .await;

    assert!(matches!(result, Err(RestError::DocumentNotFound(_))));
}

#[tokio::test]
async fn test_delete_document() {
    let rest = create_rest_ops().await;

    // Create a document
    let created = rest
        .create_document("Users", json!({"name": "Frank", "age": 45}))
        .await
        .unwrap();
    let doc_id = created.get("_docID").unwrap().as_str().unwrap();

    // Delete the document
    let deleted = rest.delete_document("Users", doc_id).await.unwrap();
    assert!(deleted);

    // Verify it's gone
    let fetched = rest.get_document("Users", doc_id).await.unwrap();
    assert!(fetched.is_none());
}

#[tokio::test]
async fn test_delete_nonexistent_document() {
    let rest = create_rest_ops().await;

    let deleted = rest
        .delete_document("Users", "bae-00000000-0000-0000-0000-000000000000")
        .await
        .unwrap();

    assert!(!deleted);
}

#[tokio::test]
async fn test_get_nonexistent_document() {
    let rest = create_rest_ops().await;

    let result = rest
        .get_document("Users", "bae-00000000-0000-0000-0000-000000000000")
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_full_crud_lifecycle() {
    let rest = create_rest_ops().await;

    // CREATE
    let created = rest
        .create_document("Users", json!({"name": "Grace", "age": 50}))
        .await
        .unwrap();
    let doc_id = created.get("_docID").unwrap().as_str().unwrap();
    assert_eq!(created.get("name").unwrap(), "Grace");

    // READ
    let read = rest.get_document("Users", doc_id).await.unwrap().unwrap();
    assert_eq!(read.get("name").unwrap(), "Grace");
    assert_eq!(read.get("age").unwrap(), 50);

    // UPDATE
    let updated = rest
        .update_document("Users", doc_id, json!({"age": 51}))
        .await
        .unwrap();
    assert_eq!(updated.get("age").unwrap(), 51);

    // READ again to verify update
    let read_after_update = rest.get_document("Users", doc_id).await.unwrap().unwrap();
    assert_eq!(read_after_update.get("age").unwrap(), 51);

    // DELETE
    let deleted = rest.delete_document("Users", doc_id).await.unwrap();
    assert!(deleted);

    // READ should return None
    let read_after_delete = rest.get_document("Users", doc_id).await.unwrap();
    assert!(read_after_delete.is_none());

    // LIST should be empty
    let doc_ids = rest.get_collection_doc_ids("Users").await.unwrap();
    assert!(!doc_ids.contains(&doc_id.to_string()));
}
