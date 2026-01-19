//! Tests for AutoCommitMutator struct.

use std::sync::Arc;

use db::auto_commit_mutator::AutoCommitMutator;
use db::database::DB;
use document::{DocID, Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
}

#[tokio::test]
async fn test_create_document() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db.clone());

    // Create a document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));

    let result = mutator.create("Users", doc).await.unwrap();
    assert!(!result.doc_id.to_string().is_empty());
    assert_eq!(
        result.document.get("name").and_then(|v| v.as_str()),
        Some("Alice")
    );

    // Verify document persisted
    let exists = mutator.exists("Users", &result.doc_id).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn test_update_document() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db);

    // First create a document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Bob".to_string()));
    doc.set("age", NormalValue::Int(25));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    // Update the document
    let mut updated_doc = Document::with_id(doc_id.clone());
    updated_doc.set("name", NormalValue::String("Robert".to_string()));
    updated_doc.set("age", NormalValue::Int(26));

    let update_result = mutator.update("Users", updated_doc).await.unwrap();
    assert!(update_result.fields_modified > 0);

    // Verify update
    let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
    assert_eq!(
        fetched.unwrap().get("name").and_then(|v| v.as_str()),
        Some("Robert")
    );
}

#[tokio::test]
async fn test_delete_document() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db);

    // First create a document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Charlie".to_string()));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    // Verify it exists
    assert!(mutator.exists("Users", &doc_id).await.unwrap());

    // Delete it
    let delete_result = mutator.delete("Users", &doc_id).await.unwrap();
    assert!(delete_result.existed);

    // Verify it's gone
    assert!(!mutator.exists("Users", &doc_id).await.unwrap());
}

#[tokio::test]
async fn test_delete_nonexistent_document() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db);

    // Try to delete a document that doesn't exist
    let nonexistent_id =
        DocID::from_string("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").unwrap();
    let delete_result = mutator.delete("Users", &nonexistent_id).await.unwrap();
    assert!(!delete_result.existed);
}

#[tokio::test]
async fn test_get_for_update_nonexistent() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db);

    let nonexistent_id =
        DocID::from_string("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").unwrap();
    let result = mutator
        .get_for_update("Users", &nonexistent_id)
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_unknown_collection_returns_error() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    let mutator = AutoCommitMutator::new(db);
    let doc = Document::new();
    let result = mutator.create("NonExistent", doc).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("collection not found"));
}

#[tokio::test]
async fn test_each_mutation_is_independent() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db);

    // Create first document
    let mut doc1 = Document::new();
    doc1.set("name", NormalValue::String("Doc1".to_string()));
    let result1 = mutator.create("Users", doc1).await.unwrap();

    // Create second document
    let mut doc2 = Document::new();
    doc2.set("name", NormalValue::String("Doc2".to_string()));
    let result2 = mutator.create("Users", doc2).await.unwrap();

    // Both should exist independently
    assert!(mutator.exists("Users", &result1.doc_id).await.unwrap());
    assert!(mutator.exists("Users", &result2.doc_id).await.unwrap());

    // Deleting one doesn't affect the other
    mutator.delete("Users", &result1.doc_id).await.unwrap();
    assert!(!mutator.exists("Users", &result1.doc_id).await.unwrap());
    assert!(mutator.exists("Users", &result2.doc_id).await.unwrap());
}
