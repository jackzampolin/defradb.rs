//! Tests for AutoCommitFetcher.

use std::sync::Arc;

use db::auto_commit_fetcher::AutoCommitFetcher;
use db::database::DB;
use document::{Document, NormalValue};
use query::runner::DocFetcher;
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
async fn test_get_all_empty_collection() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let fetcher = AutoCommitFetcher::new(db);
    let docs = fetcher.get_all("Users").await.unwrap();
    assert!(docs.is_empty());
}

#[tokio::test]
async fn test_get_all_with_documents() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    // Insert some documents
    let collection = db.get_collection("Users").unwrap().unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let mut doc1 = Document::new();
    doc1.set("name", NormalValue::String("Alice".to_string()));
    doc1.set("age", NormalValue::Int(30));
    doc1.generate_and_set_doc_id().unwrap();
    collection.create(&txn, &doc1).await.unwrap();

    let mut doc2 = Document::new();
    doc2.set("name", NormalValue::String("Bob".to_string()));
    doc2.set("age", NormalValue::Int(25));
    doc2.generate_and_set_doc_id().unwrap();
    collection.create(&txn, &doc2).await.unwrap();

    txn.commit().await.unwrap();

    // Now fetch all
    let fetcher = AutoCommitFetcher::new(db);
    let docs = fetcher.get_all("Users").await.unwrap();
    assert_eq!(docs.len(), 2);
}

#[tokio::test]
async fn test_get_by_ids_found() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    // Insert a document
    let collection = db.get_collection("Users").unwrap().unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.generate_and_set_doc_id().unwrap();
    let doc_id = doc.id().unwrap().to_string();
    collection.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Fetch by ID
    let fetcher = AutoCommitFetcher::new(db);
    let result = fetcher.get_by_ids("Users", &[doc_id]).await.unwrap();
    assert_eq!(result.docs().len(), 1);
    assert!(result.missing_ids().is_empty());
}

#[tokio::test]
async fn test_get_by_ids_not_found() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let fetcher = AutoCommitFetcher::new(db);
    // Use a valid DocID format (bae-<uuid>) that doesn't exist
    let nonexistent_id = "bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc".to_string();
    let result = fetcher
        .get_by_ids("Users", &[nonexistent_id.clone()])
        .await
        .unwrap();
    assert!(result.docs().is_empty());
    assert_eq!(result.missing_ids().len(), 1);
    assert_eq!(result.missing_ids()[0], nonexistent_id);
}

#[tokio::test]
async fn test_unknown_collection_returns_error() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    let fetcher = AutoCommitFetcher::new(db);
    let result = fetcher.get_all("NonExistent").await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("collection not found"));
}
