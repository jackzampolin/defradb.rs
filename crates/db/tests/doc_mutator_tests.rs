//! Tests for DbDocMutator struct.

use std::sync::Arc;

use db::database::DB;
use db::doc_mutator::DbDocMutator;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

fn test_schema() -> CollectionVersion {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "name", FieldKind::string()),
        FieldDescription::new("3", "age", FieldKind::int()),
    ];
    CollectionVersion::new("Users", "v1", "col-users", fields)
}

async fn setup_db_with_collection() -> DB<MemoryStore> {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    db.create_collection(test_schema()).await.unwrap();
    db
}

#[tokio::test]
async fn test_create_document() {
    let db = setup_db_with_collection().await;

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

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

    // Commit the transaction
    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Verify the document was persisted
    let txn = db.new_txn(true).await.unwrap();
    let read_mutator = DbDocMutator::new(txn);
    let exists = read_mutator.exists("Users", &result.doc_id).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn test_delete_document() {
    let db = setup_db_with_collection().await;

    // First create a document
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Bob".to_string()));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Now delete it
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let delete_result = mutator.delete("Users", &doc_id).await.unwrap();
    assert!(delete_result.existed);

    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Verify it's gone
    let txn = db.new_txn(true).await.unwrap();
    let mutator = DbDocMutator::new(txn);
    let exists = mutator.exists("Users", &doc_id).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn test_update_document() {
    let db = setup_db_with_collection().await;

    // First create a document
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Charlie".to_string()));
    doc.set("age", NormalValue::Int(25));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Now update it
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut updated_doc = Document::with_id(doc_id.clone());
    updated_doc.set("name", NormalValue::String("Charles".to_string()));
    updated_doc.set("age", NormalValue::Int(26));

    let update_result = mutator.update("Users", updated_doc).await.unwrap();
    assert!(update_result.fields_modified > 0);

    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Verify the update
    let txn = db.new_txn(true).await.unwrap();
    let mutator = DbDocMutator::new(txn);
    let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
    assert!(fetched.is_some());
    assert_eq!(
        fetched.unwrap().get("name").and_then(|v| v.as_str()),
        Some("Charles")
    );
}

#[tokio::test]
async fn test_get_for_update() {
    let db = setup_db_with_collection().await;

    // First create a document
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Diana".to_string()));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Get for update
    let txn = db.new_txn(true).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
    assert!(fetched.is_some());
    assert_eq!(
        fetched.unwrap().get("name").and_then(|v| v.as_str()),
        Some("Diana")
    );
}

#[tokio::test]
async fn test_unknown_collection_returns_error() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    // Don't create any collections

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let doc = Document::new();
    let result = mutator.create("NonExistent", doc).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("collection not found"));
}

#[tokio::test]
async fn test_consumed_transaction_returns_error() {
    let db = setup_db_with_collection().await;

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    // Consume the transaction
    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Now try to use the mutator
    let doc = Document::new();
    let result = mutator.create("Users", doc).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("transaction already consumed"));
}

#[tokio::test]
async fn test_rollback_reverts_mutations() {
    let db = setup_db_with_collection().await;

    // Create a document in a transaction
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("RollbackTest".to_string()));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    // Verify document exists within the transaction
    let exists_in_txn = mutator.exists("Users", &doc_id).await.unwrap();
    assert!(exists_in_txn, "Document should exist within transaction");

    // Drop the transaction without committing (implicit rollback)
    let txn = mutator.take_txn().await.unwrap();
    drop(txn); // Transaction dropped without commit = rollback

    // Verify document does NOT exist after rollback
    let txn = db.new_txn(true).await.unwrap();
    let read_mutator = DbDocMutator::new(txn);
    let exists_after_rollback = read_mutator.exists("Users", &doc_id).await.unwrap();
    assert!(
        !exists_after_rollback,
        "Document should NOT exist after rollback"
    );
}

#[tokio::test]
async fn test_partial_mutation_rollback() {
    let db = setup_db_with_collection().await;

    // Create first document successfully
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut doc1 = Document::new();
    doc1.set("name", NormalValue::String("Doc1".to_string()));
    let result1 = mutator.create("Users", doc1).await.unwrap();
    let doc1_id = result1.doc_id.clone();

    let mut doc2 = Document::new();
    doc2.set("name", NormalValue::String("Doc2".to_string()));
    let result2 = mutator.create("Users", doc2).await.unwrap();
    let doc2_id = result2.doc_id.clone();

    // Verify both documents exist within the transaction
    assert!(mutator.exists("Users", &doc1_id).await.unwrap());
    assert!(mutator.exists("Users", &doc2_id).await.unwrap());

    // Drop without committing (simulating failure scenario)
    drop(mutator);

    // Verify NEITHER document exists after rollback
    let txn = db.new_txn(true).await.unwrap();
    let read_mutator = DbDocMutator::new(txn);
    assert!(
        !read_mutator.exists("Users", &doc1_id).await.unwrap(),
        "Doc1 should not exist after rollback"
    );
    assert!(
        !read_mutator.exists("Users", &doc2_id).await.unwrap(),
        "Doc2 should not exist after rollback"
    );
}

#[tokio::test]
async fn test_concurrent_mutations_are_serialized() {
    let db = setup_db_with_collection().await;

    let txn = db.new_txn(false).await.unwrap();
    let mutator = Arc::new(DbDocMutator::new(txn));

    // Spawn multiple concurrent create operations
    let m1 = mutator.clone();
    let m2 = mutator.clone();
    let m3 = mutator.clone();

    let (r1, r2, r3) = tokio::join!(
        async move {
            let mut doc = Document::new();
            doc.set("name", NormalValue::String("Concurrent1".to_string()));
            m1.create("Users", doc).await
        },
        async move {
            let mut doc = Document::new();
            doc.set("name", NormalValue::String("Concurrent2".to_string()));
            m2.create("Users", doc).await
        },
        async move {
            let mut doc = Document::new();
            doc.set("name", NormalValue::String("Concurrent3".to_string()));
            m3.create("Users", doc).await
        }
    );

    // All operations should succeed
    assert!(r1.is_ok(), "First concurrent create should succeed");
    assert!(r2.is_ok(), "Second concurrent create should succeed");
    assert!(r3.is_ok(), "Third concurrent create should succeed");

    // All documents should have unique IDs
    let doc1_id = r1.unwrap().doc_id;
    let doc2_id = r2.unwrap().doc_id;
    let doc3_id = r3.unwrap().doc_id;

    assert_ne!(doc1_id, doc2_id, "Doc IDs should be unique");
    assert_ne!(doc2_id, doc3_id, "Doc IDs should be unique");
    assert_ne!(doc1_id, doc3_id, "Doc IDs should be unique");

    // Commit and verify all documents exist
    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    let txn = db.new_txn(true).await.unwrap();
    let read_mutator = DbDocMutator::new(txn);
    assert!(read_mutator.exists("Users", &doc1_id).await.unwrap());
    assert!(read_mutator.exists("Users", &doc2_id).await.unwrap());
    assert!(read_mutator.exists("Users", &doc3_id).await.unwrap());
}

#[tokio::test]
async fn test_concurrent_read_write_operations() {
    let db = setup_db_with_collection().await;

    // First create a document
    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(txn);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("ReadWriteTest".to_string()));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();

    // Now test concurrent read and write operations
    let txn = db.new_txn(false).await.unwrap();
    let mutator = Arc::new(DbDocMutator::new(txn));

    let m1 = mutator.clone();
    let m2 = mutator.clone();
    let doc_id_clone = doc_id.clone();

    let (read_result, update_result) = tokio::join!(
        async move { m1.get_for_update("Users", &doc_id_clone).await },
        async move {
            let mut updated_doc = Document::with_id(doc_id.clone());
            updated_doc.set("name", NormalValue::String("UpdatedName".to_string()));
            m2.update("Users", updated_doc).await
        }
    );

    // Both operations should succeed
    assert!(read_result.is_ok(), "Read should succeed");
    assert!(update_result.is_ok(), "Update should succeed");
}
