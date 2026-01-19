//! Tests for the Collection struct.

use db::collection::Collection;
use db::database::DB;
use db::index_manager::IndexManager;
use document::{Document, NormalValue};
use schema::{CollectionVersion, FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription};
use storage::backends::MemoryStore;

fn test_collection() -> Collection {
    Collection::new(CollectionVersion::new("users", "v1", "col-1", vec![]))
}

/// Create a typed collection with schema fields for validation tests.
fn typed_collection() -> Collection {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "name", FieldKind::string()),
        FieldDescription::new("3", "age", FieldKind::int()),
        FieldDescription::new("4", "active", FieldKind::bool()),
    ];
    Collection::new(CollectionVersion::new(
        "typed_users",
        "v1",
        "col-typed",
        fields,
    ))
}

#[tokio::test]
async fn test_collection_name() {
    let col = test_collection();
    assert_eq!(col.name(), "users");
    assert_eq!(col.collection_id(), "col-1");
}

#[tokio::test]
async fn test_collection_create_get() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create a document
    let doc = Document::from_json_str(r#"{"name": "Alice", "age": 30}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();

    col.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Read it back
    let txn = db.new_txn(true).await.unwrap();
    let retrieved = col.get(&txn, &doc_id).await.unwrap();
    assert!(retrieved.is_some());

    let retrieved_doc = retrieved.unwrap();
    assert_eq!(
        retrieved_doc.get("name").and_then(|v| v.as_str()),
        Some("Alice")
    );
}

#[tokio::test]
async fn test_collection_delete() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create
    let txn = db.new_txn(false).await.unwrap();
    let doc = Document::from_json_str(r#"{"name": "Alice"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();
    col.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Verify exists
    let txn = db.new_txn(true).await.unwrap();
    assert!(col.exists(&txn, &doc_id).await.unwrap());

    // Delete
    let txn = db.new_txn(false).await.unwrap();
    let deleted = col.delete(&txn, &doc_id).await.unwrap();
    assert!(deleted);
    txn.commit().await.unwrap();

    // Verify gone
    let txn = db.new_txn(true).await.unwrap();
    assert!(!col.exists(&txn, &doc_id).await.unwrap());
}

#[tokio::test]
async fn test_collection_exists_nonexistent() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create a document to get a valid DocID format, then check for non-existent
    let doc = Document::from_json_str(r#"{"name": "Test"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();

    let txn = db.new_txn(true).await.unwrap();
    // Document was never saved, so it shouldn't exist
    assert!(!col.exists(&txn, &doc_id).await.unwrap());
}

#[tokio::test]
async fn test_collection_save_upsert() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Save (create)
    let txn = db.new_txn(false).await.unwrap();
    let doc = Document::from_json_str(r#"{"name": "Bob"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();
    col.save(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Save again (update) - keep the same doc_id
    let txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::with_id(doc_id.clone());
    doc.set("name", NormalValue::String("Robert".to_string()));
    col.save(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Verify
    let txn = db.new_txn(true).await.unwrap();
    let retrieved = col.get(&txn, &doc_id).await.unwrap().unwrap();
    assert_eq!(
        retrieved.get("name").and_then(|v| v.as_str()),
        Some("Robert")
    );
}

// Edge case tests

#[tokio::test]
async fn test_collection_create_duplicate_returns_error() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create a document
    let txn = db.new_txn(false).await.unwrap();
    let doc = Document::from_json_str(r#"{"name": "Alice"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();
    col.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Try to create the same document again
    let txn = db.new_txn(false).await.unwrap();
    let doc2 = Document::with_id(doc_id);
    let result = col.create(&txn, &doc2).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), db::Error::InvalidDocument(_)));
}

#[tokio::test]
async fn test_collection_update_nonexistent_returns_error() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create a document to get a valid DocID, but don't save it
    let doc = Document::from_json_str(r#"{"name": "Ghost"}"#).unwrap();
    doc.generate_doc_id().unwrap();

    // Try to update a non-existent document
    let txn = db.new_txn(false).await.unwrap();
    let result = col.update(&txn, &doc).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), db::Error::DocumentNotFound(_)));
}

#[tokio::test]
async fn test_collection_delete_nonexistent_returns_false() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create a document to get a valid DocID, but don't save it
    let doc = Document::from_json_str(r#"{"name": "Ghost"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();

    // Delete should return false for non-existent
    let txn = db.new_txn(false).await.unwrap();
    let deleted = col.delete(&txn, &doc_id).await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn test_collection_get_nonexistent_returns_none() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create a document to get a valid DocID, but don't save it
    let doc = Document::from_json_str(r#"{"name": "Ghost"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();

    // Get should return None for non-existent
    let txn = db.new_txn(true).await.unwrap();
    let result = col.get(&txn, &doc_id).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_collection_get_all_empty() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Get all from empty collection
    let txn = db.new_txn(true).await.unwrap();
    let docs = col.get_all(&txn).await.unwrap();
    assert!(docs.is_empty());
}

#[tokio::test]
async fn test_collection_get_all_multiple() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create multiple documents
    let txn = db.new_txn(false).await.unwrap();
    for i in 0..5 {
        let doc =
            Document::from_json_str(&format!(r#"{{"name": "User{}", "index": {}}}"#, i, i))
                .unwrap();
        doc.generate_doc_id().unwrap();
        col.create(&txn, &doc).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Get all should return all 5
    let txn = db.new_txn(true).await.unwrap();
    let docs = col.get_all(&txn).await.unwrap();
    assert_eq!(docs.len(), 5);
}

#[tokio::test]
async fn test_collection_create_without_id_returns_error() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = test_collection();

    // Create a document without an ID using Document::new()
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("NoID".to_string()));
    // Don't set an ID

    let txn = db.new_txn(false).await.unwrap();
    let result = col.create(&txn, &doc).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), db::Error::InvalidDocument(_)));
}

#[tokio::test]
async fn test_collection_isolation_between_collections() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    // Create two different collections
    let col1 = Collection::new(CollectionVersion::new("users", "v1", "col-users", vec![]));
    let col2 = Collection::new(CollectionVersion::new("posts", "v1", "col-posts", vec![]));

    // Create document in col1
    let txn = db.new_txn(false).await.unwrap();
    let doc = Document::from_json_str(r#"{"name": "Alice"}"#).unwrap();
    doc.generate_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();
    col1.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Document should exist in col1 but not col2
    let txn = db.new_txn(true).await.unwrap();
    assert!(col1.exists(&txn, &doc_id).await.unwrap());
    assert!(!col2.exists(&txn, &doc_id).await.unwrap());
}

// Schema validation tests

#[tokio::test]
async fn test_validation_correct_types_passes() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create document with correct types
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));
    doc.set("active", NormalValue::Bool(true));
    doc.generate_and_set_doc_id().unwrap();

    // Should succeed
    let result = col.create(&txn, &doc).await;
    assert!(result.is_ok(), "Expected success but got: {:?}", result);
}

#[tokio::test]
async fn test_validation_wrong_string_type_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create document with wrong type for "name" (int instead of string)
    let mut doc = Document::new();
    doc.set("name", NormalValue::Int(123)); // Wrong type!
    doc.set("age", NormalValue::Int(30));
    doc.generate_and_set_doc_id().unwrap();

    let result = col.create(&txn, &doc).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, db::Error::InvalidDocument(_)));
    assert!(err.to_string().contains("name"));
}

#[tokio::test]
async fn test_validation_wrong_int_type_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create document with wrong type for "age" (string instead of int)
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::String("thirty".to_string())); // Wrong type!
    doc.generate_and_set_doc_id().unwrap();

    let result = col.create(&txn, &doc).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, db::Error::InvalidDocument(_)));
    assert!(err.to_string().contains("age"));
}

#[tokio::test]
async fn test_validation_null_values_allowed() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create document with null values (allowed in DefraDB)
    let mut doc = Document::new();
    doc.set("name", NormalValue::Null);
    doc.set("age", NormalValue::Null);
    doc.generate_and_set_doc_id().unwrap();

    // Should succeed - null is allowed for any field
    let result = col.create(&txn, &doc).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validation_missing_fields_allowed() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create document with missing fields (only has name)
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    // age and active are missing - should be allowed
    doc.generate_and_set_doc_id().unwrap();

    let result = col.create(&txn, &doc).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validation_update_validates() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    // First create a valid document
    let txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));
    doc.generate_and_set_doc_id().unwrap();
    let doc_id = doc.id().unwrap().clone();
    col.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Now try to update with invalid type
    let txn = db.new_txn(false).await.unwrap();
    let mut invalid_doc = Document::with_id(doc_id);
    invalid_doc.set("name", NormalValue::Int(999)); // Wrong type!

    let result = col.update(&txn, &invalid_doc).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), db::Error::InvalidDocument(_)));
}

#[tokio::test]
async fn test_validation_save_validates() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Try to save with invalid type
    let mut doc = Document::new();
    doc.set("name", NormalValue::Bool(true)); // Wrong type for string field!
    doc.generate_and_set_doc_id().unwrap();

    let result = col.save(&txn, &doc).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), db::Error::InvalidDocument(_)));
}

#[tokio::test]
async fn test_validation_extra_fields_allowed() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = typed_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Create document with extra fields not in schema
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));
    doc.set("extra_field", NormalValue::String("extra".to_string())); // Not in schema
    doc.generate_and_set_doc_id().unwrap();

    // Should succeed - extra fields are allowed for flexibility
    let result = col.create(&txn, &doc).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validation_schemaless_collection_accepts_any() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    // Empty schema - no validation
    let col = test_collection();

    let txn = db.new_txn(false).await.unwrap();

    // Any document structure should be accepted
    let mut doc = Document::new();
    doc.set("anything", NormalValue::Int(123));
    doc.set("goes", NormalValue::String("here".to_string()));
    doc.set("mixed", NormalValue::Bool(false));
    doc.generate_and_set_doc_id().unwrap();

    let result = col.create(&txn, &doc).await;
    assert!(result.is_ok());
}

// =========================================================================
// Index Tests
// =========================================================================

fn collection_with_indexes() -> Collection {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "name", FieldKind::string()),
        FieldDescription::new("3", "age", FieldKind::int()),
        FieldDescription::new("4", "email", FieldKind::string()),
    ];
    let mut cv = CollectionVersion::new("users_indexed", "v1", "col-indexed", fields);
    cv.indexes = vec![
        IndexDescription {
            name: "idx_name".to_string(),
            id: 1,
            fields: vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
            unique: false,
        },
        IndexDescription {
            name: "idx_email".to_string(),
            id: 2,
            fields: vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            unique: true,
        },
    ];
    Collection::new(cv)
}

#[tokio::test]
async fn test_collection_get_indexes() {
    let col = collection_with_indexes();
    let indexes = col.get_indexes();
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0].name, "idx_name");
    assert_eq!(indexes[1].name, "idx_email");
}

#[tokio::test]
async fn test_collection_has_index() {
    let col = collection_with_indexes();
    assert!(col.has_index("idx_name"));
    assert!(col.has_index("idx_email"));
    assert!(!col.has_index("nonexistent"));
}

#[tokio::test]
async fn test_collection_get_index() {
    let col = collection_with_indexes();

    let idx = col.get_index("idx_name").unwrap();
    assert_eq!(idx.name, "idx_name");
    assert!(!idx.unique);

    let idx = col.get_index("idx_email").unwrap();
    assert_eq!(idx.name, "idx_email");
    assert!(idx.unique);

    assert!(col.get_index("nonexistent").is_none());
}

#[tokio::test]
async fn test_create_with_indexes() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = collection_with_indexes();
    let txn = db.new_txn(false).await.unwrap();

    // Create an IndexManager from the collection
    let index_manager = IndexManager::from_collection(1, col.schema()).unwrap();

    {
        let datastore = txn.datastore().unwrap();

        // Create a document with index maintenance
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));
        doc.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        );

        let doc_id = col
            .create_with_indexes(&datastore, &doc, &index_manager)
            .await
            .unwrap();

        // Verify document was created
        let retrieved = col.get_with_datastore(&datastore, &doc_id).await.unwrap();
        assert!(retrieved.is_some());
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_update_with_indexes() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = collection_with_indexes();
    let txn = db.new_txn(false).await.unwrap();

    let index_manager = IndexManager::from_collection(1, col.schema()).unwrap();

    {
        let datastore = txn.datastore().unwrap();

        // Create a document
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));
        doc.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        );

        col.create_with_indexes(&datastore, &doc, &index_manager)
            .await
            .unwrap();

        // Update the document
        let mut updated_doc = Document::with_id(doc_id.clone());
        updated_doc.set("name", NormalValue::String("Alice Smith".to_string()));
        updated_doc.set("age", NormalValue::Int(31));
        updated_doc.set(
            "email",
            NormalValue::String("alice.smith@example.com".to_string()),
        );

        col.update_with_indexes(&datastore, &updated_doc, &index_manager)
            .await
            .unwrap();

        // Verify update
        let retrieved = col.get_with_datastore(&datastore, &doc_id).await.unwrap();
        let retrieved_doc = retrieved.unwrap();
        assert_eq!(
            retrieved_doc.get("name").and_then(|v| v.as_str()),
            Some("Alice Smith")
        );
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_delete_with_indexes() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = collection_with_indexes();
    let txn = db.new_txn(false).await.unwrap();

    let index_manager = IndexManager::from_collection(1, col.schema()).unwrap();

    {
        let datastore = txn.datastore().unwrap();

        // Create a document
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));
        doc.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        );

        col.create_with_indexes(&datastore, &doc, &index_manager)
            .await
            .unwrap();

        // Verify exists
        assert!(col
            .exists_with_datastore(&datastore, &doc_id)
            .await
            .unwrap());

        // Delete with index cleanup
        let deleted = col
            .delete_with_indexes(&datastore, &doc_id, &index_manager)
            .await
            .unwrap();
        assert!(deleted);

        // Verify deleted
        assert!(!col
            .exists_with_datastore(&datastore, &doc_id)
            .await
            .unwrap());
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_delete_with_indexes_nonexistent() {
    let store = MemoryStore::new();
    let db = DB::new(store);
    let col = collection_with_indexes();
    let txn = db.new_txn(false).await.unwrap();

    let index_manager = IndexManager::from_collection(1, col.schema()).unwrap();

    {
        let datastore = txn.datastore().unwrap();

        // Create a document just to get a valid doc ID
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        // Don't actually create the document

        // Delete should return false for non-existent
        let deleted = col
            .delete_with_indexes(&datastore, &doc_id, &index_manager)
            .await
            .unwrap();
        assert!(!deleted);
    }
}
