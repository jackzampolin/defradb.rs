//! Property-based tests for index operations.
//!
//! These tests use proptest to verify that index operations maintain
//! important invariants like consistency and correct behavior under
//! various inputs.

use db::{IndexManager, DB};
use document::{Document, NormalValue};
use proptest::prelude::*;
use schema::{CollectionVersion, FieldDescription, FieldKind, IndexedFieldDescription};
use storage::backends::MemoryStore;
use storage::index::IndexIterator;

/// Assign a distinct valid DocID for index-layer tests. Index entries are
/// keyed by DocID string; these tests only need identity, not derivation, so
/// the ID comes from a per-process seed sequence instead of the genesis-CID
/// create flow.
fn set_test_doc_id(doc: &mut Document) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    doc.set_id(document::DocID::new_v0_from_seed(&format!("test-doc-{n}")));
}

/// Generate a test schema with common fields.
fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "test_collection",
        "v1",
        "col-test",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "score", FieldKind::float64()),
        ],
    )
}

// ============================================================================
// Property: Index count matches document count after bulk indexing
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn prop_bulk_index_count_matches_documents(
        doc_count in 1usize..20,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();
            let schema = test_schema();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            // Create index
            manager
                .create_index(
                    &datastore,
                    "users",
                    "idx_name".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    false,
                    &[],
                )
                .await
                .unwrap();

            // Create documents
            // IMPORTANT: Set fields BEFORE generating doc_id
            let mut docs = Vec::new();
            for i in 0..doc_count {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String(format!("user_{}", i)));
                set_test_doc_id(&mut doc);
                docs.push(doc);
            }

            // Bulk index
            let result = manager
                .bulk_index(&datastore, "idx_name", &docs, &schema)
                .await
                .unwrap();

            // Property: indexed count should match document count
            assert_eq!(result.indexed, doc_count);
            assert_eq!(result.skipped, 0);
        });
    }
}

// ============================================================================
// Property: Create then delete leaves index empty for that document
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_create_then_delete_removes_from_index(
        name in "[a-zA-Z]{3,10}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();
            let schema = test_schema();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            manager
                .create_index(
                    &datastore,
                    "users",
                    "idx_name".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    false,
                    &[],
                )
                .await
                .unwrap();

            // Create document
            // IMPORTANT: Set fields BEFORE generating doc_id
            let mut doc = Document::new();
            doc.set("name", NormalValue::String(name.clone()));
            set_test_doc_id(&mut doc);

            manager
                .on_document_create(&datastore, &doc, &schema)
                .await
                .unwrap();

            // Verify it's there
            let index = manager.get_index("idx_name").unwrap();
            let mut iter = index
                .get(&datastore, &[NormalValue::String(name.clone())])
                .await
                .unwrap();
            let before = iter.collect_all().await.unwrap();
            assert_eq!(before.len(), 1);

            // Delete document
            manager
                .on_document_delete(&datastore, &doc, &schema)
                .await
                .unwrap();

            // Property: index should be empty for this value
            let mut iter = index
                .get(&datastore, &[NormalValue::String(name)])
                .await
                .unwrap();
            let after = iter.collect_all().await.unwrap();
            assert!(after.is_empty(), "Index should be empty after delete");
        });
    }
}

// ============================================================================
// Property: Update changes index entry correctly
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_update_moves_index_entry(
        old_name in "[a-zA-Z]{3,10}",
        new_name in "[a-zA-Z]{3,10}",
    ) {
        // Skip if names are the same (no update needed)
        prop_assume!(old_name != new_name);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();
            let schema = test_schema();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            manager
                .create_index(
                    &datastore,
                    "users",
                    "idx_name".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    false,
                    &[],
                )
                .await
                .unwrap();

            // Create document with old name
            // IMPORTANT: Set fields BEFORE generating doc_id
            let mut old_doc = Document::new();
            old_doc.set("name", NormalValue::String(old_name.clone()));
            set_test_doc_id(&mut old_doc);
            let doc_id = old_doc.id().unwrap().clone();

            manager
                .on_document_create(&datastore, &old_doc, &schema)
                .await
                .unwrap();

            // Update to new name
            let mut new_doc = Document::with_id(doc_id);
            new_doc.set("name", NormalValue::String(new_name.clone()));

            manager
                .on_document_update(&datastore, &old_doc, &new_doc, &schema)
                .await
                .unwrap();

            // Property 1: Old value should not be in index
            let index = manager.get_index("idx_name").unwrap();
            let mut iter = index
                .get(&datastore, &[NormalValue::String(old_name)])
                .await
                .unwrap();
            let old_results = iter.collect_all().await.unwrap();
            assert!(old_results.is_empty(), "Old value should not be in index");

            // Property 2: New value should be in index
            let mut iter = index
                .get(&datastore, &[NormalValue::String(new_name)])
                .await
                .unwrap();
            let new_results = iter.collect_all().await.unwrap();
            assert_eq!(new_results.len(), 1, "New value should be in index");
        });
    }
}

// ============================================================================
// Property: Non-unique index allows multiple documents with same value
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn prop_non_unique_index_allows_duplicates(
        name in "[a-zA-Z]{3,10}",
        count in 2usize..10,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();
            let schema = test_schema();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            // Non-unique index
            manager
                .create_index(
                    &datastore,
                    "users",
                    "idx_name".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    false, // NOT unique
                    &[],
                )
                .await
                .unwrap();

            // Create multiple documents with same name but different IDs
            // IMPORTANT: Set fields BEFORE generating doc_id
            for i in 0..count {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String(name.clone()));
                // Add unique field to ensure different doc_ids
                doc.set("age", NormalValue::Int(i as i64));
                set_test_doc_id(&mut doc);

                manager
                    .on_document_create(&datastore, &doc, &schema)
                    .await
                    .unwrap();
            }

            // Property: All documents should be indexed
            let index = manager.get_index("idx_name").unwrap();
            let mut iter = index
                .get(&datastore, &[NormalValue::String(name)])
                .await
                .unwrap();
            let results = iter.collect_all().await.unwrap();
            assert_eq!(results.len(), count, "All documents should be in index");
        });
    }
}

// ============================================================================
// Property: Unique index rejects duplicates
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn prop_unique_index_rejects_duplicates(
        name in "[a-zA-Z]{3,10}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();
            let schema = test_schema();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            // Unique index
            manager
                .create_index(
                    &datastore,
                    "users",
                    "idx_name_unique".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    true, // UNIQUE
                    &[],
                )
                .await
                .unwrap();

            // First document should succeed
            // IMPORTANT: Set fields BEFORE generating doc_id
            let mut doc1 = Document::new();
            doc1.set("name", NormalValue::String(name.clone()));
            doc1.set("age", NormalValue::Int(1)); // Unique field for different ID
            set_test_doc_id(&mut doc1);

            let result1 = manager
                .on_document_create(&datastore, &doc1, &schema)
                .await;
            assert!(result1.is_ok(), "First document should succeed");

            // Second document with same name value should fail (unique constraint)
            // IMPORTANT: Set fields BEFORE generating doc_id
            let mut doc2 = Document::new();
            doc2.set("name", NormalValue::String(name.clone()));
            doc2.set("age", NormalValue::Int(2)); // Different age for different ID
            set_test_doc_id(&mut doc2);

            let result2 = manager
                .on_document_create(&datastore, &doc2, &schema)
                .await;
            assert!(result2.is_err(), "Duplicate should be rejected");
        });
    }
}

// ============================================================================
// Property: Index count is always consistent with operations
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn prop_index_count_consistent_with_operations(
        creates in proptest::collection::vec("[a-z]{3,8}", 1..10),
        deletes in 0usize..5,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();
            let schema = test_schema();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            manager
                .create_index(
                    &datastore,
                    "users",
                    "idx_name".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    false,
                    &[],
                )
                .await
                .unwrap();

            // Create all documents
            // IMPORTANT: Set fields BEFORE generating doc_id
            let mut docs = Vec::new();
            for (i, name) in creates.iter().enumerate() {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String(name.clone()));
                // Add unique index to ensure different doc_ids even if names are the same
                doc.set("age", NormalValue::Int(i as i64));
                set_test_doc_id(&mut doc);

                manager
                    .on_document_create(&datastore, &doc, &schema)
                    .await
                    .unwrap();
                docs.push(doc);
            }

            // Delete some documents (up to what we have)
            let actual_deletes = deletes.min(docs.len());
            for doc in docs.iter().take(actual_deletes) {
                manager
                    .on_document_delete(&datastore, doc, &schema)
                    .await
                    .unwrap();
            }

            // Property: Total index entries should equal creates - deletes
            // We need to scan all index entries to count
            let index = manager.get_index("idx_name").unwrap();
            let mut iter = index.scan(&datastore, false).await.unwrap();
            let all_entries = iter.collect_all().await.unwrap();

            let expected_count = creates.len() - actual_deletes;
            assert_eq!(
                all_entries.len(),
                expected_count,
                "Index entry count should match creates - deletes"
            );
        });
    }
}

// ============================================================================
// Property: Empty fields list is rejected
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn prop_empty_fields_rejected(
        name in "[a-zA-Z_]{3,15}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            // Attempt to create index with empty fields
            let result = manager
                .create_index(&datastore, "users", name, vec![], false, &[])
                .await;

            // Property: Empty fields should always be rejected
            assert!(result.is_err(), "Empty fields should be rejected");
        });
    }
}

// ============================================================================
// Property: Duplicate index names are rejected
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]

    #[test]
    fn prop_duplicate_index_names_rejected(
        name in "[a-zA-Z_]{3,15}",
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStore::new();
            let db = DB::new(store).unwrap();
            let txn = db.new_txn(false).await.unwrap();

            let mut manager = IndexManager::new(1);
            let datastore = txn.datastore().unwrap();

            let fields = vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }];

            // First creation should succeed
            let result1 = manager
                .create_index(&datastore, "users", name.clone(), fields.clone(), false, &[])
                .await;
            assert!(result1.is_ok(), "First creation should succeed");

            // Second creation with same name should fail
            let result2 = manager
                .create_index(&datastore, "users", name, fields, false, &[])
                .await;
            assert!(result2.is_err(), "Duplicate name should be rejected");
        });
    }
}
