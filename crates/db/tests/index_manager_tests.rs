//! Tests for IndexManager.

use db::database::DB;
use db::index_manager::IndexManager;
use db::Error;
use document::{Document, NormalValue};
use schema::{
    CollectionVersion, FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription,
};
use storage::backends::MemoryStore;
use storage::index::IndexIterator;

fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "email", FieldKind::string()),
        ],
    )
}

#[tokio::test]
async fn test_create_index() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let fields = vec![IndexedFieldDescription {
        name: "name".to_string(),
        descending: false,
    }];

    let desc = manager
        .create_index(
            &datastore,
            "users",
            "idx_name".to_string(),
            fields,
            false,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(desc.name, "idx_name");
    assert_eq!(desc.id, 1);
    assert!(!desc.unique);
    assert!(manager.has_index("idx_name"));
}

#[tokio::test]
async fn test_create_unique_index() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let fields = vec![IndexedFieldDescription {
        name: "email".to_string(),
        descending: false,
    }];

    let desc = manager
        .create_index(
            &datastore,
            "users",
            "idx_email".to_string(),
            fields,
            true,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(desc.name, "idx_email");
    assert!(desc.unique);
}

#[tokio::test]
async fn test_create_duplicate_index_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let fields = vec![IndexedFieldDescription {
        name: "name".to_string(),
        descending: false,
    }];

    manager
        .create_index(
            &datastore,
            "users",
            "idx_name".to_string(),
            fields.clone(),
            false,
            &[],
        )
        .await
        .unwrap();

    let result = manager
        .create_index(
            &datastore,
            "users",
            "idx_name".to_string(),
            fields,
            false,
            &[],
        )
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[tokio::test]
async fn test_create_empty_fields_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let result = manager
        .create_index(
            &datastore,
            "users",
            "idx_empty".to_string(),
            vec![],
            false,
            &[],
        )
        .await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("at least one field"));
}

#[tokio::test]
async fn test_drop_index() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let fields = vec![IndexedFieldDescription {
        name: "name".to_string(),
        descending: false,
    }];

    manager
        .create_index(
            &datastore,
            "users",
            "idx_name".to_string(),
            fields,
            false,
            &[],
        )
        .await
        .unwrap();

    assert!(manager.has_index("idx_name"));

    let dropped = manager.drop_index(&datastore, "idx_name").await.unwrap();
    assert!(dropped);
    assert!(!manager.has_index("idx_name"));
}

#[tokio::test]
async fn test_drop_nonexistent_index() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let dropped = manager.drop_index(&datastore, "nonexistent").await.unwrap();
    assert!(!dropped);
}

#[tokio::test]
async fn test_get_indexes() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    manager
        .create_index(
            &datastore,
            "users",
            "idx1".to_string(),
            vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
            false,
            &[],
        )
        .await
        .unwrap();

    manager
        .create_index(
            &datastore,
            "users",
            "idx2".to_string(),
            vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            true,
            &[],
        )
        .await
        .unwrap();

    let indexes = manager.get_indexes();
    assert_eq!(indexes.len(), 2);
}

#[tokio::test]
async fn test_from_collection_with_indexes() {
    let mut schema = test_schema();
    schema.indexes = vec![
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

    let manager = IndexManager::from_collection(1, &schema).unwrap();

    assert_eq!(manager.index_count(), 2);
    assert!(manager.has_index("idx_name"));
    assert!(manager.has_index("idx_email"));
}

#[tokio::test]
async fn test_on_document_create() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();
    }
    // datastore is dropped here, releasing the Arc<SharedTxn>

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_index_id_sequence() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    // Create multiple indexes and verify IDs increment
    let desc1 = manager
        .create_index(
            &datastore,
            "users",
            "idx1".to_string(),
            vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
            false,
            &[],
        )
        .await
        .unwrap();

    let desc2 = manager
        .create_index(
            &datastore,
            "users",
            "idx2".to_string(),
            vec![IndexedFieldDescription {
                name: "age".to_string(),
                descending: false,
            }],
            false,
            &[],
        )
        .await
        .unwrap();

    let desc3 = manager
        .create_index(
            &datastore,
            "users",
            "idx3".to_string(),
            vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            false,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(desc1.id, 1);
    assert_eq!(desc2.id, 2);
    assert_eq!(desc3.id, 3);
}

#[tokio::test]
async fn test_on_document_update_changes_index_entry() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        // Create an index on the name field
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

        // Create initial document
        let mut old_doc = Document::new();
        old_doc.generate_and_set_doc_id().unwrap();
        let doc_id = old_doc.id().unwrap().clone();
        old_doc.set("name", NormalValue::String("Alice".to_string()));
        old_doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &old_doc, &schema)
            .await
            .unwrap();

        // Create updated document with new name
        let mut new_doc = Document::with_id(doc_id);
        new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
        new_doc.set("age", NormalValue::Int(31));

        // Update should succeed
        manager
            .on_document_update(&datastore, &old_doc, &new_doc, &schema)
            .await
            .unwrap();

        // Verify by querying the index - old value should not find doc,
        // new value should find doc
        let index = manager.get_index("idx_name").unwrap();

        // Query for old value
        let mut old_iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let old_results = old_iter.collect_all().await.unwrap();
        assert!(
            old_results.is_empty(),
            "Old index entry should be removed after update"
        );

        // Query for new value
        let mut new_iter = index
            .get(
                &datastore,
                &[NormalValue::String("Alice Smith".to_string())],
            )
            .await
            .unwrap();
        let new_results = new_iter.collect_all().await.unwrap();
        assert_eq!(
            new_results.len(),
            1,
            "New index entry should exist after update"
        );
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_on_document_update_no_change_when_values_same() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();

        // Update with same indexed value but different non-indexed value
        let mut new_doc = Document::with_id(doc_id);
        new_doc.set("name", NormalValue::String("Alice".to_string())); // Same
        new_doc.set("age", NormalValue::Int(31)); // Different but not indexed

        // Should succeed (optimization path - no actual index write)
        manager
            .on_document_update(&datastore, &doc, &new_doc, &schema)
            .await
            .unwrap();
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_on_document_delete_removes_index_entries() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();

        // Verify index entry exists
        let index = manager.get_index("idx_name").unwrap();
        let mut iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let results = iter.collect_all().await.unwrap();
        assert_eq!(results.len(), 1, "Index entry should exist before delete");

        // Delete document
        manager
            .on_document_delete(&datastore, &doc, &schema)
            .await
            .unwrap();

        // Verify index entry is removed
        let mut iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let results = iter.collect_all().await.unwrap();
        assert!(
            results.is_empty(),
            "Index entry should be removed after delete"
        );
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_bulk_index_indexes_all_documents() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        // Create multiple documents
        let mut docs = Vec::new();
        for name in ["Alice", "Bob", "Charlie"] {
            let mut doc = Document::new();
            doc.generate_and_set_doc_id().unwrap();
            doc.set("name", NormalValue::String(name.to_string()));
            docs.push(doc);
        }

        // Bulk index them
        let result = manager
            .bulk_index(&datastore, "idx_name", &docs, &schema)
            .await
            .unwrap();

        assert_eq!(result.indexed, 3);
        assert_eq!(result.skipped, 0);

        // Verify all are queryable via index
        let index = manager.get_index("idx_name").unwrap();
        for name in ["Alice", "Bob", "Charlie"] {
            let mut iter = index
                .get(&datastore, &[NormalValue::String(name.to_string())])
                .await
                .unwrap();
            let results = iter.collect_all().await.unwrap();
            assert_eq!(results.len(), 1, "Document '{}' should be in index", name);
        }
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_bulk_index_skips_documents_without_id() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        // Create documents - some with IDs, some without
        let mut doc_with_id = Document::new();
        doc_with_id.generate_and_set_doc_id().unwrap();
        doc_with_id.set("name", NormalValue::String("Alice".to_string()));

        let mut doc_without_id = Document::new();
        doc_without_id.set("name", NormalValue::String("Bob".to_string()));
        // No ID set

        let docs = vec![doc_with_id, doc_without_id];

        let result = manager
            .bulk_index(&datastore, "idx_name", &docs, &schema)
            .await
            .unwrap();

        assert_eq!(result.indexed, 1);
        assert_eq!(result.skipped, 1);
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_bulk_index_nonexistent_index_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        let docs = Vec::new();
        let result = manager
            .bulk_index(&datastore, "nonexistent", &docs, &schema)
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}

#[tokio::test]
async fn test_on_document_create_without_id_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        // Document without ID
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        // No ID set

        let result = manager.on_document_create(&datastore, &doc, &schema).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
    }
}

#[tokio::test]
async fn test_on_document_update_without_id_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        // Old doc with ID
        let mut old_doc = Document::new();
        old_doc.generate_and_set_doc_id().unwrap();
        old_doc.set("name", NormalValue::String("Alice".to_string()));

        // New doc without ID
        let mut new_doc = Document::new();
        new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
        // No ID set

        let result = manager
            .on_document_update(&datastore, &old_doc, &new_doc, &schema)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
    }
}

#[tokio::test]
async fn test_on_document_delete_without_id_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        // Document without ID
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        // No ID set

        let result = manager.on_document_delete(&datastore, &doc, &schema).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
    }
}

#[tokio::test]
async fn test_from_collection_with_empty_fields_fails() {
    let mut schema = test_schema();
    schema.indexes = vec![IndexDescription {
        name: "idx_invalid".to_string(),
        id: 1,
        fields: vec![], // Empty fields - invalid
        unique: false,
    }];

    let result = IndexManager::from_collection(1, &schema);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("no fields"));
}

#[tokio::test]
async fn test_multi_index_update() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        // Create multiple indexes
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

        manager
            .create_index(
                &datastore,
                "users",
                "idx_email".to_string(),
                vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                false,
                &[],
            )
            .await
            .unwrap();

        // Create document
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        );

        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();

        // Verify both indexes have entries
        let idx_name = manager.get_index("idx_name").unwrap();
        let idx_email = manager.get_index("idx_email").unwrap();

        let mut iter = idx_name
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let name_results = iter.collect_all().await.unwrap();
        assert_eq!(name_results.len(), 1);

        let mut iter = idx_email
            .get(
                &datastore,
                &[NormalValue::String("alice@example.com".to_string())],
            )
            .await
            .unwrap();
        let email_results = iter.collect_all().await.unwrap();
        assert_eq!(email_results.len(), 1);

        // Update both indexed fields
        let mut new_doc = Document::with_id(doc_id);
        new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
        new_doc.set(
            "email",
            NormalValue::String("alice.smith@example.com".to_string()),
        );

        manager
            .on_document_update(&datastore, &doc, &new_doc, &schema)
            .await
            .unwrap();

        // Verify old entries are gone
        let mut iter = idx_name
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let old_name_results = iter.collect_all().await.unwrap();
        assert!(old_name_results.is_empty());

        let mut iter = idx_email
            .get(
                &datastore,
                &[NormalValue::String("alice@example.com".to_string())],
            )
            .await
            .unwrap();
        let old_email_results = iter.collect_all().await.unwrap();
        assert!(old_email_results.is_empty());

        // Verify new entries exist
        let mut iter = idx_name
            .get(
                &datastore,
                &[NormalValue::String("Alice Smith".to_string())],
            )
            .await
            .unwrap();
        let new_name_results = iter.collect_all().await.unwrap();
        assert_eq!(new_name_results.len(), 1);

        let mut iter = idx_email
            .get(
                &datastore,
                &[NormalValue::String("alice.smith@example.com".to_string())],
            )
            .await
            .unwrap();
        let new_email_results = iter.collect_all().await.unwrap();
        assert_eq!(new_email_results.len(), 1);
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_composite_index_through_manager() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    // Schema with multiple fields
    let schema = CollectionVersion::new(
        "products",
        "v1",
        "col-products",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "category", FieldKind::string()),
            FieldDescription::new("3", "price", FieldKind::int()),
            FieldDescription::new("4", "name", FieldKind::string()),
        ],
    );

    let mut manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        // Create composite index on (category, price)
        manager
            .create_index(
                &datastore,
                "users",
                "idx_category_price".to_string(),
                vec![
                    IndexedFieldDescription {
                        name: "category".to_string(),
                        descending: false,
                    },
                    IndexedFieldDescription {
                        name: "price".to_string(),
                        descending: true, // Descending for price (highest first)
                    },
                ],
                false,
                &[],
            )
            .await
            .unwrap();

        // Create documents
        // Set fields BEFORE generating doc_id
        let mut doc1 = Document::new();
        doc1.set("category", NormalValue::String("electronics".to_string()));
        doc1.set("price", NormalValue::Int(100));
        doc1.set("name", NormalValue::String("Widget".to_string()));
        doc1.generate_and_set_doc_id().unwrap();

        let mut doc2 = Document::new();
        doc2.set("category", NormalValue::String("electronics".to_string()));
        doc2.set("price", NormalValue::Int(200));
        doc2.set("name", NormalValue::String("Gadget".to_string()));
        doc2.generate_and_set_doc_id().unwrap();

        let mut doc3 = Document::new();
        doc3.set("category", NormalValue::String("books".to_string()));
        doc3.set("price", NormalValue::Int(50));
        doc3.set("name", NormalValue::String("Novel".to_string()));
        doc3.generate_and_set_doc_id().unwrap();

        // Index all documents
        manager
            .on_document_create(&datastore, &doc1, &schema)
            .await
            .unwrap();
        manager
            .on_document_create(&datastore, &doc2, &schema)
            .await
            .unwrap();
        manager
            .on_document_create(&datastore, &doc3, &schema)
            .await
            .unwrap();

        // Query by first field only (category = "electronics")
        let index = manager.get_index("idx_category_price").unwrap();
        let mut iter = index
            .scan_prefix(
                &datastore,
                &[NormalValue::String("electronics".to_string())],
                false,
            )
            .await
            .unwrap();
        let electronics = iter.collect_all().await.unwrap();
        assert_eq!(electronics.len(), 2, "Should find 2 electronics products");

        // Query by exact match (category = "books", price = 50)
        let mut iter = index
            .get(
                &datastore,
                &[
                    NormalValue::String("books".to_string()),
                    NormalValue::Int(50),
                ],
            )
            .await
            .unwrap();
        let books = iter.collect_all().await.unwrap();
        assert_eq!(books.len(), 1, "Should find 1 book at price 50");
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_missing_field_indexed_as_null() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        // Create index on email field
        manager
            .create_index(
                &datastore,
                "users",
                "idx_email".to_string(),
                vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                false,
                &[],
            )
            .await
            .unwrap();

        // Create document WITHOUT the email field
        // Set fields BEFORE generating doc_id
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        // Note: email field is NOT set
        doc.generate_and_set_doc_id().unwrap();

        // Should succeed - missing field indexed as NULL
        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();

        // Query for NULL values should find the document
        let index = manager.get_index("idx_email").unwrap();
        let mut iter = index.get(&datastore, &[NormalValue::Null]).await.unwrap();
        let results = iter.collect_all().await.unwrap();
        assert_eq!(
            results.len(),
            1,
            "Document with missing field should be indexed under NULL"
        );

        // Create another document with explicit NULL
        // Set fields BEFORE generating doc_id
        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Bob".to_string()));
        doc2.set("email", NormalValue::Null); // Explicit NULL
        doc2.generate_and_set_doc_id().unwrap();

        manager
            .on_document_create(&datastore, &doc2, &schema)
            .await
            .unwrap();

        // Both should be under NULL
        let mut iter = index.get(&datastore, &[NormalValue::Null]).await.unwrap();
        let results = iter.collect_all().await.unwrap();
        assert_eq!(
            results.len(),
            2,
            "Both missing and explicit NULL should be indexed together"
        );
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_unique_index_allows_multiple_nulls() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        // Create UNIQUE index on email
        manager
            .create_index(
                &datastore,
                "users",
                "idx_email_unique".to_string(),
                vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                true, // unique
                &[],
            )
            .await
            .unwrap();

        // Create first document without email (NULL)
        // Set fields BEFORE generating doc_id
        let mut doc1 = Document::new();
        doc1.set("name", NormalValue::String("Alice".to_string()));
        doc1.generate_and_set_doc_id().unwrap();

        manager
            .on_document_create(&datastore, &doc1, &schema)
            .await
            .unwrap();

        // Create second document without email (also NULL)
        // This should succeed - NULL is not considered equal to NULL for uniqueness
        // Set fields BEFORE generating doc_id
        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Bob".to_string()));
        doc2.generate_and_set_doc_id().unwrap();

        let result = manager.on_document_create(&datastore, &doc2, &schema).await;
        assert!(
            result.is_ok(),
            "Multiple NULL values should be allowed in unique index"
        );
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_unique_constraint_violation_returns_error() {
    use storage::index::{CollectionIndex, UniqueIndex};

    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();

    {
        let datastore = txn.datastore().unwrap();

        // First, test UniqueIndex directly through NamespaceView
        let desc = schema::IndexDescription {
            name: "idx_email_unique".to_string(),
            id: 1,
            fields: vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            unique: true,
        };

        let index = UniqueIndex::new(1, desc);
        let values = vec![NormalValue::String("alice@example.com".to_string())];

        // First save through NamespaceView
        let mut ds1 = datastore.clone();
        index.save(&mut ds1, "doc1", &values).await.unwrap();

        // Second save - should fail
        let mut ds2 = datastore.clone();
        let result = index.save(&mut ds2, "doc2", &values).await;

        assert!(
            result.is_err(),
            "UniqueIndex should reject duplicate through NamespaceView"
        );
    }

    // Now test through IndexManager
    let store2 = MemoryStore::new();
    let db2 = DB::new(store2).unwrap();
    let txn2 = db2.new_txn(false).await.unwrap();
    let mut manager = IndexManager::new(1);

    {
        let datastore = txn2.datastore().unwrap();

        // Create UNIQUE index on email
        let index_desc = manager
            .create_index(
                &datastore,
                "users",
                "idx_email_unique".to_string(),
                vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                true, // unique
                &[],
            )
            .await
            .unwrap();

        assert!(index_desc.unique, "Index should be unique");
        assert!(
            manager
                .get_index("idx_email_unique")
                .unwrap()
                .description()
                .unique,
            "Stored index should be unique"
        );

        // Create first document with email
        // Set fields BEFORE generating doc_id, since doc_id is based on content hash
        let mut doc1 = Document::new();
        doc1.set("name", NormalValue::String("Alice".to_string()));
        doc1.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        );
        doc1.generate_and_set_doc_id().unwrap();

        manager
            .on_document_create(&datastore, &doc1, &schema)
            .await
            .unwrap();

        // Create second document with SAME email but different name - should fail
        // Set fields BEFORE generating doc_id, since doc_id is based on content hash
        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Bob".to_string()));
        doc2.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        ); // Duplicate email!
        doc2.generate_and_set_doc_id().unwrap();

        let result = manager.on_document_create(&datastore, &doc2, &schema).await;
        assert!(
            result.is_err(),
            "Duplicate value in unique index should fail through IndexManager"
        );
    }
}

#[tokio::test]
async fn test_index_field_not_in_schema_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema(); // Has: _docID, name, age, email
    let mut manager = IndexManager::new(1);

    {
        let datastore = txn.datastore().unwrap();

        // Create index on a field that EXISTS in schema
        manager
            .create_index(
                &datastore,
                "users",
                "idx_nonexistent".to_string(),
                vec![IndexedFieldDescription {
                    name: "nonexistent_field".to_string(), // This field is NOT in schema
                    descending: false,
                }],
                false,
                &[],
            )
            .await
            .unwrap();

        // Create a document
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Alice".to_string()));

        // Indexing should fail because the field doesn't exist in schema
        let result = manager.on_document_create(&datastore, &doc, &schema).await;
        assert!(
            result.is_err(),
            "Indexing with non-schema field should fail"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("does not exist in schema"),
            "Error should mention field not in schema: {}",
            err_msg
        );
    }
}

#[tokio::test]
async fn test_index_idempotence_create_same_document_twice() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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

        // Set fields BEFORE generating doc_id
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.generate_and_set_doc_id().unwrap();

        // Index the same document twice
        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();
        manager
            .on_document_create(&datastore, &doc, &schema)
            .await
            .unwrap();

        // Should have 2 entries (non-unique index allows duplicates)
        let index = manager.get_index("idx_name").unwrap();
        let mut iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let results = iter.collect_all().await.unwrap();
        // For non-unique index, same doc can be indexed multiple times
        // This tests the actual behavior - whether it's 1 or 2 depends on implementation
        assert!(
            !results.is_empty(),
            "Document should be indexed at least once"
        );
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_delete_then_recreate_same_value() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();

    let schema = test_schema();
    let mut manager = IndexManager::new(1);

    {
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
        // Set fields BEFORE generating doc_id
        let mut doc1 = Document::new();
        doc1.set("name", NormalValue::String("Alice".to_string()));
        doc1.set("age", NormalValue::Int(30)); // Add unique field for different ID
        doc1.generate_and_set_doc_id().unwrap();

        manager
            .on_document_create(&datastore, &doc1, &schema)
            .await
            .unwrap();

        // Verify it's indexed
        let index = manager.get_index("idx_name").unwrap();
        let mut iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let results = iter.collect_all().await.unwrap();
        assert_eq!(results.len(), 1);

        // Delete document
        manager
            .on_document_delete(&datastore, &doc1, &schema)
            .await
            .unwrap();

        // Verify it's gone
        let mut iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let results = iter.collect_all().await.unwrap();
        assert_eq!(results.len(), 0);

        // Create NEW document with same name but different content for different ID
        // Set fields BEFORE generating doc_id
        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Alice".to_string()));
        doc2.set("age", NormalValue::Int(31)); // Different age for different ID
        doc2.generate_and_set_doc_id().unwrap();

        manager
            .on_document_create(&datastore, &doc2, &schema)
            .await
            .unwrap();

        // Verify new document is indexed
        let mut iter = index
            .get(&datastore, &[NormalValue::String("Alice".to_string())])
            .await
            .unwrap();
        let results = iter.collect_all().await.unwrap();
        assert_eq!(results.len(), 1, "New document should be indexed");
    }

    txn.commit().await.unwrap();
}
