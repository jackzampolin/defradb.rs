//! Tests for IndexManager.

use crate::common::fixture::next_test_doc_short_id;
use db::database::DB;
use db::index::{Error, IndexManager};
use document::{Document, NormalValue};
use schema::{
    CollectionVersion, FieldDescription, FieldKind, FullTextIndexDescription, IndexDescription,
    IndexedFieldDescription,
};
use storage::index::IndexIterator;
use storage::RegolithStore;

/// Allocate a distinct doc short ID for index-layer tests. Index entries are
/// keyed by node-local short IDs; these tests only need identity, not the
/// full allocation/mapping flow of the create path.
const RESERVED_FULLTEXT_INDEX_NAME_FOR_NAME: &str = "__fulltext__:name";

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
    let store = RegolithStore::in_memory().unwrap();
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
    let store = RegolithStore::in_memory().unwrap();
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
async fn unique_json_index_rejects_duplicate_array_values() {
    let store = RegolithStore::in_memory().unwrap();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut schema = CollectionVersion::new(
        "users",
        "v1",
        "col-users",
        vec![FieldDescription::new("1", "custom", FieldKind::json())],
    );
    schema.indexes = vec![IndexDescription {
        name: "idx_custom".to_string(),
        id: 1,
        fields: vec![IndexedFieldDescription {
            name: "custom".to_string(),
            descending: false,
        }],
        unique: true,
        kind: None,
        auto_generated: false,
    }];
    let manager = IndexManager::from_collection(1, &schema).unwrap();
    let mut doc = Document::new();
    doc.set(
        "custom",
        NormalValue::Json(serde_json::json!({"numbers": [5, 8, 5]})),
    );

    let error = manager
        .on_document_create(&datastore, &doc, next_test_doc_short_id(), &schema)
        .await
        .expect_err("duplicate JSON array values must violate a unique index");

    assert!(matches!(
        error,
        Error::Storage(storage::Error::UniqueConstraintViolation)
    ));
}

#[tokio::test]
async fn unique_typed_array_index_deduplicates_repeated_values() {
    let store = RegolithStore::in_memory().unwrap();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut schema = CollectionVersion::new(
        "users",
        "v1",
        "col-users",
        vec![FieldDescription::new(
            "1",
            "tags",
            FieldKind::string_array(),
        )],
    );
    schema.indexes = vec![IndexDescription {
        name: "idx_tags".to_string(),
        id: 1,
        fields: vec![IndexedFieldDescription {
            name: "tags".to_string(),
            descending: false,
        }],
        unique: true,
        kind: None,
        auto_generated: false,
    }];
    let manager = IndexManager::from_collection(1, &schema).unwrap();
    let mut first = Document::new();
    first.set(
        "tags",
        NormalValue::StringArray(vec!["a".to_string(), "a".to_string(), "b".to_string()]),
    );

    manager
        .on_document_create(&datastore, &first, next_test_doc_short_id(), &schema)
        .await
        .expect("repeated values in one typed array must share one index entry");
}

#[tokio::test]
async fn test_create_duplicate_index_fails() {
    let store = RegolithStore::in_memory().unwrap();
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
    let store = RegolithStore::in_memory().unwrap();
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
async fn test_delete_index() {
    let store = RegolithStore::in_memory().unwrap();
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

    let dropped = manager.delete_index(&datastore, "idx_name").await.unwrap();
    assert!(dropped);
    assert!(!manager.has_index("idx_name"));
}

#[tokio::test]
async fn test_delete_nonexistent_index() {
    let store = RegolithStore::in_memory().unwrap();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut manager = IndexManager::new(1);

    let dropped = manager
        .delete_index(&datastore, "nonexistent")
        .await
        .unwrap();
    assert!(!dropped);
}

#[tokio::test]
async fn test_get_indexes() {
    let store = RegolithStore::in_memory().unwrap();
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
            kind: None,
            auto_generated: false,
        },
        IndexDescription {
            name: "idx_email".to_string(),
            id: 2,
            fields: vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            unique: true,
            kind: None,
            auto_generated: false,
        },
    ];

    let manager = IndexManager::from_collection(1, &schema).unwrap();

    assert_eq!(manager.index_count(), 2);
    assert!(manager.has_index("idx_name"));
    assert!(manager.has_index("idx_email"));
}

#[tokio::test]
async fn test_fulltext_indexes_use_reserved_internal_names() {
    let mut schema = test_schema();
    schema.fulltext_indexes = vec![FullTextIndexDescription::new("name")];

    let manager = IndexManager::from_collection(1, &schema).unwrap();

    assert!(manager.has_index(RESERVED_FULLTEXT_INDEX_NAME_FOR_NAME));
    assert!(!manager.has_index("name_fulltext"));
}

#[tokio::test]
async fn test_regular_index_named_field_fulltext_does_not_collide_with_fulltext_index() {
    let store = RegolithStore::in_memory().unwrap();
    let db = DB::new(store).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();

    let mut schema = test_schema();
    schema.fulltext_indexes = vec![FullTextIndexDescription::new("name")];

    let mut manager = IndexManager::from_collection(1, &schema).unwrap();

    let desc = manager
        .create_index(
            &datastore,
            "users",
            "name_fulltext".to_string(),
            vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            false,
            &schema.fields,
        )
        .await
        .unwrap();

    assert_eq!(desc.name, "name_fulltext");
    assert!(manager.has_index("name_fulltext"));
    assert!(manager.has_index(RESERVED_FULLTEXT_INDEX_NAME_FOR_NAME));
}

#[tokio::test]
async fn test_on_document_create() {
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
            .await
            .unwrap();
    }
    // datastore is dropped here, releasing the Arc<SharedTxn>

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_index_id_sequence() {
    let store = RegolithStore::in_memory().unwrap();
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
    let store = RegolithStore::in_memory().unwrap();
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
        let old_doc_short_id = next_test_doc_short_id();
        old_doc.set("name", NormalValue::String("Alice".to_string()));
        old_doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &old_doc, old_doc_short_id, &schema)
            .await
            .unwrap();

        // Create updated document with new name
        let mut new_doc = Document::new();
        new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
        new_doc.set("age", NormalValue::Int(31));

        // Update should succeed
        manager
            .on_document_update(&datastore, &old_doc, &new_doc, old_doc_short_id, &schema)
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
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
            .await
            .unwrap();

        // Update with same indexed value but different non-indexed value
        let mut new_doc = Document::new();
        new_doc.set("name", NormalValue::String("Alice".to_string())); // Same
        new_doc.set("age", NormalValue::Int(31)); // Different but not indexed

        // Should succeed (optimization path - no actual index write)
        manager
            .on_document_update(&datastore, &doc, &new_doc, doc_short_id, &schema)
            .await
            .unwrap();
    }

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_on_document_delete_removes_index_entries() {
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
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
            .on_document_delete(&datastore, &doc, doc_short_id, &schema)
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
    let store = RegolithStore::in_memory().unwrap();
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
            doc.set("name", NormalValue::String(name.to_string()));
            docs.push((next_test_doc_short_id(), doc));
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
async fn test_bulk_index_skips_documents_without_short_id() {
    let store = RegolithStore::in_memory().unwrap();
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

        // Create documents - one with a short ID, one with the unset marker (0)
        let mut doc_with_id = Document::new();
        doc_with_id.set("name", NormalValue::String("Alice".to_string()));

        let mut doc_without_id = Document::new();
        doc_without_id.set("name", NormalValue::String("Bob".to_string()));

        let docs = vec![(next_test_doc_short_id(), doc_with_id), (0, doc_without_id)];

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
    let store = RegolithStore::in_memory().unwrap();
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
async fn test_on_document_create_without_short_id_fails() {
    let store = RegolithStore::in_memory().unwrap();
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

        // Unset short ID (0) marks a document without storage identity
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));

        let result = manager
            .on_document_create(&datastore, &doc, 0, &schema)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
    }
}

#[tokio::test]
async fn test_on_document_update_without_short_id_fails() {
    let store = RegolithStore::in_memory().unwrap();
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

        let mut old_doc = Document::new();
        old_doc.set("name", NormalValue::String("Alice".to_string()));

        let mut new_doc = Document::new();
        new_doc.set("name", NormalValue::String("Alice Smith".to_string()));

        // Unset short ID (0) marks a document without storage identity
        let result = manager
            .on_document_update(&datastore, &old_doc, &new_doc, 0, &schema)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
    }
}

#[tokio::test]
async fn test_on_document_delete_without_short_id_fails() {
    let store = RegolithStore::in_memory().unwrap();
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

        // Unset short ID (0) marks a document without storage identity
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));

        let result = manager
            .on_document_delete(&datastore, &doc, 0, &schema)
            .await;

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
        kind: None,
        auto_generated: false,
    }];

    let result = IndexManager::from_collection(1, &schema);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("no fields"));
}

#[tokio::test]
async fn test_multi_index_update() {
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set(
            "email",
            NormalValue::String("alice@example.com".to_string()),
        );

        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
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
        let mut new_doc = Document::new();
        new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
        new_doc.set(
            "email",
            NormalValue::String("alice.smith@example.com".to_string()),
        );

        manager
            .on_document_update(&datastore, &doc, &new_doc, doc_short_id, &schema)
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
    let store = RegolithStore::in_memory().unwrap();
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
        let doc1_short_id = next_test_doc_short_id();

        let mut doc2 = Document::new();
        doc2.set("category", NormalValue::String("electronics".to_string()));
        doc2.set("price", NormalValue::Int(200));
        doc2.set("name", NormalValue::String("Gadget".to_string()));
        let doc2_short_id = next_test_doc_short_id();

        let mut doc3 = Document::new();
        doc3.set("category", NormalValue::String("books".to_string()));
        doc3.set("price", NormalValue::Int(50));
        doc3.set("name", NormalValue::String("Novel".to_string()));
        let doc3_short_id = next_test_doc_short_id();

        // Index all documents
        manager
            .on_document_create(&datastore, &doc1, doc1_short_id, &schema)
            .await
            .unwrap();
        manager
            .on_document_create(&datastore, &doc2, doc2_short_id, &schema)
            .await
            .unwrap();
        manager
            .on_document_create(&datastore, &doc3, doc3_short_id, &schema)
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
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();

        // Should succeed - missing field indexed as NULL
        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
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
        let doc2_short_id = next_test_doc_short_id();

        manager
            .on_document_create(&datastore, &doc2, doc2_short_id, &schema)
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
    let store = RegolithStore::in_memory().unwrap();
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
        let doc1_short_id = next_test_doc_short_id();

        manager
            .on_document_create(&datastore, &doc1, doc1_short_id, &schema)
            .await
            .unwrap();

        // Create second document without email (also NULL)
        // This should succeed - NULL is not considered equal to NULL for uniqueness
        // Set fields BEFORE generating doc_id
        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Bob".to_string()));
        let doc2_short_id = next_test_doc_short_id();

        let result = manager
            .on_document_create(&datastore, &doc2, doc2_short_id, &schema)
            .await;
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

    let store = RegolithStore::in_memory().unwrap();
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
            kind: None,
            auto_generated: false,
        };

        let index = UniqueIndex::new(1, desc);
        let values = vec![NormalValue::String("alice@example.com".to_string())];

        // First save through NamespaceView
        let mut ds1 = datastore.clone();
        index.save(&mut ds1, 1, &values).await.unwrap();

        // Second save - should fail
        let mut ds2 = datastore.clone();
        let result = index.save(&mut ds2, 2, &values).await;

        assert!(
            result.is_err(),
            "UniqueIndex should reject duplicate through NamespaceView"
        );
    }

    // Now test through IndexManager
    let store2 = RegolithStore::in_memory().unwrap();
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
        let doc1_short_id = next_test_doc_short_id();

        manager
            .on_document_create(&datastore, &doc1, doc1_short_id, &schema)
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
        let doc2_short_id = next_test_doc_short_id();

        let result = manager
            .on_document_create(&datastore, &doc2, doc2_short_id, &schema)
            .await;
        let error = result.expect_err("duplicate value should fail through IndexManager");
        assert!(
            matches!(
                error,
                db::index::Error::Storage(storage::Error::UniqueConstraintViolation)
            ),
            "duplicate value should preserve typed unique constraint error: {error}"
        );
    }
}

#[tokio::test]
async fn test_index_field_not_in_schema_fails() {
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();
        doc.set("name", NormalValue::String("Alice".to_string()));

        // Indexing should fail because the field doesn't exist in schema
        let result = manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
            .await;
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
    let store = RegolithStore::in_memory().unwrap();
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
        let doc_short_id = next_test_doc_short_id();

        // Index the same document twice
        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
            .await
            .unwrap();
        manager
            .on_document_create(&datastore, &doc, doc_short_id, &schema)
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
    let store = RegolithStore::in_memory().unwrap();
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
        let doc1_short_id = next_test_doc_short_id();

        manager
            .on_document_create(&datastore, &doc1, doc1_short_id, &schema)
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
            .on_document_delete(&datastore, &doc1, doc1_short_id, &schema)
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
        let doc2_short_id = next_test_doc_short_id();

        manager
            .on_document_create(&datastore, &doc2, doc2_short_id, &schema)
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

/// Unique-index semantics at the deletion and merge boundaries (#1111 /
/// source-inc/gents#700).
mod unique_boundaries {
    use super::*;
    use datastore::NamespaceView;
    use document::DocID;
    use storage::corekv::Writer;

    const COLLECTION_ID: &str = "col-users";
    const COLLECTION_SHORT_ID: u32 = 1;

    fn unique_email_schema() -> CollectionVersion {
        let mut schema = test_schema();
        schema.indexes = vec![IndexDescription {
            name: "idx_email_unique".to_string(),
            id: 1,
            fields: vec![IndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            unique: true,
            kind: None,
            auto_generated: false,
        }];
        schema
    }

    /// A test document plus the node-local short ID it is stored under.
    ///
    /// Documents and index entries are keyed by short IDs (#4838), but the
    /// deterministic merge winner is decided on the PUBLIC DocID, so each doc
    /// carries a distinct DocID whose short ID is registered in the systemstore.
    struct Doc {
        doc: Document,
        short_id: u64,
    }

    fn doc_with_email(email: &str) -> Document {
        // Distinct seed per doc: DocIDs are content-addressed, so identical
        // content would collapse "two docs" into one id and the conflict under
        // test would evaporate.
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let mut doc = Document::new();
        doc.set("email", NormalValue::String(email.to_string()));
        doc.set_id(DocID::new_v0_from_seed(&format!(
            "doc-{}",
            SEQ.fetch_add(1, Ordering::SeqCst)
        )));
        doc
    }

    /// Allocate a short ID for a doc and register the short ID -> DocID mapping
    /// the merge winner resolution reads back.
    async fn register(systemstore: &NamespaceView, email: &str) -> Doc {
        let doc = doc_with_email(email);
        let short_id = next_test_doc_short_id();
        db::docid::map::set_doc_id_mapping(
            systemstore,
            COLLECTION_SHORT_ID,
            short_id,
            &doc.id().unwrap().to_string(),
        )
        .await
        .unwrap();
        Doc { doc, short_id }
    }

    async fn write_doc_body(datastore: &mut NamespaceView, entry: &Doc) {
        let key = storage::keys::doc_key(COLLECTION_ID, entry.short_id);
        datastore
            .set(&key, &entry.doc.to_cbor().unwrap())
            .await
            .unwrap();
    }

    async fn write_tombstone(datastore: &mut NamespaceView, entry: &Doc) {
        let key = storage::keys::deleted_doc_key(COLLECTION_ID, entry.short_id);
        datastore.set(&key, &[1u8]).await.unwrap();
    }

    /// Go-parity regression (source-inc/gents#700): deleting the doc
    /// that holds a unique value frees the slot for a new doc with that value.
    #[tokio::test]
    async fn recreate_after_delete_frees_the_unique_slot() {
        let store = RegolithStore::in_memory().unwrap();
        let db = DB::new(store).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        let mut datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();

        let schema = unique_email_schema();
        let manager = IndexManager::from_collection(COLLECTION_SHORT_ID, &schema).unwrap();

        let first = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &first).await;
        manager
            .on_document_create(&datastore, &first.doc, first.short_id, &schema)
            .await
            .unwrap();

        manager
            .on_document_delete(&datastore, &first.doc, first.short_id, &schema)
            .await
            .unwrap();
        write_tombstone(&mut datastore, &first).await;

        let second = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &second).await;
        manager
            .on_document_create(&datastore, &second.doc, second.short_id, &schema)
            .await
            .expect("a tombstone must not hold the unique slot");
    }

    /// The #700 wound itself: a stale entry pointing at a TOMBSTONED doc
    /// (minted by an era or path without index maintenance) must be reclaimed
    /// by the next create instead of blocking the value forever.
    #[tokio::test]
    async fn stale_entry_pointing_at_tombstoned_doc_is_reclaimed() {
        let store = RegolithStore::in_memory().unwrap();
        let db = DB::new(store).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        let mut datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();

        let schema = unique_email_schema();
        let manager = IndexManager::from_collection(COLLECTION_SHORT_ID, &schema).unwrap();

        // Wound the store: holder is indexed, then tombstoned WITHOUT index
        // cleanup (exactly what pre-maintenance eras and out-of-order merges
        // left behind).
        let holder = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &holder).await;
        manager
            .on_document_create(&datastore, &holder.doc, holder.short_id, &schema)
            .await
            .unwrap();
        write_tombstone(&mut datastore, &holder).await;

        let newcomer = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &newcomer).await;
        manager
            .on_document_create(&datastore, &newcomer.doc, newcomer.short_id, &schema)
            .await
            .expect("a stale unique entry pointing at a tombstone must be reclaimed");
    }

    /// A stale entry whose holder never existed locally (index written, doc
    /// body missing — the partial-write wound) is equally reclaimable.
    #[tokio::test]
    async fn stale_entry_pointing_at_missing_doc_is_reclaimed() {
        let store = RegolithStore::in_memory().unwrap();
        let db = DB::new(store).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        let mut datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();

        let schema = unique_email_schema();
        let manager = IndexManager::from_collection(COLLECTION_SHORT_ID, &schema).unwrap();

        let ghost = register(&systemstore, "a@x").await;
        // Index the ghost WITHOUT writing its body.
        manager
            .on_document_create(&datastore, &ghost.doc, ghost.short_id, &schema)
            .await
            .unwrap();

        let newcomer = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &newcomer).await;
        manager
            .on_document_create(&datastore, &newcomer.doc, newcomer.short_id, &schema)
            .await
            .expect("a unique entry pointing at a missing doc must be reclaimed");
    }

    /// Healing must not weaken real enforcement: a live holder still rejects.
    #[tokio::test]
    async fn live_conflict_is_still_rejected_on_the_local_path() {
        let store = RegolithStore::in_memory().unwrap();
        let db = DB::new(store).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        let mut datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();

        let schema = unique_email_schema();
        let manager = IndexManager::from_collection(COLLECTION_SHORT_ID, &schema).unwrap();

        let holder = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &holder).await;
        manager
            .on_document_create(&datastore, &holder.doc, holder.short_id, &schema)
            .await
            .unwrap();

        let challenger = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &challenger).await;
        let result = manager
            .on_document_create(&datastore, &challenger.doc, challenger.short_id, &schema)
            .await;
        assert!(
            result.is_err(),
            "a live unique conflict must still reject on the local path"
        );
    }

    /// #1111: the merge path resolves a live conflict deterministically —
    /// the lexicographically smallest PUBLIC DocID wins — instead of failing
    /// the merge and wedging the document's history in permanent retry. The
    /// winner is decided on the public DocID (identical on every replica), not
    /// the node-local short id, so both orders land on the same winner.
    #[tokio::test]
    async fn merge_conflict_resolves_to_the_smallest_doc_id_in_both_orders() {
        let store = RegolithStore::in_memory().unwrap();
        let db = DB::new(store).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        let mut datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();

        let schema = unique_email_schema();
        let manager = IndexManager::from_collection(COLLECTION_SHORT_ID, &schema).unwrap();

        let a = register(&systemstore, "a@x").await;
        let b = register(&systemstore, "a@x").await;
        let (smaller, larger) = if a.doc.id().unwrap().to_string() < b.doc.id().unwrap().to_string()
        {
            (a, b)
        } else {
            (b, a)
        };

        // Order 1: larger holds the entry, smaller arrives via merge — the
        // incoming doc wins and takes the entry.
        write_doc_body(&mut datastore, &larger).await;
        manager
            .on_document_create(&datastore, &larger.doc, larger.short_id, &schema)
            .await
            .unwrap();
        write_doc_body(&mut datastore, &smaller).await;
        manager
            .on_document_create_merge(
                &datastore,
                &systemstore,
                &smaller.doc,
                smaller.short_id,
                &schema,
            )
            .await
            .expect("merge must not fail on a live unique conflict");

        // The winner (smaller) now holds the entry: a fresh local challenger
        // conflicts, and after tombstoning the LOSER nothing changes (the
        // loser holds no entry).
        let challenger = register(&systemstore, "a@x").await;
        write_doc_body(&mut datastore, &challenger).await;
        assert!(
            manager
                .on_document_create(&datastore, &challenger.doc, challenger.short_id, &schema)
                .await
                .is_err(),
            "the winner must hold the unique entry after resolution"
        );

        // Order 2 (fresh store): smaller holds, larger arrives via merge —
        // the incoming doc loses and stays unindexed; merge still succeeds.
        let store2 = RegolithStore::in_memory().unwrap();
        let db2 = DB::new(store2).unwrap();
        let txn2 = db2.new_txn(false).await.unwrap();
        let mut datastore2 = txn2.datastore().unwrap();
        let systemstore2 = txn2.systemstore().unwrap();
        let manager2 = IndexManager::from_collection(COLLECTION_SHORT_ID, &schema).unwrap();

        let a2 = register(&systemstore2, "a@x").await;
        let b2 = register(&systemstore2, "a@x").await;
        let (smaller2, larger2) =
            if a2.doc.id().unwrap().to_string() < b2.doc.id().unwrap().to_string() {
                (a2, b2)
            } else {
                (b2, a2)
            };
        write_doc_body(&mut datastore2, &smaller2).await;
        manager2
            .on_document_create(&datastore2, &smaller2.doc, smaller2.short_id, &schema)
            .await
            .unwrap();
        write_doc_body(&mut datastore2, &larger2).await;
        manager2
            .on_document_create_merge(
                &datastore2,
                &systemstore2,
                &larger2.doc,
                larger2.short_id,
                &schema,
            )
            .await
            .expect("merge must not fail when the incoming doc loses the pick");

        // The entry still belongs to smaller2: deleting larger2 (the loser,
        // unindexed) must leave the slot occupied.
        manager2
            .on_document_delete(&datastore2, &larger2.doc, larger2.short_id, &schema)
            .await
            .unwrap();
        let challenger2 = register(&systemstore2, "a@x").await;
        write_doc_body(&mut datastore2, &challenger2).await;
        assert!(
            manager2
                .on_document_create(&datastore2, &challenger2.doc, challenger2.short_id, &schema)
                .await
                .is_err(),
            "the winner must still hold the unique entry after the loser is deleted"
        );
    }
}

/// A vector index covers exactly one field and is never unique, which is why
/// `IndexKind::Vector` has no `unique` to carry.
///
/// The reference refuses both pairings (`errVectorIndexCannotBeUnique`,
/// `errVectorIndexRequiresSingleField`); we used to absorb them, building an
/// index that did not enforce the uniqueness asked for, or one over a different
/// column than the request named.
mod requested_kind {
    use db::index::manager::IndexManager;
    use schema::{
        DistanceMetric, IndexKind, IndexedFieldDescription, VectorAlgorithm, VectorIndexDescription,
    };

    fn field(name: &str) -> IndexedFieldDescription {
        IndexedFieldDescription {
            name: name.to_string(),
            descending: false,
        }
    }

    fn vector() -> VectorIndexDescription {
        VectorIndexDescription::with_defaults(VectorAlgorithm::Hnsw, DistanceMetric::Cosine, 8)
    }

    #[test]
    fn no_vector_config_is_an_ordered_index() {
        for unique in [false, true] {
            let kind = IndexManager::requested_kind(&[field("name")], unique, None)
                .expect("an ordered index takes any uniqueness");
            match kind {
                IndexKind::Ordered(ordered) => assert_eq!(ordered.unique, unique),
                other => panic!("expected ordered, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_single_field_vector_request_is_a_vector_index() {
        let kind = IndexManager::requested_kind(&[field("embedding")], false, Some(vector()))
            .expect("one field and not unique is the valid shape");
        match kind {
            IndexKind::Vector(described) => assert_eq!(described, vector()),
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[test]
    fn a_unique_vector_request_is_refused() {
        let error = IndexManager::requested_kind(&[field("embedding")], true, Some(vector()))
            .expect_err("unique and vector cannot both hold")
            .to_string();
        assert!(error.contains("unique"), "got: {error}");
    }

    #[test]
    fn a_multi_field_vector_request_is_refused() {
        for fields in [vec![], vec![field("a"), field("b")]] {
            let error = IndexManager::requested_kind(&fields, false, Some(vector()))
                .expect_err("a vector index covers exactly one field")
                .to_string();
            assert!(error.contains("exactly one field"), "got: {error}");
        }
    }
}
