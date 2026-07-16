//! Additional tests for index functionality
//!
//! This module contains:
//! - Field count mismatch error tests
//! - Concurrent index operation tests
//! - Index validation tests

use document::NormalValue;
use schema::{IndexDescription, IndexedFieldDescription};
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::index::{CollectionIndex, SimpleIndex, UniqueIndex};

// ============================================================================
// Helper Functions
// ============================================================================

fn simple_index_description() -> IndexDescription {
    IndexDescription {
        id: 1,
        name: "test_simple_index".to_string(),
        unique: false,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
    }
}

fn unique_index_description() -> IndexDescription {
    IndexDescription {
        id: 1,
        name: "test_unique_index".to_string(),
        unique: true,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "email".to_string(),
            descending: false,
        }],
    }
}

fn composite_index_description(unique: bool) -> IndexDescription {
    IndexDescription {
        id: 2,
        name: "test_composite_index".to_string(),
        unique,
        auto_generated: false,
        fields: vec![
            IndexedFieldDescription {
                name: "category".to_string(),
                descending: false,
            },
            IndexedFieldDescription {
                name: "priority".to_string(),
                descending: true,
            },
        ],
    }
}

// ============================================================================
// Field Count Mismatch Error Tests
// ============================================================================

#[tokio::test]
async fn test_simple_index_save_too_few_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, composite_index_description(false));
    // Composite index expects 2 values, but we provide only 1
    let values = vec![NormalValue::String("electronics".to_string())];

    let result = index.save(&mut txn, 1, &values).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("field count mismatch"),
        "error should mention field count mismatch: {}",
        err_msg
    );
    assert!(
        err_msg.contains("expected 2 fields"),
        "error should mention expected count: {}",
        err_msg
    );
    assert!(
        err_msg.contains("got 1"),
        "error should mention actual count: {}",
        err_msg
    );
    assert!(
        err_msg.contains("for document '1'"),
        "error should mention the doc short ID: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_simple_index_save_too_many_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, simple_index_description());
    // Simple index expects 1 value, but we provide 2
    let values = vec![
        NormalValue::String("alice".to_string()),
        NormalValue::String("extra".to_string()),
    ];

    let result = index.save(&mut txn, 1, &values).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("field count mismatch"),
        "error should mention field count mismatch: {}",
        err_msg
    );
    assert!(
        err_msg.contains("expected 1 fields"),
        "error should mention expected count: {}",
        err_msg
    );
    assert!(
        err_msg.contains("got 2"),
        "error should mention actual count: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_simple_index_update_old_values_mismatch() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, composite_index_description(false));
    // Old values has wrong count
    let old_values = vec![NormalValue::String("electronics".to_string())];
    let new_values = vec![
        NormalValue::String("electronics".to_string()),
        NormalValue::Int(10),
    ];

    let result = index.update(&mut txn, 1, &old_values, &new_values).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("field count"));
}

#[tokio::test]
async fn test_simple_index_update_new_values_mismatch() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, composite_index_description(false));
    let old_values = vec![
        NormalValue::String("electronics".to_string()),
        NormalValue::Int(5),
    ];
    // New values has wrong count
    let new_values = vec![NormalValue::String("electronics".to_string())];

    let result = index.update(&mut txn, 1, &old_values, &new_values).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("field count"));
}

#[tokio::test]
async fn test_simple_index_delete_values_mismatch() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, composite_index_description(false));
    // Wrong number of values for delete
    let values = vec![NormalValue::String("electronics".to_string())];

    let result = index.delete(&mut txn, 1, &values).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("field count"));
}

#[tokio::test]
async fn test_unique_index_save_too_few_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, composite_index_description(true));
    let values = vec![NormalValue::String("electronics".to_string())];

    let result = index.save(&mut txn, 1, &values).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("field count mismatch"));
}

#[tokio::test]
async fn test_unique_index_save_too_many_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, unique_index_description());
    let values = vec![
        NormalValue::String("alice@example.com".to_string()),
        NormalValue::String("extra".to_string()),
    ];

    let result = index.save(&mut txn, 1, &values).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("field count mismatch"));
}

#[tokio::test]
async fn test_unique_index_empty_values() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, unique_index_description());
    let values: Vec<NormalValue> = vec![];

    let result = index.save(&mut txn, 1, &values).await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("field count mismatch"));
    assert!(err_msg.contains("got 0"));
}

// ============================================================================
// Index Validation Tests
// ============================================================================

#[test]
#[should_panic(expected = "SimpleIndex requires non-unique index")]
fn test_simple_index_rejects_unique_description() {
    let desc = unique_index_description(); // unique = true
    let _ = SimpleIndex::new(1, desc);
}

#[test]
#[should_panic(expected = "UniqueIndex requires unique index")]
fn test_unique_index_rejects_non_unique_description() {
    let desc = simple_index_description(); // unique = false
    let _ = UniqueIndex::new(1, desc);
}

#[test]
fn test_simple_index_try_new_rejects_unique() {
    let desc = unique_index_description();
    let result = SimpleIndex::try_new(1, desc);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("non-unique index"));
}

#[test]
fn test_unique_index_try_new_rejects_non_unique() {
    let desc = simple_index_description();
    let result = UniqueIndex::try_new(1, desc);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("unique index"));
}

// ============================================================================
// Concurrent Index Operation Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_saves_to_simple_index() {
    use std::sync::Arc;

    let store = Arc::new(MemoryStore::new());
    let index = Arc::new(SimpleIndex::new(1, simple_index_description()));

    // Spawn multiple concurrent save operations
    let mut handles = vec![];
    for i in 0..10 {
        let store_clone = store.clone();
        let index_clone = index.clone();
        let handle = tokio::spawn(async move {
            let mut txn = store_clone.new_txn(false).await.unwrap();
            let doc_short_id = (i + 1) as u64;
            let values = vec![NormalValue::String(format!("value{}", i))];
            index_clone
                .save(&mut txn, doc_short_id, &values)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all entries were saved
    let txn = store.new_txn(true).await.unwrap();
    let prefix = storage::keys::IndexDataStoreKey::index_prefix(1, 1);
    let opts = storage::corekv::IterOptions::default().with_prefix(prefix);
    let mut iter = storage::corekv::Reader::iterator(txn.as_ref(), opts)
        .await
        .unwrap();
    let count = iter.count().await.unwrap();
    assert_eq!(count, 10, "all 10 concurrent saves should succeed");
}

#[tokio::test]
async fn test_concurrent_saves_to_unique_index_different_values() {
    use std::sync::Arc;

    let store = Arc::new(MemoryStore::new());
    let index = Arc::new(UniqueIndex::new(1, unique_index_description()));

    // Spawn multiple concurrent save operations with different values
    let mut handles = vec![];
    for i in 0..10 {
        let store_clone = store.clone();
        let index_clone = index.clone();
        let handle = tokio::spawn(async move {
            let mut txn = store_clone.new_txn(false).await.unwrap();
            let doc_short_id = (i + 1) as u64;
            let values = vec![NormalValue::String(format!("unique_email_{}@test.com", i))];
            index_clone
                .save(&mut txn, doc_short_id, &values)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all entries were saved
    let txn = store.new_txn(true).await.unwrap();
    let prefix = storage::keys::IndexDataStoreKey::index_prefix(1, 1);
    let opts = storage::corekv::IterOptions::default().with_prefix(prefix);
    let mut iter = storage::corekv::Reader::iterator(txn.as_ref(), opts)
        .await
        .unwrap();
    let count = iter.count().await.unwrap();
    assert_eq!(count, 10);
}

#[tokio::test]
async fn test_concurrent_save_and_delete() {
    use std::sync::Arc;

    let store = Arc::new(MemoryStore::new());
    let index = Arc::new(SimpleIndex::new(1, simple_index_description()));

    // First, save some entries
    {
        let mut txn = store.new_txn(false).await.unwrap();
        for i in 0..5 {
            let doc_short_id = (i + 1) as u64;
            let values = vec![NormalValue::String(format!("value{}", i))];
            index.save(&mut txn, doc_short_id, &values).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    // Concurrently save new entries and delete old ones
    let mut handles = vec![];

    // Save new entries (doc5-doc9)
    for i in 5..10 {
        let store_clone = store.clone();
        let index_clone = index.clone();
        let handle = tokio::spawn(async move {
            let mut txn = store_clone.new_txn(false).await.unwrap();
            let doc_short_id = (i + 1) as u64;
            let values = vec![NormalValue::String(format!("value{}", i))];
            index_clone
                .save(&mut txn, doc_short_id, &values)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        });
        handles.push(handle);
    }

    // Delete old entries (doc0-doc2)
    for i in 0..3 {
        let store_clone = store.clone();
        let index_clone = index.clone();
        let handle = tokio::spawn(async move {
            let mut txn = store_clone.new_txn(false).await.unwrap();
            let doc_short_id = (i + 1) as u64;
            let values = vec![NormalValue::String(format!("value{}", i))];
            index_clone
                .delete(&mut txn, doc_short_id, &values)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify: should have doc3, doc4, doc5, doc6, doc7, doc8, doc9 = 7 entries
    let txn = store.new_txn(true).await.unwrap();
    let prefix = storage::keys::IndexDataStoreKey::index_prefix(1, 1);
    let opts = storage::corekv::IterOptions::default().with_prefix(prefix);
    let mut iter = storage::corekv::Reader::iterator(txn.as_ref(), opts)
        .await
        .unwrap();
    let count = iter.count().await.unwrap();
    assert_eq!(count, 7, "should have 7 entries after concurrent ops");
}

#[tokio::test]
async fn test_concurrent_updates() {
    use std::sync::Arc;

    let store = Arc::new(MemoryStore::new());
    let index = Arc::new(SimpleIndex::new(1, simple_index_description()));

    // First, save some entries
    {
        let mut txn = store.new_txn(false).await.unwrap();
        for i in 0..5 {
            let doc_short_id = (i + 1) as u64;
            let values = vec![NormalValue::String(format!("old_value{}", i))];
            index.save(&mut txn, doc_short_id, &values).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    // Concurrently update all entries
    let mut handles = vec![];
    for i in 0..5 {
        let store_clone = store.clone();
        let index_clone = index.clone();
        let handle = tokio::spawn(async move {
            let mut txn = store_clone.new_txn(false).await.unwrap();
            let doc_short_id = (i + 1) as u64;
            let old_values = vec![NormalValue::String(format!("old_value{}", i))];
            let new_values = vec![NormalValue::String(format!("new_value{}", i))];
            index_clone
                .update(&mut txn, doc_short_id, &old_values, &new_values)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all updates
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify: should still have 5 entries (updates don't change count)
    let txn = store.new_txn(true).await.unwrap();
    let prefix = storage::keys::IndexDataStoreKey::index_prefix(1, 1);
    let opts = storage::corekv::IterOptions::default().with_prefix(prefix);
    let mut iter = storage::corekv::Reader::iterator(txn.as_ref(), opts)
        .await
        .unwrap();
    let count = iter.count().await.unwrap();
    assert_eq!(count, 5);
}

// ============================================================================
// Partial NULL Composite Index Tests
// ============================================================================

#[tokio::test]
async fn test_composite_unique_partial_null_first_field() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, composite_index_description(true));

    // First field NULL, second field non-NULL
    let values1 = vec![NormalValue::Null, NormalValue::Int(10)];
    let values2 = vec![NormalValue::Null, NormalValue::Int(20)];

    // Both should succeed since NULL is in the composite
    index.save(&mut txn, 1, &values1).await.unwrap();
    index.save(&mut txn, 2, &values2).await.unwrap();
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_composite_unique_partial_null_second_field() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, composite_index_description(true));

    // First field non-NULL, second field NULL
    let values1 = vec![
        NormalValue::String("category".to_string()),
        NormalValue::Null,
    ];
    let values2 = vec![
        NormalValue::String("category".to_string()),
        NormalValue::Null,
    ];

    // Go-compatible behavior: if ANY field is NULL, uniqueness is bypassed.
    // Multiple documents can have the same non-NULL values as long as at least
    // one field is NULL. This matches Go's hasIndexKeyNilField() behavior.
    index.save(&mut txn, 1, &values1).await.unwrap();
    index.save(&mut txn, 2, &values2).await.unwrap();
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_composite_unique_enforced_on_non_null() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, composite_index_description(true));

    // Both fields non-NULL with same values
    let values = vec![
        NormalValue::String("electronics".to_string()),
        NormalValue::Int(10),
    ];

    index.save(&mut txn, 1, &values).await.unwrap();

    // Should fail - same non-NULL values
    let result = index.save(&mut txn, 2, &values).await;
    let error = result.expect_err("duplicate composite unique value should fail");
    assert!(matches!(
        error,
        storage::corekv::Error::UniqueConstraintViolation
    ));
    assert_eq!(
        error.to_string(),
        storage::corekv::UNIQUE_CONSTRAINT_VIOLATION_MESSAGE
    );
}
