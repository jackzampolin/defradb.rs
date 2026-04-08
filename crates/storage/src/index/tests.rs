use super::*;
use crate::backends::MemoryStore;
use crate::corekv::{IterOptions, Store};
use crate::keys::IndexDataStoreKey;
use document::NormalValue;
use schema::IndexedFieldDescription;

fn test_index_description(unique: bool) -> schema::IndexDescription {
    schema::IndexDescription {
        id: 1,
        name: "test_index".to_string(),
        unique,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
    }
}

fn composite_index_description(unique: bool) -> schema::IndexDescription {
    schema::IndexDescription {
        id: 2,
        name: "composite_index".to_string(),
        unique,
        auto_generated: false,
        fields: vec![
            IndexedFieldDescription {
                name: "category".to_string(),
                descending: false,
            },
            IndexedFieldDescription {
                name: "created_at".to_string(),
                descending: true,
            },
        ],
    }
}

/// Helper to count entries with a prefix
async fn count_entries(txn: &dyn crate::corekv::Reader, prefix: &[u8]) -> usize {
    let opts = IterOptions::default().with_prefix(prefix.to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();
    iter.count().await.unwrap()
}

/// Helper to get entries with a prefix
async fn get_entries(txn: &dyn crate::corekv::Reader, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let opts = IterOptions::default().with_prefix(prefix.to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();
    let items = iter.collect_all().await.unwrap();
    items.into_iter().map(|kv| (kv.key, kv.value)).collect()
}

#[tokio::test]
async fn test_simple_index_save() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, test_index_description(false));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, "doc1", &values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify entry exists
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_simple_index_allows_duplicates() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, test_index_description(false));
    let values = vec![NormalValue::String("alice".to_string())];

    // Same value, different doc IDs - should work
    index.save(&mut txn, "doc1", &values).await.unwrap();
    index.save(&mut txn, "doc2", &values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify both entries exist
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_simple_index_update() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, test_index_description(false));
    let old_values = vec![NormalValue::String("alice".to_string())];
    let new_values = vec![NormalValue::String("bob".to_string())];

    index.save(&mut txn, "doc1", &old_values).await.unwrap();
    index
        .update(&mut txn, "doc1", &old_values, &new_values)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify only one entry exists (with new value)
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_simple_index_delete() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, test_index_description(false));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, "doc1", &values).await.unwrap();
    index.delete(&mut txn, "doc1", &values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify no entries exist
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_simple_index_remove_all() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, test_index_description(false));

    index
        .save(&mut txn, "doc1", &[NormalValue::String("a".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::String("b".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, "doc3", &[NormalValue::String("c".to_string())])
        .await
        .unwrap();
    index.remove_all(&mut txn).await.unwrap();
    txn.commit().await.unwrap();

    // Verify all entries removed
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_unique_index_save() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, "doc1", &values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify entry exists with doc_id in value
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let entries = get_entries(txn.as_ref(), &prefix).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, "doc1".as_bytes());
}

#[tokio::test]
async fn test_unique_index_rejects_duplicates() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, "doc1", &values).await.unwrap();

    // Same value, different doc ID - should fail
    let result = index.save(&mut txn, "doc2", &values).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("violates unique index"),
        "error should mention unique index violation: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_unique_index_rejects_same_doc_duplicate() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, "doc1", &values).await.unwrap();

    // Same value, same doc ID - should fail (e.g., JSON array self-duplicates)
    let result = index.save(&mut txn, "doc1", &values).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_unique_index_null_allows_duplicates() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let null_values = vec![NormalValue::Null];

    // Multiple docs with NULL value - should be allowed
    index.save(&mut txn, "doc1", &null_values).await.unwrap();
    index.save(&mut txn, "doc2", &null_values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify both entries exist
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_unique_index_update() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let old_values = vec![NormalValue::String("alice".to_string())];
    let new_values = vec![NormalValue::String("bob".to_string())];

    index.save(&mut txn, "doc1", &old_values).await.unwrap();
    index
        .update(&mut txn, "doc1", &old_values, &new_values)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify only new entry exists
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let entries = get_entries(txn.as_ref(), &prefix).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, "doc1".as_bytes());
}

#[tokio::test]
async fn test_unique_index_delete() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, "doc1", &values).await.unwrap();
    index.delete(&mut txn, "doc1", &values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify no entries exist
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_composite_index() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, composite_index_description(false));
    let values = vec![
        NormalValue::String("electronics".to_string()),
        NormalValue::Int(1705000000),
    ];

    index.save(&mut txn, "doc1", &values).await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 2);
    let count = count_entries(txn.as_ref(), &prefix).await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_index_type_factory() {
    let simple_desc = test_index_description(false);
    let unique_desc = test_index_description(true);

    let simple_index = IndexType::new(1, simple_desc);
    assert!(!simple_index.description().unique);

    let unique_index = IndexType::new(1, unique_desc);
    assert!(unique_index.description().unique);
}

#[tokio::test]
async fn test_index_sort_order() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, test_index_description(false));

    // Insert in non-sorted order
    index
        .save(
            &mut txn,
            "doc3",
            &[NormalValue::String("charlie".to_string())],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify sorted iteration
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let entries = get_entries(txn.as_ref(), &prefix).await;
    assert_eq!(entries.len(), 3);

    // Keys should be in lexicographic order of encoded values
    // alice < bob < charlie
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys[0] < keys[1], "alice key should be < bob key");
    assert!(keys[1] < keys[2], "bob key should be < charlie key");
}

#[tokio::test]
async fn test_unique_index_update_to_existing_value_fails() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));

    // Save two documents with different values
    index
        .save(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();
    index
        .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();

    // Try to update doc1 to have the same value as doc2 - should fail
    let result = index
        .update(
            &mut txn,
            "doc1",
            &[NormalValue::String("alice".to_string())],
            &[NormalValue::String("bob".to_string())],
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("violates unique index"),
        "error should mention unique index violation: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_composite_index_sort_order() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = SimpleIndex::new(1, composite_index_description(false));

    // Insert documents with same first field, different second field
    // composite_index_description has first field ascending, second descending
    index
        .save(
            &mut txn,
            "doc1",
            &[
                NormalValue::String("electronics".to_string()),
                NormalValue::Int(100),
            ],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc2",
            &[
                NormalValue::String("electronics".to_string()),
                NormalValue::Int(300),
            ],
        )
        .await
        .unwrap();
    index
        .save(
            &mut txn,
            "doc3",
            &[
                NormalValue::String("electronics".to_string()),
                NormalValue::Int(200),
            ],
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify entries
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 2);
    let entries = get_entries(txn.as_ref(), &prefix).await;
    assert_eq!(entries.len(), 3);

    // Since second field is descending, keys should be ordered:
    // electronics/300 < electronics/200 < electronics/100
    // (higher int values should have smaller byte sequences for descending)
    let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert!(
        keys[0] < keys[1] && keys[1] < keys[2],
        "composite index should maintain correct sort order"
    );
}
