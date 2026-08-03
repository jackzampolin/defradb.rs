use super::*;
use crate::backends::MemoryStore;
use crate::corekv::{IterOptions, Store};
use crate::keys::IndexDataStoreKey;
use document::NormalValue;
use schema::{FullTextIndexDescription, IndexedFieldDescription};

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

fn fulltext_index_description() -> schema::IndexDescription {
    schema::IndexDescription {
        id: 3,
        name: "__fulltext__:text".to_string(),
        unique: false,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "text".to_string(),
            descending: false,
        }],
    }
}

fn test_fulltext_index() -> FullTextIndex {
    FullTextIndex::new(
        1,
        fulltext_index_description(),
        FullTextIndexDescription::new("text"),
    )
}

fn legacy_fulltext_stats_key() -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&1u32.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&3u32.to_be_bytes());
    key.extend_from_slice(b"/_stats");
    key
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

    index.save(&mut txn, 1, &values).await.unwrap();
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
    index.save(&mut txn, 1, &values).await.unwrap();
    index.save(&mut txn, 2, &values).await.unwrap();
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

    index.save(&mut txn, 1, &old_values).await.unwrap();
    index
        .update(&mut txn, 1, &old_values, &new_values)
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

    index.save(&mut txn, 1, &values).await.unwrap();
    index.delete(&mut txn, 1, &values).await.unwrap();
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
        .save(&mut txn, 1, &[NormalValue::String("a".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::String("b".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, 3, &[NormalValue::String("c".to_string())])
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

    index.save(&mut txn, 1, &values).await.unwrap();
    txn.commit().await.unwrap();

    // Verify entry exists with the encoded doc short ID in the value
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let entries = get_entries(txn.as_ref(), &prefix).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].1,
        crate::keys::doc_id_index::encode_doc_short_id(1)
    );
}

#[tokio::test]
async fn test_unique_index_rejects_duplicates() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, 1, &values).await.unwrap();

    // Same value, different doc ID - should fail
    let result = index.save(&mut txn, 2, &values).await;
    let error = result.expect_err("duplicate unique value should fail");
    assert!(matches!(
        error,
        crate::corekv::Error::UniqueConstraintViolation
    ));
    assert_eq!(
        error.to_string(),
        crate::corekv::UNIQUE_CONSTRAINT_VIOLATION_MESSAGE
    );
}

#[tokio::test]
async fn test_unique_index_rejects_same_doc_duplicate() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, 1, &values).await.unwrap();

    // Same value, same doc ID - should fail (e.g., JSON array self-duplicates)
    let result = index.save(&mut txn, 1, &values).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_unique_index_null_allows_duplicates() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let null_values = vec![NormalValue::Null];

    // Multiple docs with NULL value - should be allowed
    index.save(&mut txn, 1, &null_values).await.unwrap();
    index.save(&mut txn, 2, &null_values).await.unwrap();
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

    index.save(&mut txn, 1, &old_values).await.unwrap();
    index
        .update(&mut txn, 1, &old_values, &new_values)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify only new entry exists
    let txn = store.new_txn(true).await.unwrap();
    let prefix = IndexDataStoreKey::index_prefix(1, 1);
    let entries = get_entries(txn.as_ref(), &prefix).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].1,
        crate::keys::doc_id_index::encode_doc_short_id(1)
    );
}

#[tokio::test]
async fn test_unique_index_delete() {
    let store = MemoryStore::new();
    let mut txn = store.new_txn(false).await.unwrap();

    let index = UniqueIndex::new(1, test_index_description(true));
    let values = vec![NormalValue::String("alice".to_string())];

    index.save(&mut txn, 1, &values).await.unwrap();
    index.delete(&mut txn, 1, &values).await.unwrap();
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

    index.save(&mut txn, 1, &values).await.unwrap();
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
        .save(&mut txn, 3, &[NormalValue::String("charlie".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, 1, &[NormalValue::String("alice".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::String("bob".to_string())])
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
        .save(&mut txn, 1, &[NormalValue::String("alice".to_string())])
        .await
        .unwrap();
    index
        .save(&mut txn, 2, &[NormalValue::String("bob".to_string())])
        .await
        .unwrap();

    // Try to update doc1 to have the same value as doc2 - should fail
    let result = index
        .update(
            &mut txn,
            1,
            &[NormalValue::String("alice".to_string())],
            &[NormalValue::String("bob".to_string())],
        )
        .await;

    let error = result.expect_err("duplicate unique value should fail");
    assert!(matches!(
        error,
        crate::corekv::Error::UniqueConstraintViolation
    ));
    assert_eq!(
        error.to_string(),
        crate::corekv::UNIQUE_CONSTRAINT_VIOLATION_MESSAGE
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
            1,
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
            2,
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
            3,
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

#[tokio::test]
async fn concurrent_fulltext_saves_use_independent_stats_shards() {
    let store = MemoryStore::new();
    let index = test_fulltext_index();
    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();

    index
        .save(&mut first, 1, &[NormalValue::String("one two".to_string())])
        .await
        .unwrap();
    index
        .save(
            &mut second,
            2,
            &[NormalValue::String("three four five".to_string())],
        )
        .await
        .unwrap();

    first.commit().await.unwrap();
    second.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(index.stats(&txn).await.unwrap(), (2, 2.5));
}

#[tokio::test]
async fn fulltext_stats_preserve_legacy_base_and_apply_deltas() {
    let store = MemoryStore::new();
    let index = test_fulltext_index();
    let legacy_key = legacy_fulltext_stats_key();
    let mut legacy_value = Vec::new();
    legacy_value.extend_from_slice(&2u64.to_be_bytes());
    legacy_value.extend_from_slice(&4u64.to_be_bytes());

    let mut seed = store.new_txn(false).await.unwrap();
    seed.set(&legacy_key, &legacy_value).await.unwrap();
    seed.commit().await.unwrap();

    let mut create = store.new_txn(false).await.unwrap();
    index
        .save(
            &mut create,
            10,
            &[NormalValue::String("new sharded document".to_string())],
        )
        .await
        .unwrap();
    create.commit().await.unwrap();

    let read = store.new_txn(true).await.unwrap();
    assert_eq!(index.stats(&read).await.unwrap(), (3, 7.0 / 3.0));

    let mut update = store.new_txn(false).await.unwrap();
    index
        .update(
            &mut update,
            99,
            &[NormalValue::String("old document".to_string())],
            &[NormalValue::String("updated legacy document".to_string())],
        )
        .await
        .unwrap();
    update.commit().await.unwrap();

    let read = store.new_txn(true).await.unwrap();
    assert_eq!(index.stats(&read).await.unwrap(), (3, 8.0 / 3.0));

    let legacy_value = read.get(&legacy_key).await.unwrap().unwrap();
    assert_eq!(
        u64::from_be_bytes(legacy_value[0..8].try_into().unwrap()),
        2
    );
    assert_eq!(
        u64::from_be_bytes(legacy_value[8..16].try_into().unwrap()),
        4
    );

    let mut delete = store.new_txn(false).await.unwrap();
    index
        .delete(
            &mut delete,
            99,
            &[NormalValue::String("updated legacy document".to_string())],
        )
        .await
        .unwrap();
    delete.commit().await.unwrap();

    let read = store.new_txn(true).await.unwrap();
    assert_eq!(index.stats(&read).await.unwrap(), (2, 2.5));
}

#[tokio::test]
async fn fulltext_scoring_tracks_updates_and_deletes_with_sharded_stats() {
    let store = MemoryStore::new();
    let index = test_fulltext_index();
    let mut create = store.new_txn(false).await.unwrap();
    index
        .save(
            &mut create,
            1,
            &[NormalValue::String("rust database".to_string())],
        )
        .await
        .unwrap();
    index
        .save(
            &mut create,
            2,
            &[NormalValue::String("rust storage engine".to_string())],
        )
        .await
        .unwrap();
    create.commit().await.unwrap();

    let read = store.new_txn(true).await.unwrap();
    assert_eq!(index.search_scored(&read, "rust").await.unwrap().len(), 2);

    let mut mutate = store.new_txn(false).await.unwrap();
    index
        .update(
            &mut mutate,
            1,
            &[NormalValue::String("rust database".to_string())],
            &[NormalValue::String("graph database".to_string())],
        )
        .await
        .unwrap();
    index
        .delete(
            &mut mutate,
            2,
            &[NormalValue::String("rust storage engine".to_string())],
        )
        .await
        .unwrap();
    mutate.commit().await.unwrap();

    let read = store.new_txn(true).await.unwrap();
    assert!(index.search_scored(&read, "rust").await.unwrap().is_empty());
    assert_eq!(index.stats(&read).await.unwrap(), (1, 2.0));
}
