//! Tests for Multistore - Coordinator for all specialized stores
//!
//! These tests verify namespace isolation, prefix stripping, and
//! correct coordination between all specialized stores.

use cid::Cid;
use std::str::FromStr;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::{
    blockstore::BlockstoreKey, datastore::DataStoreKey, headstore::HeadstoreDocKey,
    peerstore::ReplicatorKey, systemstore::CollectionKey, utils::InstanceType,
};
use storage::stores::multistore::MemoryMultistore;

#[tokio::test]
async fn test_multistore_creation() {
    let ms = MemoryMultistore::new_memory();
    assert!(ms.close().await.is_ok());
}

#[tokio::test]
async fn test_multistore_all_stores_isolated() {
    let ms = MemoryMultistore::new_memory();

    // Write to each store with same logical key
    // Datastore
    let ds_key = DataStoreKey::new(1, InstanceType::Value, "doc1", "field");
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(&ds_key.bytes(), b"datastore_value").await.unwrap();
    txn.commit().await.unwrap();

    // Blockstore
    let cid =
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let bs_key = BlockstoreKey::new(cid);
    let mut txn = ms.blockstore.new_txn(false).await.unwrap();
    txn.set(&bs_key.bytes(), b"blockstore_value").await.unwrap();
    txn.commit().await.unwrap();

    // Headstore
    let hs_key = HeadstoreDocKey::new("doc1", "field", cid);
    let mut txn = ms.headstore.new_txn(false).await.unwrap();
    txn.set(&hs_key.bytes(), b"headstore_value").await.unwrap();
    txn.commit().await.unwrap();

    // Systemstore
    let ss_key = CollectionKey::new("users");
    let mut txn = ms.systemstore.new_txn(false).await.unwrap();
    txn.set(&ss_key.bytes(), b"systemstore_value")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Peerstore
    let ps_key = ReplicatorKey::new("rep1");
    let mut txn = ms.peerstore.new_txn(false).await.unwrap();
    txn.set(&ps_key.bytes(), b"peerstore_value").await.unwrap();
    txn.commit().await.unwrap();

    // Verify each store has its own value
    let txn = ms.datastore.new_txn(true).await.unwrap();
    let val = txn.get(&ds_key.bytes()).await.unwrap();
    assert_eq!(val, Some(b"datastore_value".to_vec()));

    let txn = ms.blockstore.new_txn(true).await.unwrap();
    let val = txn.get(&bs_key.bytes()).await.unwrap();
    assert_eq!(val, Some(b"blockstore_value".to_vec()));

    let txn = ms.headstore.new_txn(true).await.unwrap();
    let val = txn.get(&hs_key.bytes()).await.unwrap();
    assert_eq!(val, Some(b"headstore_value".to_vec()));

    let txn = ms.systemstore.new_txn(true).await.unwrap();
    let val = txn.get(&ss_key.bytes()).await.unwrap();
    assert_eq!(val, Some(b"systemstore_value".to_vec()));

    let txn = ms.peerstore.new_txn(true).await.unwrap();
    let val = txn.get(&ps_key.bytes()).await.unwrap();
    assert_eq!(val, Some(b"peerstore_value".to_vec()));
}

#[tokio::test]
async fn test_multistore_rootstore_sees_all() {
    let ms = MemoryMultistore::new_memory();

    // Write to datastore (namespace 'd')
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"key1", b"value1").await.unwrap();
    txn.commit().await.unwrap();

    // Read from rootstore with full prefixed key
    let txn = ms.root.new_txn(true).await.unwrap();
    let value = txn.get(b"dkey1").await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}

// =========================================================================
// NAMESPACE ISOLATION TESTS - Critical for data integrity
// =========================================================================

#[tokio::test]
async fn test_multistore_stores_cannot_see_each_others_data() {
    let ms = MemoryMultistore::new_memory();

    // Write to datastore
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"shared_key", b"datastore_value").await.unwrap();
    txn.commit().await.unwrap();

    // Blockstore should NOT see the key (different namespace)
    let txn = ms.blockstore.new_txn(true).await.unwrap();
    let value = txn.get(b"shared_key").await.unwrap();
    assert_eq!(value, None, "Blockstore should not see datastore's data");

    // Headstore should NOT see it either
    let txn = ms.headstore.new_txn(true).await.unwrap();
    let value = txn.get(b"shared_key").await.unwrap();
    assert_eq!(value, None, "Headstore should not see datastore's data");

    // Systemstore should NOT see it
    let txn = ms.systemstore.new_txn(true).await.unwrap();
    let value = txn.get(b"shared_key").await.unwrap();
    assert_eq!(value, None, "Systemstore should not see datastore's data");

    // Peerstore should NOT see it
    let txn = ms.peerstore.new_txn(true).await.unwrap();
    let value = txn.get(b"shared_key").await.unwrap();
    assert_eq!(value, None, "Peerstore should not see datastore's data");
}

#[tokio::test]
async fn test_multistore_same_key_different_values() {
    // Each store can have the same key with different values
    let ms = MemoryMultistore::new_memory();

    // Write same key to multiple stores with different values
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"key", b"datastore").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = ms.systemstore.new_txn(false).await.unwrap();
    txn.set(b"key", b"systemstore").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = ms.peerstore.new_txn(false).await.unwrap();
    txn.set(b"key", b"peerstore").await.unwrap();
    txn.commit().await.unwrap();

    // Each store should have its own value
    let txn = ms.datastore.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), Some(b"datastore".to_vec()));

    let txn = ms.systemstore.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"key").await.unwrap(),
        Some(b"systemstore".to_vec())
    );

    let txn = ms.peerstore.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), Some(b"peerstore".to_vec()));
}

#[tokio::test]
async fn test_multistore_raw_prefix_collision_prevented() {
    // Test that writing raw bytes that happen to match another namespace's prefix
    // doesn't cause data to leak between stores
    let ms = MemoryMultistore::new_memory();

    // Datastore uses prefix 'd', so let's write a key that starts with 'b'
    // (blockstore prefix) to datastore
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"bfake_block", b"not_a_real_block").await.unwrap();
    txn.commit().await.unwrap();

    // Blockstore should NOT see this key because the namespace prefix
    // is added BEFORE the key
    let txn = ms.blockstore.new_txn(true).await.unwrap();
    let value = txn.get(b"bfake_block").await.unwrap();
    assert_eq!(
        value, None,
        "Blockstore should not see datastore key even if key starts with 'b'"
    );

    // The key should be at "d" + "bfake_block" in root
    let txn = ms.root.new_txn(true).await.unwrap();
    let value = txn.get(b"dbfake_block").await.unwrap();
    assert_eq!(
        value,
        Some(b"not_a_real_block".to_vec()),
        "Key should be prefixed with datastore namespace"
    );
}

#[tokio::test]
async fn test_multistore_iterator_isolation() {
    // Test that iterators only see data from their own namespace
    let ms = MemoryMultistore::new_memory();

    // Write to multiple stores
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"d_key1", b"value1").await.unwrap();
    txn.set(b"d_key2", b"value2").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = ms.blockstore.new_txn(false).await.unwrap();
    txn.set(b"b_key1", b"block1").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = ms.systemstore.new_txn(false).await.unwrap();
    txn.set(b"s_key1", b"system1").await.unwrap();
    txn.commit().await.unwrap();

    // Iterate over datastore - should only see datastore keys
    let txn = ms.datastore.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut ds_keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        ds_keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }
    assert_eq!(ds_keys.len(), 2);
    assert!(ds_keys.contains(&"d_key1".to_string()));
    assert!(ds_keys.contains(&"d_key2".to_string()));

    // Iterate over blockstore - should only see blockstore keys
    let txn = ms.blockstore.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut bs_keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        bs_keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }
    assert_eq!(bs_keys.len(), 1);
    assert!(bs_keys.contains(&"b_key1".to_string()));

    // Iterate over systemstore - should only see systemstore keys
    let txn = ms.systemstore.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut ss_keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        ss_keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }
    assert_eq!(ss_keys.len(), 1);
    assert!(ss_keys.contains(&"s_key1".to_string()));
}

#[tokio::test]
async fn test_multistore_delete_isolation() {
    // Test that deleting from one store doesn't affect others
    let ms = MemoryMultistore::new_memory();

    // Write same key to multiple stores
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"key", b"datastore").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = ms.systemstore.new_txn(false).await.unwrap();
    txn.set(b"key", b"systemstore").await.unwrap();
    txn.commit().await.unwrap();

    // Delete from datastore only
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.delete(b"key").await.unwrap();
    txn.commit().await.unwrap();

    // Datastore should not have the key
    let txn = ms.datastore.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), None);

    // Systemstore should still have its key
    let txn = ms.systemstore.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"key").await.unwrap(),
        Some(b"systemstore".to_vec()),
        "Systemstore should be unaffected by datastore delete"
    );
}

#[tokio::test]
async fn test_multistore_encstore_separate_from_blockstore() {
    // Encstore uses blockstore implementation but different namespace
    let ms = MemoryMultistore::new_memory();

    // Write to blockstore
    let mut txn = ms.blockstore.new_txn(false).await.unwrap();
    txn.set(b"block", b"blockstore_data").await.unwrap();
    txn.commit().await.unwrap();

    // Write to encstore
    let mut txn = ms.encstore.new_txn(false).await.unwrap();
    txn.set(b"block", b"encstore_data").await.unwrap();
    txn.commit().await.unwrap();

    // Each should have its own data
    let txn = ms.blockstore.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"block").await.unwrap(),
        Some(b"blockstore_data".to_vec())
    );

    let txn = ms.encstore.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"block").await.unwrap(),
        Some(b"encstore_data".to_vec())
    );
}

// =========================================================================
// NAMESPACE PREFIX STRIPPING TESTS
// Verify that namespace prefixes are correctly stripped from iterator keys
// =========================================================================

#[tokio::test]
async fn test_namespace_iterator_strips_prefix() {
    // This test verifies that when iterating within a namespace,
    // the returned keys have the namespace prefix removed
    let ms = MemoryMultistore::new_memory();

    // Write keys to datastore (namespace 'd')
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"key1", b"value1").await.unwrap();
    txn.set(b"key2", b"value2").await.unwrap();
    txn.set(b"key3", b"value3").await.unwrap();
    txn.commit().await.unwrap();

    // Iterate over datastore - keys should NOT have 'd' prefix
    let txn = ms.datastore.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        let key = String::from_utf8_lossy(kv.key_bytes()).to_string();
        keys.push(key.clone());
        // Verify no namespace prefix
        assert!(
            !key.starts_with("d"),
            "Key '{}' should not start with namespace prefix 'd'",
            key
        );
    }

    assert_eq!(keys, vec!["key1", "key2", "key3"]);
}

#[tokio::test]
async fn test_namespace_iterator_strips_prefix_with_user_prefix() {
    // Test that when user specifies a prefix, the returned keys
    // are relative to that prefix (namespace still stripped)
    let ms = MemoryMultistore::new_memory();

    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"users/alice", b"data1").await.unwrap();
    txn.set(b"users/bob", b"data2").await.unwrap();
    txn.set(b"posts/hello", b"data3").await.unwrap();
    txn.commit().await.unwrap();

    // Iterate with prefix "users/"
    let txn = ms.datastore.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"users/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        let key = String::from_utf8_lossy(kv.key_bytes()).to_string();
        keys.push(key);
    }

    // Should get "users/alice" and "users/bob" without namespace prefix
    assert_eq!(keys, vec!["users/alice", "users/bob"]);
}

#[tokio::test]
async fn test_namespace_iterator_reverse_strips_prefix() {
    let ms = MemoryMultistore::new_memory();

    let mut txn = ms.systemstore.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    // Reverse iteration
    let txn = ms.systemstore.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        let key = String::from_utf8_lossy(kv.key_bytes()).to_string();
        // Verify no 's' (systemstore) prefix
        assert!(
            !key.starts_with("s"),
            "Reverse iterator key should not have namespace prefix"
        );
        keys.push(key);
    }

    assert_eq!(keys, vec!["c", "b", "a"]);
}

#[tokio::test]
async fn test_rootstore_iterator_shows_full_keys() {
    // Rootstore has no namespace, so it should show the raw keys
    // including other stores' namespace prefixes
    let ms = MemoryMultistore::new_memory();

    // Write to datastore (prefix 'd')
    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"mykey", b"value").await.unwrap();
    txn.commit().await.unwrap();

    // Write to systemstore (prefix 's')
    let mut txn = ms.systemstore.new_txn(false).await.unwrap();
    txn.set(b"mykey", b"value").await.unwrap();
    txn.commit().await.unwrap();

    // Rootstore should see both with their prefixes
    let txn = ms.root.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    // Should see "dmykey" and "smykey" (with namespace prefixes)
    assert!(
        keys.contains(&"dmykey".to_string()),
        "Should see datastore key with 'd' prefix"
    );
    assert!(
        keys.contains(&"smykey".to_string()),
        "Should see systemstore key with 's' prefix"
    );
}

#[tokio::test]
async fn test_get_size_through_namespace() {
    let ms = MemoryMultistore::new_memory();

    let mut txn = ms.datastore.new_txn(false).await.unwrap();
    txn.set(b"sized_key", b"12345").await.unwrap();
    txn.commit().await.unwrap();

    // get_size should work through namespace
    let txn = ms.datastore.new_txn(true).await.unwrap();
    assert_eq!(txn.get_size(b"sized_key").await.unwrap(), Some(5));
    assert_eq!(txn.get_size(b"nonexistent").await.unwrap(), None);
}
