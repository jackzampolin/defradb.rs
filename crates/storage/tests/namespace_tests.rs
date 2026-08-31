//! Tests for Namespace - Store isolation via prefix
//!
//! These tests verify namespace store isolation and iterator scoping.
//! Note: prefix_key and unprefix_key are internal methods tested via
//! the inline unit tests.

use bytes::Bytes;
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::namespace::{Namespace, NamespacedStore};
use storage::RegolithStore;

#[test]
fn test_namespace_prefix() {
    assert_eq!(Namespace::Datastore.prefix(), b'd');
    assert_eq!(Namespace::Blockstore.prefix(), b'b');
    assert_eq!(Namespace::Headstore.prefix(), b'h');
    assert_eq!(Namespace::Systemstore.prefix(), b's');
    assert_eq!(Namespace::Peerstore.prefix(), b'p');
    assert_eq!(Namespace::Encstore.prefix(), b'e');
    assert_eq!(Namespace::Acpstore.prefix(), b'a');
}

#[test]
fn test_namespace_name() {
    assert_eq!(Namespace::Datastore.name(), "datastore");
    assert_eq!(Namespace::Blockstore.name(), "blockstore");
    assert_eq!(Namespace::Headstore.name(), "headstore");
    assert_eq!(Namespace::Systemstore.name(), "systemstore");
    assert_eq!(Namespace::Peerstore.name(), "peerstore");
    assert_eq!(Namespace::Encstore.name(), "encstore");
    assert_eq!(Namespace::Acpstore.name(), "acpstore");
}

#[tokio::test]
async fn test_namespaced_store_isolation() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());

    let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);
    let blockstore = NamespacedStore::new(store.clone(), Namespace::Blockstore);

    // Write to datastore
    let mut txn = datastore.new_txn(false).await.unwrap();
    txn.set(b"key1", b"value1").await.unwrap();
    txn.commit().await.unwrap();

    // Write to blockstore with same key
    let mut txn = blockstore.new_txn(false).await.unwrap();
    txn.set(b"key1", b"value2").await.unwrap();
    txn.commit().await.unwrap();

    // Read from datastore - should get value1
    let txn = datastore.new_txn(true).await.unwrap();
    let value = txn.get(b"key1").await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value1")));

    // Read from blockstore - should get value2
    let txn = blockstore.new_txn(true).await.unwrap();
    let value = txn.get(b"key1").await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value2")));

    // Keys are isolated - blockstore shouldn't see datastore key
    let txn = blockstore.new_txn(true).await.unwrap();
    let has_key = txn.has(b"key1").await.unwrap();
    assert!(has_key); // Should have its own key1

    // But the actual values are different
    let value = txn.get(b"key1").await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value2")));
    assert_ne!(value, Some(Bytes::from_static(b"value1")));
}

#[tokio::test]
async fn test_namespaced_iterator() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);

    // Write multiple keys
    let mut txn = datastore.new_txn(false).await.unwrap();
    txn.set(b"key1", b"value1").await.unwrap();
    txn.set(b"key2", b"value2").await.unwrap();
    txn.set(b"key3", b"value3").await.unwrap();
    txn.commit().await.unwrap();

    // Iterate
    let txn = datastore.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::default()).await.unwrap();

    let mut count = 0;
    while let Some(pair) = iter.next().await.unwrap() {
        // Keys should not have the 'd' prefix
        assert_ne!(pair.key[0], b'd');
        assert!(pair.key.starts_with(b"key"));
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_namespace_prefix_iteration() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);

    // Write keys with common prefix
    let mut txn = datastore.new_txn(false).await.unwrap();
    txn.set(b"user/1", b"alice").await.unwrap();
    txn.set(b"user/2", b"bob").await.unwrap();
    txn.set(b"post/1", b"hello").await.unwrap();
    txn.commit().await.unwrap();

    // Iterate with prefix
    let txn = datastore.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"user/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut count = 0;
    while let Some(pair) = iter.next().await.unwrap() {
        assert!(pair.key.starts_with(b"user/"));
        count += 1;
    }
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_namespace_no_prefix_collision() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());

    // Create a key in datastore that starts with 'b' (blockstore prefix)
    // This tests that namespace isolation prevents cross-namespace access
    let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);
    let mut txn = datastore.new_txn(false).await.unwrap();
    txn.set(b"bmalicious_key", b"datastore_value")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Blockstore should NOT see this key, even though the key starts with 'b'
    let blockstore = NamespacedStore::new(store.clone(), Namespace::Blockstore);
    let txn = blockstore.new_txn(true).await.unwrap();

    // The key "malicious_key" should not exist in blockstore
    // (because the actual stored key is "d" + "bmalicious_key", not "b" + "malicious_key")
    let value = txn.get(b"malicious_key").await.unwrap();
    assert_eq!(value, None, "Blockstore should not see datastore key");

    // Also check the key with 'b' prefix doesn't exist in blockstore
    let value = txn.get(b"bmalicious_key").await.unwrap();
    assert_eq!(
        value, None,
        "Blockstore should not see key starting with 'b' from datastore"
    );
}

#[tokio::test]
async fn test_namespace_default_prefix_scoping() {
    // Test that iterating with no prefix still stays within namespace
    let store = Arc::new(RegolithStore::in_memory().unwrap());

    // Write to multiple namespaces
    let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);
    let blockstore = NamespacedStore::new(store.clone(), Namespace::Blockstore);

    let mut txn = datastore.new_txn(false).await.unwrap();
    txn.set(b"ds_key1", b"ds_value1").await.unwrap();
    txn.set(b"ds_key2", b"ds_value2").await.unwrap();
    txn.commit().await.unwrap();

    let mut txn = blockstore.new_txn(false).await.unwrap();
    txn.set(b"bs_key1", b"bs_value1").await.unwrap();
    txn.commit().await.unwrap();

    // Iterate datastore with no prefix - should only see datastore keys
    let txn = datastore.new_txn(true).await.unwrap();
    let opts = IterOptions::default(); // No prefix, start, or end
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut ds_keys: Vec<String> = vec![];
    while let Some(pair) = iter.next().await.unwrap() {
        ds_keys.push(pair.key_str());
    }
    drop(txn);

    // Should only see datastore keys, not blockstore keys
    assert_eq!(ds_keys.len(), 2);
    assert!(ds_keys.contains(&"ds_key1".to_string()));
    assert!(ds_keys.contains(&"ds_key2".to_string()));
    assert!(!ds_keys.contains(&"bs_key1".to_string()));

    // Similarly, blockstore iteration should only see blockstore keys
    let txn = blockstore.new_txn(true).await.unwrap();
    let opts = IterOptions::default();
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut bs_keys: Vec<String> = vec![];
    while let Some(pair) = iter.next().await.unwrap() {
        bs_keys.push(pair.key_str());
    }

    assert_eq!(bs_keys.len(), 1);
    assert!(bs_keys.contains(&"bs_key1".to_string()));
}
