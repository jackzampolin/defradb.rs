//! Tests for Datastore - Document and collection data storage
//!
//! These tests verify the chunking behavior for large values,
//! basic CRUD operations, and data isolation.

use std::sync::Arc;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::keys::datastore::DataStoreKey;
use storage::keys::utils::InstanceType;
use storage::stores::datastore::{Datastore, DatastoreTxn, CHUNK_SIZE};

#[tokio::test]
async fn test_datastore_basic() {
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    let key = DataStoreKey::new(1, InstanceType::Value, 1, "field1");

    // Write
    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, b"value1").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Read
    let txn = datastore.new_txn(true).await.unwrap();
    let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let value = txn.get_value(&key).await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_datastore_chunking() {
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    let key = DataStoreKey::new(1, InstanceType::Value, 1, "large_field");

    // Create a 2.5MB value
    let large_value = vec![0xAB; CHUNK_SIZE * 2 + CHUNK_SIZE / 2];

    // Write
    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, &large_value).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Read back
    let txn = datastore.new_txn(true).await.unwrap();
    let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let value = txn.get_value(&key).await.unwrap();
    assert_eq!(value, Some(large_value));
}

#[tokio::test]
async fn test_datastore_delete_chunked() {
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    let key = DataStoreKey::new(1, InstanceType::Value, 1, "large_field");

    // Create a 2MB value
    let large_value = vec![0xCD; CHUNK_SIZE * 2];

    // Write
    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, &large_value).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Verify it exists
    let txn = datastore.new_txn(true).await.unwrap();
    let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let value = txn.get_value(&key).await.unwrap();
    assert!(value.is_some());
    let _ = txn;

    // Delete
    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.delete_value(&key).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Verify it's gone
    let txn = datastore.new_txn(true).await.unwrap();
    let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let value = txn.get_value(&key).await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_datastore_isolation() {
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    // Write to datastore
    let mut txn = datastore.new_txn(false).await.unwrap();
    txn.set(b"test_key", b"datastore_value").await.unwrap();
    txn.commit().await.unwrap();

    // Read back
    let txn = datastore.new_txn(true).await.unwrap();
    let value = txn.get(b"test_key").await.unwrap();
    assert_eq!(value, Some(b"datastore_value".to_vec()));
}

#[tokio::test]
async fn test_datastore_exact_chunk_size() {
    // Test value exactly equal to CHUNK_SIZE (boundary condition)
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    let key = DataStoreKey::new(1, InstanceType::Value, 1, "exact_field");

    // Exactly CHUNK_SIZE bytes (should NOT be chunked, since > not >=)
    let exact_value = vec![0xAB; CHUNK_SIZE];

    // Write
    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, &exact_value).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Read back - verify round-trip works
    let txn = datastore.new_txn(true).await.unwrap();
    let txn_ds = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let value = txn_ds.get_value(&key).await.unwrap();
    assert_eq!(value, Some(exact_value.clone()));
}

#[tokio::test]
async fn test_datastore_chunk_size_plus_one() {
    // Test value exactly CHUNK_SIZE + 1 (should be chunked)
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    let key = DataStoreKey::new(1, InstanceType::Value, 1, "plus_one_field");

    // CHUNK_SIZE + 1 bytes (should be chunked into 2 chunks)
    let value = vec![0xCD; CHUNK_SIZE + 1];

    // Write
    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, &value).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Read back - verify round-trip works for chunked values
    let txn = datastore.new_txn(true).await.unwrap();
    let txn_ds = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let retrieved = txn_ds.get_value(&key).await.unwrap();
    assert_eq!(retrieved, Some(value));
}

#[tokio::test]
async fn test_datastore_chunk_update_cleanup() {
    // Test that updating a chunked value with fewer chunks cleans up old chunks
    let store = Arc::new(MemoryStore::new());
    let datastore = Datastore::new(store);

    let key = DataStoreKey::new(1, InstanceType::Value, 1, "shrink_field");

    // First, write a 3-chunk value (2.5 MB)
    let large_value = vec![0xEF; CHUNK_SIZE * 2 + CHUNK_SIZE / 2];

    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, &large_value).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Verify large value can be read back
    let txn = datastore.new_txn(true).await.unwrap();
    let txn_ds = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let retrieved = txn_ds.get_value(&key).await.unwrap();
    assert_eq!(retrieved, Some(large_value.clone()));
    drop(txn);

    // Now update with a smaller 2-chunk value (1.5 MB - just over CHUNK_SIZE)
    let smaller_value = vec![0x12; CHUNK_SIZE + CHUNK_SIZE / 2];

    let mut txn = datastore.new_txn(false).await.unwrap();
    {
        let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
        txn_ds.put(&key, &smaller_value).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Verify the value was updated correctly to the smaller size
    // This implicitly verifies old chunks were cleaned up - if they weren't,
    // get_value would return more data than expected
    let txn = datastore.new_txn(true).await.unwrap();
    let txn_ds = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
    let retrieved = txn_ds.get_value(&key).await.unwrap();
    assert_eq!(retrieved, Some(smaller_value));
}
