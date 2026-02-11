use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_redb_data_survives_close_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.redb");

    // Write data and close
    {
        let store = RedbStore::open(&path).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"persistent_key", b"persistent_value")
            .await
            .unwrap();
        txn.commit().await.unwrap();
        store.close().await.unwrap();
    }

    // Reopen and verify
    {
        let store = RedbStore::open(&path).unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"persistent_key").await.unwrap(),
            Some(b"persistent_value".to_vec()),
            "Data should survive close/reopen"
        );
    }
}

#[tokio::test]
async fn test_redb_uncommitted_data_lost_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.redb");

    // Write data but DON'T commit
    {
        let store = RedbStore::open(&path).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"uncommitted_key", b"value").await.unwrap();
        // No commit! Discard.
        txn.discard();
        store.close().await.unwrap();
    }

    // Reopen - uncommitted data should be gone
    {
        let store = RedbStore::open(&path).unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"uncommitted_key").await.unwrap(),
            None,
            "Uncommitted data should not survive reopen"
        );
    }
}

#[tokio::test]
async fn test_redb_persistence_through_multiple_sessions() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.redb");

    // Session 1: Write keys
    {
        let store = RedbStore::open(&path).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.commit().await.unwrap();
    }

    // Session 2: Modify and add
    {
        let store = RedbStore::open(&path).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key1", b"modified").await.unwrap();
        txn.set(b"key3", b"value3").await.unwrap();
        txn.delete(b"key2").await.unwrap();
        txn.commit().await.unwrap();
    }

    // Session 3: Verify all changes
    {
        let store = RedbStore::open(&path).unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"modified".to_vec()));
        assert_eq!(txn.get(b"key2").await.unwrap(), None);
        assert_eq!(txn.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
    }
}
