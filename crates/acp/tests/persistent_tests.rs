#![cfg(feature = "redb")]

//! Tests for PersistentAcpStore backed by redb.

use acp::{AcpStore, PersistentAcpStore, RelationTuple};
use identity::Did;
use std::sync::Arc;
use storage::corekv::{IterOptions, Reader, Store, Writer};
use storage::namespace::{Namespace, NamespacedStore};
use tempfile::TempDir;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
}

#[tokio::test]
async fn test_persistent_store_put_and_has() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    assert!(!store.has_tuple(&tuple).await.unwrap());
    store.put_tuple(&tuple).await.unwrap();
    assert!(store.has_tuple(&tuple).await.unwrap());
}

#[tokio::test]
async fn test_persistent_store_delete() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    store.put_tuple(&tuple).await.unwrap();
    assert!(store.has_tuple(&tuple).await.unwrap());

    store.delete_tuple(&tuple).await.unwrap();
    assert!(!store.has_tuple(&tuple).await.unwrap());
}

#[tokio::test]
async fn test_persistent_store_get_doc_tuples() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    let did1 = test_did();
    let did2 = test_did2();

    let tuple1 =
        RelationTuple::try_new(did1.clone(), "owner", "users", "doc1").expect("valid tuple");
    let tuple2 =
        RelationTuple::try_new(did2.clone(), "reader", "users", "doc1").expect("valid tuple");
    let tuple3 =
        RelationTuple::try_new(did1.clone(), "owner", "users", "doc2").expect("valid tuple");

    store.put_tuple(&tuple1).await.unwrap();
    store.put_tuple(&tuple2).await.unwrap();
    store.put_tuple(&tuple3).await.unwrap();

    let doc1_tuples = store.get_doc_tuples("users", "doc1").await.unwrap();
    assert_eq!(doc1_tuples.len(), 2);

    let doc2_tuples = store.get_doc_tuples("users", "doc2").await.unwrap();
    assert_eq!(doc2_tuples.len(), 1);
}

#[tokio::test]
async fn test_persistent_store_is_doc_registered() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    assert!(!store.is_doc_registered("users", "doc1").await.unwrap());
    store.put_tuple(&tuple).await.unwrap();
    assert!(store.is_doc_registered("users", "doc1").await.unwrap());
}

#[tokio::test]
async fn test_persistent_store_delete_doc_tuples() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    let did1 = test_did();
    let did2 = test_did2();

    let tuple1 =
        RelationTuple::try_new(did1.clone(), "owner", "users", "doc1").expect("valid tuple");
    let tuple2 =
        RelationTuple::try_new(did2.clone(), "reader", "users", "doc1").expect("valid tuple");

    store.put_tuple(&tuple1).await.unwrap();
    store.put_tuple(&tuple2).await.unwrap();
    assert!(store.is_doc_registered("users", "doc1").await.unwrap());

    store.delete_doc_tuples("users", "doc1").await.unwrap();
    assert!(!store.is_doc_registered("users", "doc1").await.unwrap());
}

#[tokio::test]
async fn test_persistent_store_survives_reopen() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path().to_path_buf();

    // Create store and write data
    {
        let store = PersistentAcpStore::open(&path).unwrap();
        let tuple =
            RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");
        store.put_tuple(&tuple).await.unwrap();
        store.close().await.unwrap();
    }

    // Reopen and verify data persisted
    {
        let store = PersistentAcpStore::open(&path).unwrap();
        let tuple =
            RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");
        assert!(
            store.has_tuple(&tuple).await.unwrap(),
            "tuple should persist across store reopen"
        );
    }
}

#[tokio::test]
async fn test_persistent_store_validates_prefix() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    // Path traversal attempts should be rejected
    let result = store.get_doc_tuples("../etc", "passwd").await;
    assert!(result.is_err(), "path traversal should be rejected");

    let result = store
        .get_doc_tuples("users", "doc/../../../etc/passwd")
        .await;
    assert!(result.is_err(), "path traversal should be rejected");
}

#[tokio::test]
async fn test_persistent_store_unicode_identifiers() {
    let tmp_dir = TempDir::new().unwrap();
    let store = PersistentAcpStore::open(tmp_dir.path()).unwrap();

    // Unicode characters in collection_id and doc_id should work
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "文档1").expect("valid tuple");
    store.put_tuple(&tuple).await.unwrap();
    assert!(
        store.has_tuple(&tuple).await.unwrap(),
        "should store tuple with unicode identifiers"
    );

    // Verify retrieval works
    let tuples = store.get_doc_tuples("users", "文档1").await.unwrap();
    assert_eq!(tuples.len(), 1);
    assert!(store.is_doc_registered("users", "文档1").await.unwrap());

    // Cleanup works
    store.delete_doc_tuples("users", "文档1").await.unwrap();
    assert!(!store.is_doc_registered("users", "文档1").await.unwrap());
}

// ============================================================================
// Unified Mode Tests (ACP sharing main database with namespace isolation)
// ============================================================================

#[tokio::test]
async fn test_unified_store_basic_operations() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("data");

    // Create main redb store (simulating the main database)
    let redb_store = Arc::new(storage::RedbStore::open(&db_path).unwrap());

    // Create ACP store from main database using unified mode
    let acp_store = PersistentAcpStore::from_store(redb_store.clone());

    // Verify basic ACP operations work
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    assert!(!acp_store.has_tuple(&tuple).await.unwrap());
    acp_store.put_tuple(&tuple).await.unwrap();
    assert!(acp_store.has_tuple(&tuple).await.unwrap());

    // Verify document registration
    assert!(acp_store.is_doc_registered("users", "doc1").await.unwrap());

    // Cleanup
    acp_store.delete_tuple(&tuple).await.unwrap();
    assert!(!acp_store.has_tuple(&tuple).await.unwrap());
}

#[tokio::test]
async fn test_unified_store_namespace_isolation() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("data");

    // Create main redb store
    let redb_store = Arc::new(storage::RedbStore::open(&db_path).unwrap());

    // Create ACP store from main database
    let acp_store = PersistentAcpStore::from_store(redb_store.clone());

    // Write an ACP tuple
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");
    acp_store.put_tuple(&tuple).await.unwrap();

    // Write some non-ACP data directly to the main store
    // This simulates other stores (datastore, blockstore, etc.) writing data
    {
        let mut txn = redb_store.new_txn(false).await.unwrap();
        txn.set(b"d/collection/doc", b"document data")
            .await
            .unwrap();
        txn.set(b"b/block/cid", b"block data").await.unwrap();
        txn.commit().await.unwrap();
    }

    // Verify ACP data is still accessible and correct
    assert!(acp_store.has_tuple(&tuple).await.unwrap());

    // Verify the raw store contains both ACP (with 'a' prefix) and other data
    {
        let txn = redb_store.new_txn(true).await.unwrap();

        // Non-ACP data should be accessible directly
        assert!(txn.has(b"d/collection/doc").await.unwrap());
        assert!(txn.has(b"b/block/cid").await.unwrap());

        // ACP data should be prefixed with 'a' namespace byte
        let acp_key = tuple.storage_key();
        let prefixed_key = format!("a{}", acp_key);
        assert!(
            txn.has(prefixed_key.as_bytes()).await.unwrap(),
            "ACP data should be stored with 'a' namespace prefix"
        );
    }
}

#[tokio::test]
async fn test_unified_store_multiple_documents() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("data");

    let redb_store = Arc::new(storage::RedbStore::open(&db_path).unwrap());
    let acp_store = PersistentAcpStore::from_store(redb_store.clone());

    let did1 = test_did();
    let did2 = test_did2();

    // Register multiple documents with different permissions
    let tuple1 =
        RelationTuple::try_new(did1.clone(), "owner", "users", "doc1").expect("valid tuple");
    let tuple2 =
        RelationTuple::try_new(did2.clone(), "reader", "users", "doc1").expect("valid tuple");
    let tuple3 =
        RelationTuple::try_new(did1.clone(), "owner", "posts", "post1").expect("valid tuple");

    acp_store.put_tuple(&tuple1).await.unwrap();
    acp_store.put_tuple(&tuple2).await.unwrap();
    acp_store.put_tuple(&tuple3).await.unwrap();

    // Verify document lookups work correctly
    let doc1_tuples = acp_store.get_doc_tuples("users", "doc1").await.unwrap();
    assert_eq!(doc1_tuples.len(), 2);

    let post1_tuples = acp_store.get_doc_tuples("posts", "post1").await.unwrap();
    assert_eq!(post1_tuples.len(), 1);

    // Verify both documents are registered
    assert!(acp_store.is_doc_registered("users", "doc1").await.unwrap());
    assert!(acp_store.is_doc_registered("posts", "post1").await.unwrap());
    assert!(!acp_store
        .is_doc_registered("users", "nonexistent")
        .await
        .unwrap());
}

#[tokio::test]
async fn test_unified_store_atomic_registration() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("data");

    let redb_store = Arc::new(storage::RedbStore::open(&db_path).unwrap());
    let acp_store = PersistentAcpStore::from_store(redb_store.clone());

    let owner = test_did();

    // First registration should succeed
    let registered = acp_store
        .register_doc_atomic(&owner, "users", "doc1")
        .await
        .unwrap();
    assert!(registered, "first registration should succeed");

    // Second registration should fail (document already registered)
    let registered_again = acp_store
        .register_doc_atomic(&owner, "users", "doc1")
        .await
        .unwrap();
    assert!(
        !registered_again,
        "second registration should fail - doc already registered"
    );

    // Document should be registered with the original owner
    assert!(acp_store.is_doc_registered("users", "doc1").await.unwrap());
}

#[tokio::test]
async fn test_unified_store_atomic_registration_concurrent() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("data");

    let redb_store = Arc::new(storage::RedbStore::open(&db_path).unwrap());
    let acp_store = Arc::new(PersistentAcpStore::from_store(redb_store.clone()));

    let owner1 = test_did();
    let owner2 = test_did2();

    // Spawn concurrent registration attempts
    let store1 = acp_store.clone();
    let o1 = owner1.clone();
    let h1 = tokio::spawn(async move { store1.register_doc_atomic(&o1, "users", "doc1").await });

    let store2 = acp_store.clone();
    let o2 = owner2.clone();
    let h2 = tokio::spawn(async move { store2.register_doc_atomic(&o2, "users", "doc1").await });

    let (r1, r2) = tokio::join!(h1, h2);
    let success1 = r1.unwrap().unwrap();
    let success2 = r2.unwrap().unwrap();

    // Exactly one should succeed
    assert!(
        (success1 && !success2) || (!success1 && success2),
        "Exactly one concurrent registration must succeed, got: r1={}, r2={}",
        success1,
        success2
    );

    // Document should be registered
    assert!(acp_store.is_doc_registered("users", "doc1").await.unwrap());
}

#[tokio::test]
async fn test_unified_store_isolation_from_namespaced_stores() {
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("data");

    let redb_store = Arc::new(storage::RedbStore::open(&db_path).unwrap());

    // Create both ACP and Datastore using the NamespacedStore pattern
    let acp_store = PersistentAcpStore::from_store(redb_store.clone());
    let datastore = NamespacedStore::new(redb_store.clone(), Namespace::Datastore);

    // Write ACP data
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");
    acp_store.put_tuple(&tuple).await.unwrap();

    // Write datastore data
    {
        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"collection/doc", b"document data").await.unwrap();
        txn.commit().await.unwrap();
    }

    // Datastore iteration should NOT see ACP data
    {
        let txn = datastore.new_txn(true).await.unwrap();
        let opts = IterOptions::default();
        let mut iter = txn.iterator(opts).await.unwrap();

        while let Some(pair) = iter.next().await.unwrap() {
            let key_str = String::from_utf8_lossy(&pair.key);
            assert!(
                !key_str.contains("/acp/"),
                "Datastore should not see ACP keys via iteration, found: {}",
                key_str
            );
        }
    }

    // ACP store should still work correctly
    assert!(acp_store.has_tuple(&tuple).await.unwrap());
    let doc_tuples = acp_store.get_doc_tuples("users", "doc1").await.unwrap();
    assert_eq!(doc_tuples.len(), 1);

    // Verify datastore can still read its own data
    {
        let txn = datastore.new_txn(true).await.unwrap();
        let value = txn.get(b"collection/doc").await.unwrap();
        assert_eq!(value, Some(b"document data".to_vec()));
    }
}
