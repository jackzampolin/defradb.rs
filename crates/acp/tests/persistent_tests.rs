//! Tests for PersistentAcpStore backed by redb.

use acp::{AcpStore, PersistentAcpStore, RelationTuple};
use identity::Did;
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
