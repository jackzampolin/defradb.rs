//! Tests for AcpStore trait implementations.

use acp::{AcpStore, MemoryAcpStore, RelationTuple};
use identity::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
}

#[tokio::test]
async fn test_memory_store_put_and_has() {
    let store = MemoryAcpStore::new();
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    assert!(!store.has_tuple(&tuple).await.unwrap());
    store.put_tuple(&tuple).await.unwrap();
    assert!(store.has_tuple(&tuple).await.unwrap());
}

#[tokio::test]
async fn test_memory_store_delete() {
    let store = MemoryAcpStore::new();
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    store.put_tuple(&tuple).await.unwrap();
    assert!(store.has_tuple(&tuple).await.unwrap());

    store.delete_tuple(&tuple).await.unwrap();
    assert!(!store.has_tuple(&tuple).await.unwrap());
}

#[tokio::test]
async fn test_memory_store_get_doc_tuples() {
    let store = MemoryAcpStore::new();
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
async fn test_memory_store_is_doc_registered() {
    let store = MemoryAcpStore::new();
    let tuple = RelationTuple::try_new(test_did(), "owner", "users", "doc1").expect("valid tuple");

    assert!(!store.is_doc_registered("users", "doc1").await.unwrap());
    store.put_tuple(&tuple).await.unwrap();
    assert!(store.is_doc_registered("users", "doc1").await.unwrap());
}

#[tokio::test]
async fn test_memory_store_delete_doc_tuples() {
    let store = MemoryAcpStore::new();
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
