//! Tests for ZanzibarDocumentACP.

use std::sync::Arc;

use acp::{
    DocumentACP, DocumentPermission, Error, Identity, MemoryZanzibarStore, ZanzibarDocumentACP,
};
use identity::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
}

fn test_did3() -> Did {
    Did::new("did:key:z6MknSLrJoTcukLrE435hVNQT4JUhbvWLX4kUzqkEStBU8Vi").unwrap()
}

#[tokio::test]
async fn test_register_document() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "documents", "doc1")
        .await
        .unwrap();

    let registered = acp
        .is_doc_registered("policy1", "documents", "doc1")
        .await
        .unwrap();
    assert!(registered);

    let result = acp
        .register_doc_object(&owner, "policy1", "documents", "doc1")
        .await;
    assert!(matches!(result, Err(Error::DocumentAlreadyRegistered(_))));
}

#[tokio::test]
async fn test_owner_has_all_permissions() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "documents", "doc1")
        .await
        .unwrap();

    let identity = Identity::Authenticated(owner);

    let can_read = acp
        .check_doc_access(
            &identity,
            DocumentPermission::Read,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_read);

    let can_update = acp
        .check_doc_access(
            &identity,
            DocumentPermission::Update,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_update);

    let can_delete = acp
        .check_doc_access(
            &identity,
            DocumentPermission::Delete,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_delete);
}

#[tokio::test]
async fn test_unregistered_doc_is_public() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let can_read = acp
        .check_doc_access(
            &Identity::Anonymous,
            DocumentPermission::Read,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_read);
}

#[tokio::test]
async fn test_anonymous_cannot_access_registered() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "documents", "doc1")
        .await
        .unwrap();

    let can_read = acp
        .check_doc_access(
            &Identity::Anonymous,
            DocumentPermission::Read,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap();
    assert!(!can_read);
}

#[tokio::test]
async fn test_add_reader_relationship() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let reader = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    let added = acp
        .add_actor_relationship(
            &owner,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();
    assert!(added);

    let can_read = acp
        .check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_read);

    let can_update = acp
        .check_doc_access(
            &Identity::Authenticated(reader),
            DocumentPermission::Update,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(!can_update);
}

#[tokio::test]
async fn test_updater_implies_read() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let updater = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &updater,
        "collection1",
        "collection1",
        "doc1",
        "updater",
        &[],
    )
    .await
    .unwrap();

    let can_read = acp
        .check_doc_access(
            &Identity::Authenticated(updater.clone()),
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_read);

    let can_update = acp
        .check_doc_access(
            &Identity::Authenticated(updater),
            DocumentPermission::Update,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_update);
}

#[tokio::test]
async fn test_non_owner_cannot_add_relationship() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let non_owner = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    let result = acp
        .add_actor_relationship(
            &non_owner,
            &owner,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await;

    assert!(matches!(result, Err(Error::NotManager { .. })));
}

#[tokio::test]
async fn test_delete_relationship() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let reader = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &reader,
        "collection1",
        "collection1",
        "doc1",
        "reader",
        &[],
    )
    .await
    .unwrap();

    let deleted = acp
        .delete_actor_relationship(
            &owner,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();
    assert!(deleted);

    let can_read = acp
        .check_doc_access(
            &Identity::Authenticated(reader),
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(!can_read);
}

#[tokio::test]
async fn test_unregister_document() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "documents", "doc1")
        .await
        .unwrap();

    acp.unregister_doc_object("policy1", "documents", "doc1")
        .await
        .unwrap();

    let registered = acp
        .is_doc_registered("policy1", "documents", "doc1")
        .await
        .unwrap();
    assert!(!registered);
}

#[tokio::test]
async fn test_cannot_add_owner_relation() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let target = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    let result = acp
        .add_actor_relationship(
            &owner,
            &target,
            "collection1",
            "collection1",
            "doc1",
            "owner",
            &[],
        )
        .await;

    assert!(matches!(result, Err(Error::InvalidRelation(_))));
}

#[tokio::test]
async fn test_invalid_relation() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let target = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    let result = acp
        .add_actor_relationship(
            &owner,
            &target,
            "collection1",
            "collection1",
            "doc1",
            "invalid_relation",
            &[],
        )
        .await;

    assert!(matches!(result, Err(Error::InvalidRelation(_))));
}

// Manager delegation pattern tests

#[tokio::test]
async fn test_admin_can_add_reader_relationship() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let admin = test_did2();
    let reader = test_did3();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &admin,
        "collection1",
        "collection1",
        "doc1",
        "admin",
        &[],
    )
    .await
    .unwrap();

    let added = acp
        .add_actor_relationship(
            &admin,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();
    assert!(added);

    let can_read = acp
        .check_doc_access(
            &Identity::Authenticated(reader),
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_read);
}

#[tokio::test]
async fn test_admin_can_delete_reader_relationship() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let admin = test_did2();
    let reader = test_did3();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &admin,
        "collection1",
        "collection1",
        "doc1",
        "admin",
        &[],
    )
    .await
    .unwrap();

    acp.add_actor_relationship(
        &owner,
        &reader,
        "collection1",
        "collection1",
        "doc1",
        "reader",
        &[],
    )
    .await
    .unwrap();

    let deleted = acp
        .delete_actor_relationship(
            &admin,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();
    assert!(deleted);

    let can_read = acp
        .check_doc_access(
            &Identity::Authenticated(reader),
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(!can_read);
}

#[tokio::test]
async fn test_admin_cannot_add_admin_relationship() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let admin = test_did2();
    let other = test_did3();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &admin,
        "collection1",
        "collection1",
        "doc1",
        "admin",
        &[],
    )
    .await
    .unwrap();

    let result = acp
        .add_actor_relationship(
            &admin,
            &other,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await;
    assert!(matches!(result, Err(Error::NotOwner { .. })));
}

#[tokio::test]
async fn test_reader_cannot_add_relationships() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let reader = test_did2();
    let other = test_did3();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &reader,
        "collection1",
        "collection1",
        "doc1",
        "reader",
        &[],
    )
    .await
    .unwrap();

    let result = acp
        .add_actor_relationship(
            &reader,
            &other,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await;
    assert!(matches!(result, Err(Error::NotManager { .. })));
}

#[tokio::test]
async fn test_admin_has_read_update_delete_permissions() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let admin = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &admin,
        "collection1",
        "collection1",
        "doc1",
        "admin",
        &[],
    )
    .await
    .unwrap();

    let admin_identity = Identity::Authenticated(admin);

    let can_read = acp
        .check_doc_access(
            &admin_identity,
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_read);

    let can_update = acp
        .check_doc_access(
            &admin_identity,
            DocumentPermission::Update,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_update);

    let can_delete = acp
        .check_doc_access(
            &admin_identity,
            DocumentPermission::Delete,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap();
    assert!(can_delete);
}

#[tokio::test]
async fn test_revoking_admin_removes_management_capability() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let admin = test_did2();
    let reader = test_did3();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &admin,
        "collection1",
        "collection1",
        "doc1",
        "admin",
        &[],
    )
    .await
    .unwrap();

    acp.delete_actor_relationship(
        &owner,
        &admin,
        "collection1",
        "collection1",
        "doc1",
        "admin",
        &[],
    )
    .await
    .unwrap();

    let result = acp
        .add_actor_relationship(
            &admin,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await;
    assert!(matches!(result, Err(Error::NotManager { .. })));
}
