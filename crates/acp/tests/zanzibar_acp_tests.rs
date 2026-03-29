//! Tests for ZanzibarDocumentACP.

use std::sync::Arc;

use acp::{
    policy_yaml::{build_policy, parse_policy_yaml, validate_policy_expressions},
    DocumentACP, DocumentPermission, Error, Identity, MemoryZanzibarStore, ZanzibarDocumentACP,
    ZanzibarStore,
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

fn test_did4() -> Did {
    Did::new("did:key:z6Mkm6dQZfJ4rK5QJc1t8u8yKpWq2F6f4gL3r1m9u2Yt7n8Q").unwrap()
}

async fn create_custom_policy_acp(
    yaml: &str,
) -> (ZanzibarDocumentACP<MemoryZanzibarStore>, String) {
    let parsed = parse_policy_yaml(yaml).unwrap();
    validate_policy_expressions(&parsed).unwrap();

    let policy = build_policy(&parsed, 1).unwrap();
    let policy_id = policy.id.clone();

    let store = Arc::new(MemoryZanzibarStore::new());
    store.store_policy(&policy).await.unwrap();

    (ZanzibarDocumentACP::new(store), policy_id)
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
    // Go DefraDB accepts any relation (validation is done at the adapter layer
    // against the policy definition). Only "owner" is universally rejected.
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let target = test_did2();

    acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    // "owner" should still be rejected
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

    // Non-owner relations are accepted (matching Go behavior)
    let result = acp
        .add_actor_relationship(
            &owner,
            &target,
            "collection1",
            "collection1",
            "doc1",
            "custom_relation",
            &[],
        )
        .await;
    assert!(result.is_ok());
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

#[tokio::test]
async fn test_difference_policy_with_generated_id_evaluates_at_check_time() {
    let yaml = r#"
name: public_except_blocked
resources:
- name: document
  permissions:
  - name: read
    expr: reader - blocked
  - name: update
    expr: writer - blocked
  - name: delete
    expr: admin
  relations:
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
  - name: blocked
    types: [actor]
  - name: admin
    manages: [reader, writer, blocked]
    types: [actor]
"#;

    let (acp, policy_id) = create_custom_policy_acp(yaml).await;
    assert_ne!(policy_id, "document");

    let owner = test_did();
    let bob = test_did2();
    let mallory = test_did3();
    let stranger = test_did4();

    acp.register_doc_object(&owner, &policy_id, "document", "employee_handbook")
        .await
        .unwrap();

    acp.add_actor_relationship(
        &owner,
        &bob,
        &policy_id,
        "document",
        "employee_handbook",
        "reader",
        &[],
    )
    .await
    .unwrap();
    acp.add_actor_relationship(
        &owner,
        &mallory,
        &policy_id,
        "document",
        "employee_handbook",
        "reader",
        &[],
    )
    .await
    .unwrap();
    acp.add_actor_relationship(
        &owner,
        &mallory,
        &policy_id,
        "document",
        "employee_handbook",
        "blocked",
        &[],
    )
    .await
    .unwrap();

    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(bob.clone()),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "employee_handbook"
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(mallory.clone()),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "employee_handbook"
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(stranger),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "employee_handbook"
        )
        .await
        .unwrap());

    acp.add_actor_relationship(
        &owner,
        &owner,
        &policy_id,
        "document",
        "employee_handbook",
        "blocked",
        &[],
    )
    .await
    .unwrap();
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(owner.clone()),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "employee_handbook"
        )
        .await
        .unwrap());

    assert!(acp
        .delete_actor_relationship(
            &owner,
            &bob,
            &policy_id,
            "document",
            "employee_handbook",
            "reader",
            &[],
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(bob),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "employee_handbook"
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_nested_difference_policy_with_generated_id_handles_union_and_blocklist() {
    let yaml = r#"
name: nested_difference
resources:
- name: document
  permissions:
  - name: read
    expr: (reader + writer) - blocked
  - name: update
    expr: writer - blocked
  - name: delete
    expr: admin
  relations:
  - name: reader
    types: [actor]
  - name: writer
    types: [actor]
  - name: blocked
    types: [actor]
  - name: admin
    types: [actor]
"#;

    let (acp, policy_id) = create_custom_policy_acp(yaml).await;
    assert_ne!(policy_id, "document");

    let owner = test_did();
    let reader = test_did2();
    let writer = test_did3();
    let blocked_writer = test_did4();

    acp.register_doc_object(&owner, &policy_id, "document", "doc1")
        .await
        .unwrap();

    for (target, relation) in [
        (&reader, "reader"),
        (&writer, "writer"),
        (&blocked_writer, "writer"),
        (&blocked_writer, "blocked"),
    ] {
        acp.add_actor_relationship(
            &owner,
            target,
            &policy_id,
            "document",
            "doc1",
            relation,
            &[],
        )
        .await
        .unwrap();
    }

    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(reader),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "doc1"
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(writer.clone()),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "doc1"
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(writer),
            DocumentPermission::Update,
            &policy_id,
            "document",
            "doc1"
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(blocked_writer.clone()),
            DocumentPermission::Read,
            &policy_id,
            "document",
            "doc1"
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(blocked_writer),
            DocumentPermission::Update,
            &policy_id,
            "document",
            "doc1"
        )
        .await
        .unwrap());
}
