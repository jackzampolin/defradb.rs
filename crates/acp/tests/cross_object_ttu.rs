//! Cross-object (collection-level) ACP through the widened `DocumentACP` trait.
//!
//! Carried forward from the #1056 spike, but now seeded through the real
//! `DocumentACP::add_relationship(Subject)` API instead of an inherent
//! backdoor: a `directory -> file` read cone (`file.read = reader +
//! parent->read`) resolves end-to-end once the cross-object `parent` edge is
//! seeded as a `Subject::EntitySet`. The Zanzibar backend validates the subject
//! against the policy's declared `types:` (the soundness floor) before storing;
//! backends that cannot represent a non-actor subject reject it.

use std::sync::Arc;

use acp::{
    policy_yaml::{build_policy, parse_policy_yaml},
    DocumentACP, DocumentPermission, Error, Identity, LocalDocumentACP, MemoryAcpStore,
    MemoryZanzibarStore, Subject, ZanzibarDocumentACP, ZanzibarStore,
};
use identity::Did;

fn did_owner() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}
fn did_alice() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
}
fn did_bob() -> Did {
    Did::new("did:key:z6MknSLrJoTcukLrE435hVNQT4JUhbvWLX4kUzqkEStBU8Vi").unwrap()
}

const FS_POLICY: &str = r#"
name: filesystem
resources:
- name: directory
  permissions:
  - name: read
    expr: reader
  - name: update
    expr: reader
  - name: delete
    expr: reader
  relations:
  - name: reader
    types: [actor]
- name: file
  permissions:
  - name: read
    expr: reader + parent->read
  - name: update
    expr: reader
  - name: delete
    expr: reader
  relations:
  - name: reader
    types: [actor]
  - name: parent
    types: [directory]
"#;

async fn can_read(
    acp: &ZanzibarDocumentACP<MemoryZanzibarStore>,
    policy_id: &str,
    who: Did,
    resource: &str,
    doc: &str,
) -> bool {
    acp.check_doc_access(
        &Identity::Authenticated(who),
        DocumentPermission::Read,
        policy_id,
        resource,
        doc,
    )
    .await
    .unwrap()
}

async fn fs_acp() -> (ZanzibarDocumentACP<MemoryZanzibarStore>, String) {
    let parsed = parse_policy_yaml(FS_POLICY).unwrap();
    let policy = build_policy(&parsed, 1).unwrap();
    let policy_id = policy.id.clone();

    let store = Arc::new(MemoryZanzibarStore::new());
    store.store_policy(&policy).await.unwrap();
    let acp = ZanzibarDocumentACP::new(store.clone());

    let owner = did_owner();
    acp.register_doc_object(&owner, &policy_id, "directory", "teamdir")
        .await
        .unwrap();
    acp.register_doc_object(&owner, &policy_id, "file", "report")
        .await
        .unwrap();
    (acp, policy_id)
}

#[tokio::test]
async fn cross_object_parent_edge_grants_read_inheritance() {
    let (acp, policy_id) = fs_acp().await;
    let owner = did_owner();

    // alice gets a direct reader grant on the DIRECTORY (a normal actor grant
    // through the same widened API).
    acp.add_relationship(
        &owner,
        Subject::Entity(did_alice()),
        &policy_id,
        "directory",
        "teamdir",
        "reader",
        &[],
    )
    .await
    .unwrap();
    assert!(can_read(&acp, &policy_id, did_alice(), "directory", "teamdir").await);

    // BEFORE the cross-object edge: directory access must NOT leak to the file.
    assert!(!can_read(&acp, &policy_id, did_alice(), "file", "report").await);

    // Seed the cross-object edge through the widened trait API. The subject is a
    // `directory` object reference (EntitySet, empty relation) — the parent edge.
    let added = acp
        .add_relationship(
            &owner,
            Subject::entity_set("directory", "teamdir", ""),
            &policy_id,
            "file",
            "report",
            "parent",
            &[],
        )
        .await
        .unwrap();
    assert!(added, "the cross-object parent edge should be newly added");

    // AFTER: alice reaches the file via parent->read -> directory#read -> reader.
    assert!(
        can_read(&acp, &policy_id, did_alice(), "file", "report").await,
        "alice reads the file via cross-object parent->read inheritance"
    );

    // bob has no grant anywhere -> still denied through the TTU cone.
    assert!(!can_read(&acp, &policy_id, did_bob(), "file", "report").await);
}

#[tokio::test]
async fn add_actor_relationship_still_works_via_delegation() {
    // The actor convenience path now routes through add_relationship; it must
    // behave exactly as before.
    let (acp, policy_id) = fs_acp().await;
    let owner = did_owner();

    let added = acp
        .add_actor_relationship(
            &owner,
            &did_alice(),
            &policy_id,
            "directory",
            "teamdir",
            "reader",
            &[],
        )
        .await
        .unwrap();
    assert!(added);
    assert!(can_read(&acp, &policy_id, did_alice(), "directory", "teamdir").await);
}

#[tokio::test]
async fn add_relationship_enforces_the_subject_floor() {
    // A `directory` object edge on an actor-typed relation (`reader`) must be
    // rejected by the floor validation wired into the add path.
    let (acp, policy_id) = fs_acp().await;
    let owner = did_owner();

    let err = acp
        .add_relationship(
            &owner,
            Subject::entity_set("directory", "teamdir", ""),
            &policy_id,
            "file",
            "report",
            "reader",
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::SubjectRestrictionViolation { .. }),
        "an object edge on an actor-typed relation must be rejected, got {:?}",
        err
    );
}

#[tokio::test]
async fn local_backend_rejects_cross_object_subjects() {
    // The Local backend stores bare DIDs and cannot represent a cross-object
    // edge; it must reject one with a typed UnsupportedSubject error.
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let owner = did_owner();
    acp.register_doc_object(&owner, "policy1", "file", "report")
        .await
        .unwrap();

    let err = acp
        .add_relationship(
            &owner,
            Subject::entity_set("directory", "teamdir", ""),
            "policy1",
            "file",
            "report",
            "parent",
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::UnsupportedSubject(_)),
        "Local backend must reject a cross-object subject, got {:?}",
        err
    );
}
