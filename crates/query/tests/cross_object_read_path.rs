//! Read-path coverage for cross-object (collection-level) ACP grants.
//!
//! The query engine gates every read — User queries (`select`, `aggregate`) and
//! Commits queries alike — through `check_doc_access_with_overlay`. That gate's
//! overlay only projects deferred *registration*; access resolution falls
//! straight through to the backend's `check_doc_access`. So a cross-object grant
//! resolved by the Zanzibar backend is honoured uniformly across both read
//! paths. This proves it at the gate the runner actually calls.
//!
//! (True HTTP end-to-end waits on backend selection — the default embedded
//! backend is Local, which cannot represent a cross-object subject.)

use std::sync::Arc;

use acp::{
    check_doc_read_access,
    policy_yaml::{build_policy, parse_policy_yaml},
    DocumentACP, DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore,
    MemoryZanzibarStore, Subject, ZanzibarDocumentACP, ZanzibarStore, READER_RELATION,
};
use identity::Did;
use query::txn::{check_doc_access_with_overlay, OverlayChecker};

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

fn did(s: &str) -> Did {
    Did::new(s).unwrap()
}

async fn gate_allows(
    acp: &ZanzibarDocumentACP<MemoryZanzibarStore>,
    policy_id: &str,
    who: &Did,
    resource: &str,
    doc: &str,
) -> bool {
    check_doc_access_with_overlay(
        acp,
        &Identity::Authenticated(who.clone()),
        DocumentPermission::Read,
        policy_id,
        resource,
        doc,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn query_gate_honours_cross_object_read_inheritance() {
    let owner = did("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
    let alice = did("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH");
    let bob = did("did:key:z6MknSLrJoTcukLrE435hVNQT4JUhbvWLX4kUzqkEStBU8Vi");

    let parsed = parse_policy_yaml(FS_POLICY).unwrap();
    let policy = build_policy(&parsed, 1).unwrap();
    let policy_id = policy.id.clone();

    let store = Arc::new(MemoryZanzibarStore::new());
    store.store_policy(&policy).await.unwrap();
    let acp = ZanzibarDocumentACP::new(store);

    acp.register_doc_object(&owner, &policy_id, "directory", "teamdir")
        .await
        .unwrap();
    acp.register_doc_object(&owner, &policy_id, "file", "report")
        .await
        .unwrap();
    acp.add_relationship(
        &owner,
        Subject::Entity(alice.clone()),
        &policy_id,
        "directory",
        "teamdir",
        "reader",
        &[],
    )
    .await
    .unwrap();

    // Before the parent edge: the query gate denies alice on the file.
    assert!(!gate_allows(&acp, &policy_id, &alice, "file", "report").await);

    // Seed the cross-object parent edge through the widened API.
    acp.add_relationship(
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

    // The query gate now resolves alice's read of the file via
    // parent->read -> directory#read -> reader. This is the exact gate every
    // User and Commits read funnels through.
    assert!(
        gate_allows(&acp, &policy_id, &alice, "file", "report").await,
        "query gate must honour the cross-object read inheritance"
    );

    // bob has no grant -> still denied through the cone.
    assert!(!gate_allows(&acp, &policy_id, &bob, "file", "report").await);
}

#[tokio::test]
async fn overlay_checker_applies_branchable_collection_fallback() {
    let owner = did("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
    let alice = did("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH");
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let identity = Identity::Authenticated(alice.clone());

    acp.register_doc_object(&owner, "policy1", "users", "col1")
        .await
        .unwrap();
    let checker = OverlayChecker {
        acp: &acp,
        identity: &identity,
    };

    let public_doc_allowed =
        check_doc_read_access(&checker, "policy1", "users", "col1", true, "public-doc")
            .await
            .unwrap();
    assert!(
        !public_doc_allowed,
        "public doc in a protected branchable collection must fall back to the collection object"
    );

    acp.register_doc_object(&owner, "policy1", "users", "shared-doc")
        .await
        .unwrap();
    acp.add_actor_relationship(
        &owner,
        &alice,
        "policy1",
        "users",
        "shared-doc",
        READER_RELATION,
        &[],
    )
    .await
    .unwrap();

    let shared_doc_allowed =
        check_doc_read_access(&checker, "policy1", "users", "col1", true, "shared-doc")
            .await
            .unwrap();
    assert!(
        shared_doc_allowed,
        "explicit document grants must win before checking the collection object"
    );
}
