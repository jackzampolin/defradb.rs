//! SPIKE: prove the live `ZanzibarDocumentACP` path resolves cross-object
//! TupleToUserset (TTU) inheritance — a `directory -> file` read cone — once a
//! cross-object `parent` edge can be seeded.
//!
//! Context: the live DefraDB document-ACP *seam* only accepts actor-DID targets
//! (`add_actor_relationship(target: &Did)`), so today you cannot seed a
//! cross-object edge like `file:report#parent@directory:teamdir`. But the
//! underlying Zanzibar store + engine already model and resolve it: the engine's
//! `TupleToUserset` arm reads `parent` subjects via `get_relation_subjects`/
//! `get_relation_targets`, and `MemoryZanzibarStore::get_relation_targets` maps a
//! `Subject::EntitySet { resource, object_id, .. }` to an `ObjectRef`.
//!
//! This spike seeds that edge directly through the store (standing in for the
//! seam method we'd add) and asserts that `check_doc_access` then resolves
//! `file#read` through `parent->read` to the directory's `reader`. The before/
//! after assertions prove the cross-object edge is what grants access — not some
//! pre-existing direct grant.

use std::sync::Arc;

use acp::{
    policy_yaml::{build_policy, parse_policy_yaml, validate_policy_expressions},
    DocumentACP, DocumentPermission, Identity, MemoryZanzibarStore, Relationship, Subject,
    ZanzibarDocumentACP, ZanzibarStore,
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

/// A two-resource filesystem policy. `file.read` inherits from its parent
/// directory via the cross-object TTU `parent->read`.
// `owner` is a reserved relation auto-injected into every permission by the
// Discretionary transformer, so it is neither declared nor referenced here.
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

#[tokio::test]
async fn spike_cross_object_ttu_read_inheritance() {
    // --- build + store the multi-resource filesystem policy ---
    let parsed = parse_policy_yaml(FS_POLICY).unwrap();
    validate_policy_expressions(&parsed).unwrap();
    let policy = build_policy(&parsed, 1).unwrap();
    let policy_id = policy.id.clone();

    let store = Arc::new(MemoryZanzibarStore::new());
    store.store_policy(&policy).await.unwrap();
    let acp = ZanzibarDocumentACP::new(store.clone());

    let owner = did_owner();

    // --- register a directory doc and a file doc (each gets an owner) ---
    acp.register_doc_object(&owner, &policy_id, "directory", "teamdir")
        .await
        .unwrap();
    acp.register_doc_object(&owner, &policy_id, "file", "report")
        .await
        .unwrap();

    // --- grant alice direct reader on the DIRECTORY (a normal actor grant) ---
    acp.add_actor_relationship(
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

    // Sanity: alice can read the directory directly.
    assert!(
        can_read(&acp, &policy_id, did_alice(), "directory", "teamdir").await,
        "alice has a direct reader grant on the directory"
    );

    // BEFORE the cross-object edge exists: alice must NOT reach the file.
    // (Proves the test isn't passing via some pre-existing direct grant.)
    assert!(
        !can_read(&acp, &policy_id, did_alice(), "file", "report").await,
        "without a parent edge, directory access must NOT leak to the file"
    );

    // --- seed the cross-object edge: file:report#parent@directory:teamdir ---
    // The live actor-DID-only seam cannot express this; we seed it directly
    // through the store as the seam method would. An object subject is carried as
    // `Subject::EntitySet { resource, object_id, .. }`; the engine's TTU arm and
    // `get_relation_targets` key on (resource, object_id) and ignore the relation.
    let parent_edge = Relationship::new(
        "file",
        "report",
        "parent",
        Subject::entity_set("directory", "teamdir", ""),
    );
    store.store_relationship(&policy_id, &parent_edge).await.unwrap();

    // AFTER: alice now reaches the file via parent->read -> directory#read -> reader.
    assert!(
        can_read(&acp, &policy_id, did_alice(), "file", "report").await,
        "HEADLINE: alice reads the file via cross-object parent->read inheritance"
    );

    // Control: bob has no grant anywhere -> still denied.
    assert!(
        !can_read(&acp, &policy_id, did_bob(), "file", "report").await,
        "bob has no grant and must remain denied through the TTU cone"
    );
}
