//! Tests for collection_acp module.

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore};
use db::collection_acp::{
    block_unsafe_policy_transition, check_doc_permission, check_policy_transition,
    register_collection_if_needed, register_doc_if_needed, warn_on_unsafe_policy_transition,
    AcpContext,
};
use identity::Did;
use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
}

fn collection_without_policy() -> CollectionVersion {
    CollectionVersion::new(
        "users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

fn collection_with_policy() -> CollectionVersion {
    let mut col = CollectionVersion::new(
        "users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    col.policy = Some(PolicyDescription::new("policy1", "users"));
    col
}

fn branchable_collection_with_policy() -> CollectionVersion {
    let mut col = collection_with_policy();
    col.is_branchable = true;
    col
}

fn branchable_collection_without_policy() -> CollectionVersion {
    let mut col = collection_without_policy();
    col.is_branchable = true;
    col
}

#[tokio::test]
async fn test_no_policy_allows_all() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = collection_without_policy();

    // Anyone should have access when there's no policy
    let allowed = check_doc_permission(
        &acp,
        &Identity::Anonymous,
        DocumentPermission::Read,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();

    assert!(allowed);
}

#[tokio::test]
async fn test_register_with_policy_and_identity() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let collection = collection_with_policy();
    let owner = test_did();

    // Register document
    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    // Verify owner has access
    let policy = collection.policy.as_ref().unwrap();
    let is_registered = acp
        .is_doc_registered(&policy.id, &policy.resource_name, "doc1")
        .await
        .unwrap();
    assert!(is_registered);
}

#[tokio::test]
async fn test_no_register_without_identity() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let collection = collection_with_policy();

    // Register without identity (public document)
    register_doc_if_needed(&acp, None, &collection, "doc1")
        .await
        .unwrap();

    // Document should NOT be registered
    let policy = collection.policy.as_ref().unwrap();
    let is_registered = acp
        .is_doc_registered(&policy.id, &policy.resource_name, "doc1")
        .await
        .unwrap();
    assert!(!is_registered);
}

#[tokio::test]
async fn branchable_permissioned_with_identity_registers_collection_object_owned_by_creator() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = branchable_collection_with_policy();
    let owner = test_did();

    register_collection_if_needed(&acp, Some(&owner), &collection)
        .await
        .unwrap();

    let policy = collection.policy.as_ref().unwrap();
    assert!(acp
        .is_doc_registered(&policy.id, &policy.resource_name, &collection.collection_id)
        .await
        .unwrap());
    assert_eq!(
        acp.get_doc_owner(&policy.id, &policy.resource_name, &collection.collection_id)
            .await
            .unwrap(),
        Some(owner)
    );
}

#[tokio::test]
async fn branchable_permissioned_no_identity_registers_nothing_public() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = branchable_collection_with_policy();

    register_collection_if_needed(&acp, None, &collection)
        .await
        .unwrap();

    let policy = collection.policy.as_ref().unwrap();
    assert!(!acp
        .is_doc_registered(&policy.id, &policy.resource_name, &collection.collection_id)
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &Identity::Anonymous,
            DocumentPermission::Read,
            &policy.id,
            &policy.resource_name,
            &collection.collection_id,
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn non_branchable_registers_no_collection_object() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = collection_with_policy();
    let owner = test_did();

    register_collection_if_needed(&acp, Some(&owner), &collection)
        .await
        .unwrap();

    let policy = collection.policy.as_ref().unwrap();
    assert!(!acp
        .is_doc_registered(&policy.id, &policy.resource_name, &collection.collection_id)
        .await
        .unwrap());
}

#[tokio::test]
async fn unpermissioned_branchable_is_fully_public() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = branchable_collection_without_policy();
    let owner = test_did();

    register_collection_if_needed(&acp, Some(&owner), &collection)
        .await
        .unwrap();

    assert!(check_doc_permission(
        &acp,
        &Identity::Anonymous,
        DocumentPermission::Read,
        &collection,
        &collection.collection_id,
        None,
    )
    .await
    .unwrap());
}

#[tokio::test]
async fn double_collection_registration_is_idempotent() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = branchable_collection_with_policy();
    let owner = test_did();

    register_collection_if_needed(&acp, Some(&owner), &collection)
        .await
        .unwrap();
    register_collection_if_needed(&acp, Some(&owner), &collection)
        .await
        .unwrap();

    let policy = collection.policy.as_ref().unwrap();
    assert_eq!(
        acp.get_doc_owner(&policy.id, &policy.resource_name, &collection.collection_id)
            .await
            .unwrap(),
        Some(owner)
    );
}

#[tokio::test]
async fn test_owner_has_update_permission() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let collection = collection_with_policy();
    let owner = test_did();

    // Register document
    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    // Owner should have update permission
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(owner.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(allowed);
}

#[tokio::test]
async fn test_non_owner_denied_update_permission() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let collection = collection_with_policy();
    let owner = test_did();
    let stranger = test_did2();

    // Register document with owner
    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    // Stranger should NOT have update permission
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(!allowed);
}

#[tokio::test]
async fn test_acp_context() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = Arc::new(LocalDocumentACP::new(store));
    let collection = collection_with_policy();
    let owner = test_did();

    let ctx = AcpContext::new(acp, Identity::Authenticated(owner), None);

    // Register document using context
    ctx.register_doc(&collection, "doc1").await.unwrap();

    // Check permission using context
    let allowed = ctx
        .check_permission(DocumentPermission::Delete, &collection, "doc1")
        .await
        .unwrap();
    assert!(allowed);
}

// ============================================================================
// Policy Transition Safety Tests
// ============================================================================

fn policy1() -> PolicyDescription {
    PolicyDescription::new("policy1", "users")
}

fn policy2() -> PolicyDescription {
    PolicyDescription::new("policy2", "users")
}

fn policy_different_resource() -> PolicyDescription {
    PolicyDescription::new("policy1", "profiles")
}

#[test]
fn test_policy_transition_none_to_none_is_safe() {
    let check = check_policy_transition(None, None);
    assert!(check.is_safe());
    assert!(!check.has_warning());
}

#[test]
fn test_policy_transition_none_to_some_is_safe() {
    // Adding a policy is safe - existing docs remain public
    let new_policy = policy1();
    let check = check_policy_transition(None, Some(&new_policy));
    assert!(check.is_safe());
    assert!(!check.has_warning());
}

#[test]
fn test_policy_transition_some_to_none_warns() {
    // Removing a policy is dangerous - protected docs become public
    let old_policy = policy1();
    let check = check_policy_transition(Some(&old_policy), None);
    assert!(!check.is_safe());
    assert!(check.has_warning());
    assert!(check.warning_message().unwrap().contains("public"));
}

#[test]
fn test_policy_transition_same_policy_is_safe() {
    let old_policy = policy1();
    let new_policy = policy1();
    let check = check_policy_transition(Some(&old_policy), Some(&new_policy));
    assert!(check.is_safe());
    assert!(!check.has_warning());
}

#[test]
fn test_policy_transition_different_resource_name_warns() {
    // Changing resource name orphans existing registrations
    let old_policy = policy1();
    let new_policy = policy_different_resource();
    let check = check_policy_transition(Some(&old_policy), Some(&new_policy));
    assert!(!check.is_safe());
    assert!(check.has_warning());
    assert!(check.warning_message().unwrap().contains("orphan"));
}

#[test]
fn test_policy_transition_different_policy_id_warns() {
    // Changing policy ID with same resource name also warns
    let old_policy = policy1();
    let new_policy = policy2();
    let check = check_policy_transition(Some(&old_policy), Some(&new_policy));
    assert!(!check.is_safe());
    assert!(check.has_warning());
    assert!(check.warning_message().unwrap().contains("policy ID"));
}

#[test]
fn test_warn_on_unsafe_logs_warning() {
    let old_policy = policy1();
    let check = warn_on_unsafe_policy_transition("users", Some(&old_policy), None);
    assert!(check.has_warning());
}

#[test]
fn test_block_unsafe_policy_transition_allows_safe() {
    let new_policy = policy1();
    let result = block_unsafe_policy_transition("users", None, Some(&new_policy), false);
    assert!(result.is_ok());
}

#[test]
fn test_block_unsafe_policy_transition_blocks_unsafe() {
    let old_policy = policy1();
    let result = block_unsafe_policy_transition("users", Some(&old_policy), None, false);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("unsafe policy transition"));
}

#[test]
fn test_block_unsafe_policy_transition_allows_forced() {
    let old_policy = policy1();
    // Force=true allows unsafe transitions
    let result = block_unsafe_policy_transition("users", Some(&old_policy), None, true);
    assert!(result.is_ok());
}

// ============================================================================
// Policy Change Mid-Transaction Tests
// ============================================================================

/// Test that documents registered before a policy change retain their
/// ACP registrations but may become orphaned if the resource name changes.
#[tokio::test]
async fn test_policy_change_mid_transaction_resource_name_change() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let owner = test_did();

    // Start with policy1 (resource_name = "users")
    let collection_v1 = collection_with_policy();
    let policy_v1 = collection_v1.policy.as_ref().unwrap();

    // Register a document under the old policy
    register_doc_if_needed(&acp, Some(&owner), &collection_v1, "doc1")
        .await
        .unwrap();

    // Verify document is registered under old resource name
    assert!(acp
        .is_doc_registered(&policy_v1.id, &policy_v1.resource_name, "doc1")
        .await
        .unwrap());

    // Now create a "new version" with different resource name
    let mut collection_v2 = collection_v1.clone();
    collection_v2.policy = Some(PolicyDescription::new("policy1", "profiles"));
    let policy_v2 = collection_v2.policy.as_ref().unwrap();

    // Document should NOT be registered under new resource name
    // (simulating the orphaned registration scenario)
    assert!(!acp
        .is_doc_registered(&policy_v2.id, &policy_v2.resource_name, "doc1")
        .await
        .unwrap());

    // But the old registration still exists
    assert!(acp
        .is_doc_registered(&policy_v1.id, &policy_v1.resource_name, "doc1")
        .await
        .unwrap());
}

/// Test that checking permissions uses the current collection's policy,
/// so a policy change affects permission checks immediately.
#[tokio::test]
async fn test_policy_change_mid_transaction_permission_check() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let owner = test_did();
    let stranger = test_did2();

    // Register document under policy with resource_name = "users"
    let collection_v1 = collection_with_policy();
    register_doc_if_needed(&acp, Some(&owner), &collection_v1, "doc1")
        .await
        .unwrap();

    // Owner should have access
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(owner.clone()),
        DocumentPermission::Read,
        &collection_v1,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(allowed);

    // Stranger should NOT have access
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger.clone()),
        DocumentPermission::Read,
        &collection_v1,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(!allowed);

    // Now check with new collection version (different resource name)
    // This simulates mid-transaction policy change
    let mut collection_v2 = collection_v1.clone();
    collection_v2.policy = Some(PolicyDescription::new("policy1", "profiles"));

    // Document is not registered under new resource name, so it appears
    // unregistered (public) and BOTH users have access
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger.clone()),
        DocumentPermission::Read,
        &collection_v2,
        "doc1",
        None,
    )
    .await
    .unwrap();
    // This is the dangerous case - document becomes "public"
    assert!(allowed);
}

/// Test that removing a policy mid-transaction makes documents public.
#[tokio::test]
async fn test_policy_removal_mid_transaction_makes_docs_public() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let owner = test_did();
    let stranger = test_did2();

    // Register document under policy
    let collection_with_pol = collection_with_policy();
    register_doc_if_needed(&acp, Some(&owner), &collection_with_pol, "doc1")
        .await
        .unwrap();

    // Stranger denied when policy exists
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger.clone()),
        DocumentPermission::Update,
        &collection_with_pol,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(!allowed);

    // Now check with collection without policy (simulating policy removal)
    let collection_no_pol = collection_without_policy();

    // Document is now "public" - everyone has access
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger.clone()),
        DocumentPermission::Update,
        &collection_no_pol,
        "doc1",
        None,
    )
    .await
    .unwrap();
    // This is the dangerous case - previously protected doc is now public
    assert!(allowed);

    // Even anonymous users have access
    let allowed = check_doc_permission(
        &acp,
        &Identity::Anonymous,
        DocumentPermission::Update,
        &collection_no_pol,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(allowed);
}

// =========================================================================
// Bypass tests for #738 (DAC bypass thread-local) and #739 (node identity)
// =========================================================================

#[tokio::test]
async fn test_node_identity_bypass_grants_full_access() {
    // A request from the node identity should be granted full access to a
    // protected document owned by a different identity.
    // Matches Go's `internal/db/collection_acp.go:60-62`.
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = collection_with_policy();
    let owner = test_did();
    let node_identity = test_did2();

    // Register the document with `owner` as the owner.
    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    // The node identity is a different DID — it has no document-level grants.
    // Without the bypass, this would be denied.
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(node_identity.clone()),
        DocumentPermission::Read,
        &collection,
        "doc1",
        Some(&node_identity),
    )
    .await
    .unwrap();
    assert!(allowed, "node identity must be granted full access");

    // Same for Update.
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(node_identity.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        Some(&node_identity),
    )
    .await
    .unwrap();
    assert!(allowed, "node identity must be granted Update access");

    // Same for Delete.
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(node_identity.clone()),
        DocumentPermission::Delete,
        &collection,
        "doc1",
        Some(&node_identity),
    )
    .await
    .unwrap();
    assert!(allowed, "node identity must be granted Delete access");
}

#[tokio::test]
async fn test_non_node_identity_still_denied() {
    // Sanity check: with a configured node_identity, a different identity
    // must still be subject to the normal DAC checks.
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = collection_with_policy();
    let owner = test_did();
    let node_identity = test_did2();
    let stranger = Did::new("did:key:z6MkpZqHJYYwK7gP9eVUuLAMz3jJW4Wc8nN6F2VQ8RJ7Wn1V").unwrap();

    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger),
        DocumentPermission::Read,
        &collection,
        "doc1",
        Some(&node_identity),
    )
    .await
    .unwrap();
    assert!(
        !allowed,
        "stranger must still be denied even with node_identity configured"
    );
}

#[tokio::test]
async fn test_node_identity_none_falls_through() {
    // When node_identity is None, the bypass shortcut is skipped and DAC
    // applies normally.
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = collection_with_policy();
    let owner = test_did();
    let stranger = test_did2();

    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(stranger),
        DocumentPermission::Read,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(
        !allowed,
        "stranger must be denied when no node_identity is set"
    );
}

#[tokio::test]
async fn test_dac_bypass_thread_local_grants_full_access() {
    // The thread-local `dac_bypass` flag (set by HTTP/FFI entry points after
    // resolving NAC `bypass-dac` permission via `should_bypass_dac`) must
    // grant access on the mutation path too.
    // This covers #738 — the previous gap was that the mutation path
    // (`check_doc_permission`) ignored this flag.
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let collection = collection_with_policy();
    let owner = test_did();
    let admin = test_did2();

    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    // Without the flag, admin is denied.
    defra_core::dac_bypass::set_dac_bypass(false);
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(admin.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(!allowed, "admin without bypass flag must be denied");

    // With the flag set, admin is granted access regardless of DAC.
    defra_core::dac_bypass::set_dac_bypass(true);
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(admin.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(allowed, "admin with dac_bypass flag must be granted access");

    // Reset the thread-local so it doesn't leak into other tests.
    defra_core::dac_bypass::set_dac_bypass(false);
}

// =========================================================================
// Parity test for #740: "writer" must NOT be a recognized relation
// =========================================================================

#[tokio::test]
async fn test_writer_relation_is_not_recognized() {
    // Go DefraDB uses only owner/reader/updater/deleter relations.
    // A "writer" relation (which used to be checked in local.rs) must not
    // grant Update or Read access.
    use acp::{AcpStore, RelationTuple, UPDATER_RELATION};

    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store.clone());
    let collection = collection_with_policy();
    let owner = test_did();
    let granted_user = test_did2();

    register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
        .await
        .unwrap();

    // Manually insert a "writer" relation tuple via put_tuple.
    let policy = collection.policy.as_ref().unwrap();
    let ns_collection = format!("{}:{}", policy.id, policy.resource_name);
    let tuple = RelationTuple::try_new(granted_user.clone(), "writer", &ns_collection, "doc1")
        .expect("relation tuple");
    store.put_tuple(&tuple).await.unwrap();

    // The "writer" relation must NOT grant Update access (Go has no such relation).
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(granted_user.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(
        !allowed,
        "\"writer\" relation must not grant Update — Go DefraDB uses \"updater\""
    );

    // And it must NOT grant Read either.
    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(granted_user.clone()),
        DocumentPermission::Read,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(!allowed, "\"writer\" relation must not grant Read");

    // Now grant the canonical "updater" relation and verify Read+Update work.
    let tuple = RelationTuple::try_new(
        granted_user.clone(),
        UPDATER_RELATION,
        &ns_collection,
        "doc1",
    )
    .expect("relation tuple");
    store.put_tuple(&tuple).await.unwrap();

    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(granted_user.clone()),
        DocumentPermission::Update,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(allowed, "canonical 'updater' relation must grant Update");

    let allowed = check_doc_permission(
        &acp,
        &Identity::Authenticated(granted_user),
        DocumentPermission::Read,
        &collection,
        "doc1",
        None,
    )
    .await
    .unwrap();
    assert!(
        allowed,
        "canonical 'updater' relation must imply Read (matches Go's ImplyDocumentReadPerm)"
    );
}
