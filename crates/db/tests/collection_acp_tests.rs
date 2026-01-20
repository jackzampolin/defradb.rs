//! Tests for collection_acp module.

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore};
use db::collection_acp::{
    block_unsafe_policy_transition, check_doc_permission, check_policy_transition,
    register_doc_if_needed, warn_on_unsafe_policy_transition, AcpContext,
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

    let ctx = AcpContext::new(acp, Identity::Authenticated(owner));

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
    assert!(
        acp.is_doc_registered(&policy_v1.id, &policy_v1.resource_name, "doc1")
            .await
            .unwrap()
    );

    // Now create a "new version" with different resource name
    let mut collection_v2 = collection_v1.clone();
    collection_v2.policy = Some(PolicyDescription::new("policy1", "profiles"));
    let policy_v2 = collection_v2.policy.as_ref().unwrap();

    // Document should NOT be registered under new resource name
    // (simulating the orphaned registration scenario)
    assert!(
        !acp.is_doc_registered(&policy_v2.id, &policy_v2.resource_name, "doc1")
            .await
            .unwrap()
    );

    // But the old registration still exists
    assert!(
        acp.is_doc_registered(&policy_v1.id, &policy_v1.resource_name, "doc1")
            .await
            .unwrap()
    );
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
    )
    .await
    .unwrap();
    assert!(allowed);
}
