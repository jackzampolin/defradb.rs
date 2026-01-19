//! Tests for collection_acp module.

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore};
use db::collection_acp::{check_doc_permission, register_doc_if_needed, AcpContext};
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
