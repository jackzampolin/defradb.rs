//! Tests for LocalDocumentACP implementation.

use std::sync::Arc;

use acp::{
    DocumentACP, DocumentPermission, Error, Identity, LocalDocumentACP, MemoryAcpStore,
    DELETER_RELATION, OWNER_RELATION, READER_RELATION, UPDATER_RELATION,
};
use identity::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
}

fn create_acp() -> LocalDocumentACP {
    LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()))
}

// Public Documents tests

#[tokio::test]
async fn test_unregistered_doc_allows_all_access() {
    let acp = create_acp();

    // Anonymous can access unregistered doc
    let access = acp
        .check_doc_access(
            &Identity::Anonymous,
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1",
        )
        .await
        .unwrap();
    assert!(access, "unregistered doc should allow anonymous read");

    // Any identity can access unregistered doc
    let access = acp
        .check_doc_access(
            &Identity::Authenticated(test_did()),
            DocumentPermission::Update,
            "policy1",
            "users",
            "doc1",
        )
        .await
        .unwrap();
    assert!(access, "unregistered doc should allow any update");
}

// Registered Documents tests

#[tokio::test]
async fn test_register_doc_creates_owner() {
    let acp = create_acp();
    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    assert!(acp
        .is_doc_registered("policy1", "users", "doc1")
        .await
        .unwrap());
}

#[tokio::test]
async fn test_register_doc_twice_fails() {
    let acp = create_acp();
    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let result = acp
        .register_doc_object(&owner, "policy1", "users", "doc1")
        .await;
    assert!(matches!(result, Err(Error::DocumentAlreadyRegistered(_))));
}

#[tokio::test]
async fn test_owner_has_all_permissions() {
    let acp = create_acp();
    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(owner.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(owner.clone()),
            DocumentPermission::Update,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(owner.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_anonymous_cannot_access_registered_doc() {
    let acp = create_acp();
    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let access = acp
        .check_doc_access(
            &Identity::Anonymous,
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1",
        )
        .await
        .unwrap();
    assert!(!access, "anonymous should not read registered doc");
}

#[tokio::test]
async fn test_non_owner_cannot_access_without_relation() {
    let acp = create_acp();
    let owner = test_did();
    let other = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(other.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(other.clone()),
            DocumentPermission::Update,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(other.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
}

// Sharing (AddActorRelationship) tests

#[tokio::test]
async fn test_add_reader_grants_read_only() {
    let acp = create_acp();
    let owner = test_did();
    let reader = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let added = acp
        .add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();
    assert!(added, "relationship should be added");

    // Reader can read
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Reader cannot update
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Update,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Reader cannot delete
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_add_updater_grants_read_and_update() {
    let acp = create_acp();
    let owner = test_did();
    let updater = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(&owner, &updater, "users", "doc1", UPDATER_RELATION)
        .await
        .unwrap();

    // Updater can read (implied)
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(updater.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Updater can update
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(updater.clone()),
            DocumentPermission::Update,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Updater cannot delete
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(updater.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_non_owner_cannot_add_relationship() {
    let acp = create_acp();
    let owner = test_did();
    let other = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let result = acp
        .add_actor_relationship(&other, &owner, "users", "doc1", READER_RELATION)
        .await;
    assert!(matches!(result, Err(Error::NotOwner { .. })));
}

#[tokio::test]
async fn test_cannot_add_owner_relation() {
    let acp = create_acp();
    let owner = test_did();
    let other = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let result = acp
        .add_actor_relationship(&owner, &other, "users", "doc1", OWNER_RELATION)
        .await;
    assert!(matches!(result, Err(Error::InvalidRelation(_))));
}

#[tokio::test]
async fn test_cannot_add_unknown_relation() {
    let acp = create_acp();
    let owner = test_did();
    let other = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    // Try to add a typo/unknown relation
    let result = acp
        .add_actor_relationship(&owner, &other, "users", "doc1", "reador") // typo
        .await;
    assert!(
        matches!(result, Err(Error::InvalidRelation(msg)) if msg.contains("unknown relation")),
        "should reject unknown relation names"
    );

    // Try another unknown relation
    let result = acp
        .add_actor_relationship(&owner, &other, "users", "doc1", "admin")
        .await;
    assert!(
        matches!(result, Err(Error::InvalidRelation(msg)) if msg.contains("unknown relation")),
        "should reject 'admin' as unknown relation"
    );
}

#[tokio::test]
async fn test_add_duplicate_relationship_returns_false() {
    let acp = create_acp();
    let owner = test_did();
    let reader = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let added1 = acp
        .add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();
    assert!(added1);

    let added2 = acp
        .add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();
    assert!(!added2, "duplicate add should return false");
}

// Delete relationship tests

#[tokio::test]
async fn test_delete_relationship() {
    let acp = create_acp();
    let owner = test_did();
    let reader = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();

    // Verify reader has access
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Delete relationship
    let deleted = acp
        .delete_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();
    assert!(deleted);

    // Verify reader no longer has access
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_delete_nonexistent_relationship_returns_false() {
    let acp = create_acp();
    let owner = test_did();
    let other = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    let deleted = acp
        .delete_actor_relationship(&owner, &other, "users", "doc1", READER_RELATION)
        .await
        .unwrap();
    assert!(!deleted);
}

// Deleter relation tests

#[tokio::test]
async fn test_add_deleter_grants_read_and_delete() {
    let acp = create_acp();
    let owner = test_did();
    let deleter = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(&owner, &deleter, "users", "doc1", DELETER_RELATION)
        .await
        .unwrap();

    // Deleter can read (implied by deleter relation)
    assert!(
        acp.check_doc_access(
            &Identity::Authenticated(deleter.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap(),
        "deleter should have implied read permission"
    );

    // Deleter can delete
    assert!(
        acp.check_doc_access(
            &Identity::Authenticated(deleter.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap(),
        "deleter should have delete permission"
    );

    // Deleter CANNOT update
    assert!(
        !acp.check_doc_access(
            &Identity::Authenticated(deleter.clone()),
            DocumentPermission::Update,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap(),
        "deleter should NOT have update permission"
    );
}

#[tokio::test]
async fn test_delete_deleter_relationship_revokes_access() {
    let acp = create_acp();
    let owner = test_did();
    let deleter = test_did2();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(&owner, &deleter, "users", "doc1", DELETER_RELATION)
        .await
        .unwrap();

    // Verify deleter has access
    assert!(acp
        .check_doc_access(
            &Identity::Authenticated(deleter.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Delete relationship
    acp.delete_actor_relationship(&owner, &deleter, "users", "doc1", DELETER_RELATION)
        .await
        .unwrap();

    // Verify deleter no longer has delete access
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(deleter.clone()),
            DocumentPermission::Delete,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());

    // Verify deleter also lost implied read access
    assert!(!acp
        .check_doc_access(
            &Identity::Authenticated(deleter.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap());
}

// Non-owner cannot delete relationship test

#[tokio::test]
async fn test_non_owner_cannot_delete_relationship() {
    let acp = create_acp();
    let owner = test_did();
    let reader = test_did2();
    let attacker = Did::new("did:key:z6MkattackerAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    acp.add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();

    // Attacker (non-owner) tries to delete reader relationship
    let result = acp
        .delete_actor_relationship(&attacker, &reader, "users", "doc1", READER_RELATION)
        .await;
    assert!(
        matches!(result, Err(Error::NotOwner { .. })),
        "non-owner should not be able to delete relationships"
    );
}

#[tokio::test]
async fn test_cannot_delete_owner_relation() {
    let acp = create_acp();
    let owner = test_did();

    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();

    // Owner tries to delete their own owner relation
    let result = acp
        .delete_actor_relationship(&owner, &owner, "users", "doc1", OWNER_RELATION)
        .await;
    assert!(
        matches!(result, Err(Error::InvalidRelation(_))),
        "should not be able to delete owner relation"
    );
}

// Cross-collection isolation test

#[tokio::test]
async fn test_cross_collection_isolation() {
    let acp = create_acp();
    let owner = test_did();
    let reader = test_did2();

    // Register same doc_id in two different collections
    acp.register_doc_object(&owner, "policy1", "users", "doc1")
        .await
        .unwrap();
    acp.register_doc_object(&owner, "policy1", "posts", "doc1")
        .await
        .unwrap();

    // Grant reader access to doc1 in "users" collection ONLY
    acp.add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
        .await
        .unwrap();

    // Reader CAN access users/doc1
    assert!(
        acp.check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Read,
            "policy1",
            "users",
            "doc1"
        )
        .await
        .unwrap(),
        "reader should access users/doc1"
    );

    // Reader CANNOT access posts/doc1 (different collection, no permission)
    assert!(
        !acp.check_doc_access(
            &Identity::Authenticated(reader.clone()),
            DocumentPermission::Read,
            "policy1",
            "posts",
            "doc1"
        )
        .await
        .unwrap(),
        "reader should NOT access posts/doc1 (cross-collection isolation)"
    );
}
