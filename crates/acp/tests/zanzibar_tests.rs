//! Integration tests for Zanzibar permission model.
//!
//! Tests the full Zanzibar permission system including:
//! - This rule (direct lookup)
//! - ComputedUserset (owner implies reader)
//! - TupleToUserset (file in folder, folder owner can read file)
//! - Union (owner + reader)
//! - Intersection (member & approved)
//! - Difference (member - banned)
//! - Cycle detection
//! - Backward compatibility with LocalDocumentACP

use std::sync::Arc;

use acp::{
    DocumentACP, DocumentPermission, Identity, LocalDocumentACP, MemoryAcpStore,
    MemoryZanzibarStore, PermissionEngine, Policy, Relation, RelationExpression, Relationship,
    Resource, Subject, ZanzibarDocumentACP, ZanzibarStore,
};
use zanzibar::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
}

fn to_idid(did: &Did) -> Did {
    did.clone()
}

// =============================================================================
// This Rule Tests
// =============================================================================

#[tokio::test]
async fn test_this_rule_direct_lookup() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    engine.add_policy(&policy);

    let did = test_did();

    // Store owner relationship
    let rel = Relationship::with_entity("document", "doc1", "owner", did.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Direct lookup works
    assert!(engine
        .check("policy1", "document", "doc1", "owner", &did)
        .await
        .unwrap());

    // Non-owner doesn't have access
    let other = test_did2();
    assert!(!engine
        .check("policy1", "document", "doc1", "owner", &other)
        .await
        .unwrap());
}

// =============================================================================
// ComputedUserset Tests
// =============================================================================

#[tokio::test]
async fn test_computed_userset_owner_implies_reader() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Policy: reader = _this + owner
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::computed(
                "reader",
                RelationExpression::union(vec![
                    RelationExpression::this(),
                    RelationExpression::computed_userset("owner"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let owner = test_did();

    // Store owner relationship
    let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Owner should have reader access (via computed userset)
    assert!(engine
        .check("policy1", "document", "doc1", "reader", &owner)
        .await
        .unwrap());
}

// =============================================================================
// TupleToUserset Tests
// =============================================================================

#[tokio::test]
async fn test_tuple_to_userset_folder_owner_can_read_file() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Policy: file.reader = parent->owner
    let policy = Policy::new("policy1", "Test")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "owner"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("owner")));

    engine.add_policy(&policy);

    let folder_owner = test_did();

    // Folder owner relationship
    let rel = Relationship::with_entity("folder", "folder1", "owner", folder_owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // File has parent relation to folder (entity set subject)
    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("folder", "folder1", "owner"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Folder owner can read file via tuple-to-userset
    assert!(engine
        .check("policy1", "file", "file1", "reader", &folder_owner)
        .await
        .unwrap());

    // Non-owner cannot read
    let non_owner = test_did2();
    assert!(!engine
        .check("policy1", "file", "file1", "reader", &non_owner)
        .await
        .unwrap());
}

// =============================================================================
// Union Tests
// =============================================================================

#[tokio::test]
async fn test_union_owner_or_reader_grants_access() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Policy: viewer = owner + reader
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::direct("reader"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::union(vec![
                    RelationExpression::computed_userset("owner"),
                    RelationExpression::computed_userset("reader"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let owner = test_did();
    let reader = test_did2();

    // Owner has access
    let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Reader has access
    let rel = Relationship::with_entity("document", "doc1", "reader", reader.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Both can view
    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &owner)
        .await
        .unwrap());
    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &reader)
        .await
        .unwrap());
}

// =============================================================================
// Intersection Tests
// =============================================================================

#[tokio::test]
async fn test_intersection_member_and_approved_required() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Policy: editor = member & approved
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("member"))
            .with_relation(Relation::direct("approved"))
            .with_relation(Relation::computed(
                "editor",
                RelationExpression::intersection(vec![
                    RelationExpression::computed_userset("member"),
                    RelationExpression::computed_userset("approved"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // User is member only - not editor
    let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    assert!(!engine
        .check("policy1", "document", "doc1", "editor", &user)
        .await
        .unwrap());

    // Add approval - now editor
    let rel = Relationship::with_entity("document", "doc1", "approved", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    assert!(engine
        .check("policy1", "document", "doc1", "editor", &user)
        .await
        .unwrap());
}

// =============================================================================
// Difference Tests
// =============================================================================

#[tokio::test]
async fn test_difference_member_minus_banned() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Policy: viewer = member - banned
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("member"))
            .with_relation(Relation::direct("banned"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::difference(
                    RelationExpression::computed_userset("member"),
                    RelationExpression::computed_userset("banned"),
                ),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // User is member - can view
    let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap());

    // Ban user - no longer viewer
    let rel = Relationship::with_entity("document", "doc1", "banned", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    assert!(!engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap());
}

// =============================================================================
// Wildcard Tests
// =============================================================================

#[tokio::test]
async fn test_wildcard_public_access() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("viewer")));

    engine.add_policy(&policy);

    // Wildcard relationship (public access)
    let rel = Relationship::new("document", "doc1", "viewer", Subject::Wildcard);
    store.store_relationship("policy1", &rel).await.unwrap();

    // Any user can view
    let user1 = test_did();
    let user2 = test_did2();

    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &user1)
        .await
        .unwrap());
    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &user2)
        .await
        .unwrap());
}

// =============================================================================
// ZanzibarDocumentACP Tests
// =============================================================================

#[tokio::test]
async fn test_zanzibar_document_acp_basic() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let owner_id = to_idid(&owner);

    // Register document
    acp.register_doc_object(&owner_id, "policy1", "documents", "doc1")
        .await
        .unwrap();

    // Check registration
    assert!(acp
        .is_doc_registered("policy1", "documents", "doc1")
        .await
        .unwrap());

    // Owner has all permissions
    let identity = Identity::Authenticated(owner_id);

    assert!(acp
        .check_doc_access(
            &identity,
            DocumentPermission::Read,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &identity,
            DocumentPermission::Update,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &identity,
            DocumentPermission::Delete,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_zanzibar_document_acp_sharing() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let acp = ZanzibarDocumentACP::new(store);

    let owner = test_did();
    let reader = test_did2();
    let owner_id = to_idid(&owner);
    let reader_id = to_idid(&reader);

    // Register document (use collection1 as policy_id for simplicity)
    acp.register_doc_object(&owner_id, "collection1", "collection1", "doc1")
        .await
        .unwrap();

    // Reader cannot access yet
    let reader_identity = Identity::Authenticated(reader_id.clone());
    assert!(!acp
        .check_doc_access(
            &reader_identity,
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap());

    // Owner shares with reader
    acp.add_actor_relationship(
        &owner_id,
        &reader_id,
        "collection1",
        "collection1",
        "doc1",
        "reader",
        &[],
    )
    .await
    .unwrap();

    // Reader can now read
    assert!(acp
        .check_doc_access(
            &reader_identity,
            DocumentPermission::Read,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap());

    // Reader still cannot update
    assert!(!acp
        .check_doc_access(
            &reader_identity,
            DocumentPermission::Update,
            "collection1",
            "collection1",
            "doc1",
        )
        .await
        .unwrap());
}

// =============================================================================
// Backward Compatibility Tests
// =============================================================================

#[tokio::test]
async fn test_local_document_acp_still_works() {
    let store = Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);

    let owner = test_did();
    let owner_id = to_idid(&owner);

    // Register document
    acp.register_doc_object(&owner_id, "policy1", "documents", "doc1")
        .await
        .unwrap();

    // Check registration
    assert!(acp
        .is_doc_registered("policy1", "documents", "doc1")
        .await
        .unwrap());

    // Owner has all permissions
    let identity = Identity::Authenticated(owner_id);

    assert!(acp
        .check_doc_access(
            &identity,
            DocumentPermission::Read,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap());
    assert!(acp
        .check_doc_access(
            &identity,
            DocumentPermission::Update,
            "policy1",
            "documents",
            "doc1",
        )
        .await
        .unwrap());
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[tokio::test]
async fn test_policy_not_found_error() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let engine = PermissionEngine::new(store);

    let did = test_did();
    let result = engine
        .check("nonexistent", "document", "doc1", "owner", &did)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_relation_not_found_error() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store);

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    engine.add_policy(&policy);

    let did = test_did();
    let result = engine
        .check("policy1", "document", "doc1", "nonexistent", &did)
        .await;

    assert!(result.is_err());
}

// =============================================================================
// Expression Parsing Tests
// =============================================================================

#[test]
fn test_parse_union_expression() {
    let expr = RelationExpression::parse("owner + reader").unwrap();
    match expr {
        RelationExpression::Union(exprs) => assert_eq!(exprs.len(), 2),
        _ => panic!("expected Union"),
    }
}

#[test]
fn test_parse_tuple_to_userset_expression() {
    let expr = RelationExpression::parse("parent->owner").unwrap();
    match expr {
        RelationExpression::TupleToUserset {
            tuple_relation,
            computed_relation,
        } => {
            assert_eq!(tuple_relation, "parent");
            assert_eq!(computed_relation, "owner");
        }
        _ => panic!("expected TupleToUserset"),
    }
}

#[test]
fn test_parse_complex_expression() {
    let expr = RelationExpression::parse("owner + parent->viewer").unwrap();
    match expr {
        RelationExpression::Union(exprs) => {
            assert_eq!(exprs.len(), 2);
            match &exprs[1] {
                RelationExpression::TupleToUserset { .. } => {}
                _ => panic!("expected TupleToUserset in second position"),
            }
        }
        _ => panic!("expected Union"),
    }
}
