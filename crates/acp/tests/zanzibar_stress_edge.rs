//! Edge case stress tests: TTU, difference, wildcard, and concurrent permission checks.

use std::sync::Arc;

use acp::{
    MemoryZanzibarStore, PermissionEngine, Policy, Relation, RelationExpression, Relationship,
    Resource, Subject, ZanzibarStore,
};
use zanzibar::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
}

fn test_did3() -> Did {
    Did::new("did:key:z6MkfGHSo4sV6o9q3nX5M9NdmZ6T3qXxR7C4mYvG7mWbQDYz").unwrap()
}

// =============================================================================
// Concurrent Permission Check Tests
// =============================================================================

/// Test concurrent permission checks on the same object.
#[tokio::test]
async fn test_concurrent_permission_checks() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Concurrent Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("reader")));

    engine.add_policy(&policy);

    // Store reader relationship
    let user = test_did();
    let rel = Relationship::with_entity("document", "doc1", "reader", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let engine = Arc::new(engine);

    // Run 100 concurrent permission checks
    let mut handles = vec![];
    for _ in 0..100 {
        let engine = Arc::clone(&engine);
        let user = user.clone();
        handles.push(tokio::spawn(async move {
            engine
                .check("policy1", "document", "doc1", "reader", &user)
                .await
        }));
    }

    // All checks should succeed
    for handle in handles {
        let result = handle.await.unwrap().unwrap();
        assert!(result, "All concurrent checks should return true");
    }
}

/// Test concurrent permission checks with mixed results.
#[tokio::test]
async fn test_concurrent_mixed_permissions() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Concurrent Mixed")
        .with_resource(Resource::new("document").with_relation(Relation::direct("reader")));

    engine.add_policy(&policy);

    let authorized = test_did();
    let unauthorized = test_did2();

    let rel = Relationship::with_entity("document", "doc1", "reader", authorized.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let engine = Arc::new(engine);

    // Run concurrent checks for both users
    let mut handles = vec![];
    for i in 0..50 {
        let engine = Arc::clone(&engine);
        let user = if i % 2 == 0 {
            authorized.clone()
        } else {
            unauthorized.clone()
        };
        let expected = i % 2 == 0;
        handles.push(tokio::spawn(async move {
            let result = engine
                .check("policy1", "document", "doc1", "reader", &user)
                .await
                .unwrap();
            (result, expected)
        }));
    }

    for handle in handles {
        let (result, expected) = handle.await.unwrap();
        assert_eq!(
            result, expected,
            "Concurrent check should return correct result"
        );
    }
}

// =============================================================================
// TupleToUserset Edge Cases
// =============================================================================

/// Test TupleToUserset with multiple matching targets (union semantics).
#[tokio::test]
async fn test_tuple_to_userset_multiple_targets() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // file.reader = parent->viewer
    // folder.viewer = _this
    let policy = Policy::new("policy1", "Multi-Target TTU")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("viewer")));

    engine.add_policy(&policy);

    let user1 = test_did();
    let user2 = test_did2();

    // File has two parent folders
    let rel = Relationship::new(
        "file",
        "shared_file",
        "parent",
        Subject::entity_set("folder", "folder_a", "viewer"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    let rel = Relationship::new(
        "file",
        "shared_file",
        "parent",
        Subject::entity_set("folder", "folder_b", "viewer"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // User1 is viewer of folder_a
    let rel = Relationship::with_entity("folder", "folder_a", "viewer", user1.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // User2 is viewer of folder_b
    let rel = Relationship::with_entity("folder", "folder_b", "viewer", user2.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Both users should be able to read the file
    assert!(engine
        .check("policy1", "file", "shared_file", "reader", &user1)
        .await
        .unwrap());
    assert!(engine
        .check("policy1", "file", "shared_file", "reader", &user2)
        .await
        .unwrap());

    // Non-viewer should not have access
    let non_viewer = test_did3();
    assert!(!engine
        .check("policy1", "file", "shared_file", "reader", &non_viewer)
        .await
        .unwrap());
}

/// Test TupleToUserset with no matching targets.
#[tokio::test]
async fn test_tuple_to_userset_no_targets() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "No Targets TTU")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("viewer")));

    engine.add_policy(&policy);

    // File has no parent relation set
    let user = test_did();

    let result = engine
        .check("policy1", "file", "orphan_file", "reader", &user)
        .await
        .unwrap();
    assert!(!result, "File with no parent should deny access");
}

/// Test TupleToUserset with self-referential relations.
#[tokio::test]
async fn test_tuple_to_userset_self_reference() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Folder can have parent -> another folder
    // folder.viewer = owner + parent->viewer
    let policy = Policy::new("policy1", "Self Reference TTU").with_resource(
        Resource::new("folder")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::direct("parent"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::union(vec![
                    RelationExpression::computed_userset("owner"),
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // Create hierarchy: folder_c -> folder_b -> folder_a
    let rel = Relationship::new(
        "folder",
        "folder_c",
        "parent",
        Subject::entity_set("folder", "folder_b", "viewer"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    let rel = Relationship::new(
        "folder",
        "folder_b",
        "parent",
        Subject::entity_set("folder", "folder_a", "viewer"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // User owns folder_a
    let rel = Relationship::with_entity("folder", "folder_a", "owner", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // User should be able to view folder_c through the chain
    let result = engine
        .check("policy1", "folder", "folder_c", "viewer", &user)
        .await
        .unwrap();
    assert!(result, "User should view folder_c through hierarchy");
}

// =============================================================================
// Difference Operand Order Tests
// =============================================================================

/// Verify that difference is `base - subtract` not `subtract - base`.
#[tokio::test]
async fn test_difference_operand_order() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = member - banned (member AND NOT banned)
    let policy = Policy::new("policy1", "Difference Order").with_resource(
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

    let member_only = test_did();
    let banned_only = test_did2();
    let both = test_did3();

    // member_only is just a member
    let rel = Relationship::with_entity("document", "doc1", "member", member_only.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // banned_only is just banned (not a member)
    let rel = Relationship::with_entity("document", "doc1", "banned", banned_only.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // both is both member and banned
    let rel = Relationship::with_entity("document", "doc1", "member", both.clone());
    store.store_relationship("policy1", &rel).await.unwrap();
    let rel = Relationship::with_entity("document", "doc1", "banned", both.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // member_only: member=true, banned=false -> viewer=true (member - banned = true - false = true)
    assert!(
        engine
            .check("policy1", "document", "doc1", "viewer", &member_only)
            .await
            .unwrap(),
        "Member-only user should be viewer"
    );

    // banned_only: member=false, banned=true -> viewer=false (false - true = false)
    assert!(
        !engine
            .check("policy1", "document", "doc1", "viewer", &banned_only)
            .await
            .unwrap(),
        "Banned-only user should not be viewer"
    );

    // both: member=true, banned=true -> viewer=false (true - true = false)
    assert!(
        !engine
            .check("policy1", "document", "doc1", "viewer", &both)
            .await
            .unwrap(),
        "Banned member should not be viewer"
    );
}

/// Test nested difference expressions.
#[tokio::test]
async fn test_nested_difference() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = (member - banned) - suspended
    // Only non-banned, non-suspended members can view
    let policy = Policy::new("policy1", "Nested Difference").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("member"))
            .with_relation(Relation::direct("banned"))
            .with_relation(Relation::direct("suspended"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::difference(
                    RelationExpression::difference(
                        RelationExpression::computed_userset("member"),
                        RelationExpression::computed_userset("banned"),
                    ),
                    RelationExpression::computed_userset("suspended"),
                ),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // User is member
    let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // User can view (not banned, not suspended)
    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap());

    // Suspend user
    let rel = Relationship::with_entity("document", "doc1", "suspended", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // User can no longer view
    assert!(!engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap());
}

// =============================================================================
// Wildcard Edge Cases
// =============================================================================

/// Test wildcard combined with explicit deny.
#[tokio::test]
async fn test_wildcard_with_explicit_deny() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // public_viewer = _this - blocked
    // Anyone can view unless explicitly blocked
    let policy = Policy::new("policy1", "Wildcard Deny").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("viewer"))
            .with_relation(Relation::direct("blocked"))
            .with_relation(Relation::computed(
                "public_viewer",
                RelationExpression::difference(
                    RelationExpression::computed_userset("viewer"),
                    RelationExpression::computed_userset("blocked"),
                ),
            )),
    );

    engine.add_policy(&policy);

    let normal_user = test_did();
    let blocked_user = test_did2();

    // Public access via wildcard
    let rel = Relationship::new("document", "public_doc", "viewer", Subject::Wildcard);
    store.store_relationship("policy1", &rel).await.unwrap();

    // Block specific user
    let rel = Relationship::with_entity("document", "public_doc", "blocked", blocked_user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Normal user can view
    assert!(engine
        .check(
            "policy1",
            "document",
            "public_doc",
            "public_viewer",
            &normal_user
        )
        .await
        .unwrap());

    // Blocked user cannot view (even though public)
    assert!(!engine
        .check(
            "policy1",
            "document",
            "public_doc",
            "public_viewer",
            &blocked_user
        )
        .await
        .unwrap());
}

/// Test wildcard in intersection.
#[tokio::test]
async fn test_wildcard_in_intersection() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = public & approved
    // Even public access requires approval
    let policy = Policy::new("policy1", "Wildcard Intersection").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("public"))
            .with_relation(Relation::direct("approved"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::intersection(vec![
                    RelationExpression::computed_userset("public"),
                    RelationExpression::computed_userset("approved"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let approved_user = test_did();
    let unapproved_user = test_did2();

    // Public access
    let rel = Relationship::new("document", "gated_doc", "public", Subject::Wildcard);
    store.store_relationship("policy1", &rel).await.unwrap();

    // Approve one user
    let rel = Relationship::with_entity("document", "gated_doc", "approved", approved_user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Approved user can view
    assert!(engine
        .check("policy1", "document", "gated_doc", "viewer", &approved_user)
        .await
        .unwrap());

    // Unapproved user cannot view (even though public)
    assert!(!engine
        .check(
            "policy1",
            "document",
            "gated_doc",
            "viewer",
            &unapproved_user
        )
        .await
        .unwrap());
}

/// Test wildcard in TupleToUserset chain.
#[tokio::test]
async fn test_wildcard_in_tuple_to_userset() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // file.reader = parent->viewer
    // folder.viewer = _this (can be wildcard)
    let policy = Policy::new("policy1", "Wildcard TTU")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("viewer")));

    engine.add_policy(&policy);

    let any_user = test_did();

    // File in public folder
    let rel = Relationship::new(
        "file",
        "public_file",
        "parent",
        Subject::entity_set("folder", "public_folder", "viewer"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Folder has wildcard viewer
    let rel = Relationship::new("folder", "public_folder", "viewer", Subject::Wildcard);
    store.store_relationship("policy1", &rel).await.unwrap();

    // Any user should be able to read file
    assert!(engine
        .check("policy1", "file", "public_file", "reader", &any_user)
        .await
        .unwrap());
}
