//! Stress tests and edge cases for Zanzibar permission model.
//!
//! These tests cover scenarios identified as potential issues in the PR review:
//! - Very deep nesting (10+ levels)
//! - Large fan-out (objects with many relations)
//! - Concurrent permission checks
//! - TupleToUserset edge cases
//! - Difference operand order verification
//! - Wildcard edge cases
//! - Cycle detection edge cases

use std::sync::Arc;

use acp::{
    MemoryZanzibarStore, PermissionEngine, Policy, Relation, RelationExpression, Relationship,
    Resource, Subject, ZanzibarStore,
};
use identity::Did;

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
// Deep Nesting Tests (10+ levels)
// =============================================================================

/// Test deeply nested folder hierarchy (10 levels deep).
/// file -> folder1 -> folder2 -> ... -> folder10
/// User who owns folder10 should be able to read file.
#[tokio::test]
async fn test_deep_folder_hierarchy_10_levels() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Create policy with file and folder resources
    // file.reader = parent->reader
    // folder.reader = _this + parent->reader + owner
    let policy = Policy::new("policy1", "Deep Hierarchy")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "reader"),
                )),
        )
        .with_resource(
            Resource::new("folder")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                        RelationExpression::tuple_to_userset("parent", "reader"),
                    ]),
                )),
        );

    engine.add_policy(&policy);

    let root_owner = test_did();

    // Create 10-level deep folder hierarchy
    // folder10 -> folder9 -> ... -> folder1 -> root_folder
    for i in (1..=10).rev() {
        let parent_name = if i == 10 {
            "root_folder".to_string()
        } else {
            format!("folder{}", i + 1)
        };

        let rel = Relationship::new(
            "folder",
            &format!("folder{}", i),
            "parent",
            Subject::entity_set("folder", &parent_name, "reader"),
        );
        store.store_relationship("policy1", &rel).await.unwrap();
    }

    // Root folder owner
    let rel = Relationship::with_entity("folder", "root_folder", "owner", root_owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // File in folder1 (deepest folder)
    let rel = Relationship::new(
        "file",
        "deep_file",
        "parent",
        Subject::entity_set("folder", "folder1", "reader"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Root owner should be able to read the deeply nested file
    let result = engine
        .check("policy1", "file", "deep_file", "reader", &root_owner)
        .await
        .unwrap();
    assert!(result, "Root owner should read file 10 levels deep");

    // Non-owner should not have access
    let non_owner = test_did2();
    let result = engine
        .check("policy1", "file", "deep_file", "reader", &non_owner)
        .await
        .unwrap();
    assert!(!result, "Non-owner should not read deeply nested file");
}

/// Test 15 levels of computed userset chains.
/// relation1 = relation2, relation2 = relation3, ..., relation15 = _this
#[tokio::test]
async fn test_deep_computed_userset_chain() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let mut resource = Resource::new("document");

    // Create chain: level1 -> level2 -> ... -> level15 -> base
    for i in 1..=15 {
        let name = format!("level{}", i);
        let next_name = if i == 15 {
            "base".to_string()
        } else {
            format!("level{}", i + 1)
        };

        resource = resource.with_relation(Relation::computed(
            &name,
            RelationExpression::computed_userset(&next_name),
        ));
    }

    // Base relation is direct
    resource = resource.with_relation(Relation::direct("base"));

    let policy = Policy::new("policy1", "Deep Computed").with_resource(resource);
    engine.add_policy(&policy);

    let user = test_did();

    // Store base relationship
    let rel = Relationship::with_entity("document", "doc1", "base", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // User should have level1 permission through 15 levels of indirection
    let result = engine
        .check("policy1", "document", "doc1", "level1", &user)
        .await
        .unwrap();
    assert!(result, "User should have access through 15-level chain");

    // Non-user should not have access
    let non_user = test_did2();
    let result = engine
        .check("policy1", "document", "doc1", "level1", &non_user)
        .await
        .unwrap();
    assert!(!result, "Non-user should not have access");
}

// =============================================================================
// Large Fan-Out Tests
// =============================================================================

/// Test object with 100 direct readers.
#[tokio::test]
async fn test_large_fanout_100_readers() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Large Fanout")
        .with_resource(Resource::new("document").with_relation(Relation::direct("reader")));

    engine.add_policy(&policy);

    // Create 100 readers
    for i in 0..100 {
        let did = Did::new(&format!(
            "did:key:z6Mk{}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            i
        ))
        .unwrap();
        let rel = Relationship::with_entity("document", "popular_doc", "reader", did);
        store.store_relationship("policy1", &rel).await.unwrap();
    }

    // Check that reader 50 has access
    let reader50 = Did::new("did:key:z6Mk50AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    let result = engine
        .check("policy1", "document", "popular_doc", "reader", &reader50)
        .await
        .unwrap();
    assert!(result, "Reader 50 should have access");

    // Check that non-reader doesn't have access
    let non_reader = Did::new("did:key:z6Mk999AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    let result = engine
        .check("policy1", "document", "popular_doc", "reader", &non_reader)
        .await
        .unwrap();
    assert!(!result, "Non-reader should not have access");
}

/// Test object with many nested group memberships.
/// document has 50 groups as readers, each group has different members.
#[tokio::test]
async fn test_large_fanout_group_memberships() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // document.reader = _this + parent->member
    // group.member = _this
    let policy = Policy::new("policy1", "Group Fanout")
        .with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::tuple_to_userset("parent", "member"),
                    ]),
                )),
        )
        .with_resource(Resource::new("group").with_relation(Relation::direct("member")));

    engine.add_policy(&policy);

    // Create 50 groups, each with unique members
    for i in 0..50 {
        let group_name = format!("group{}", i);

        // Add group as parent of document
        let rel = Relationship::new(
            "document",
            "shared_doc",
            "parent",
            Subject::entity_set("group", &group_name, "member"),
        );
        store.store_relationship("policy1", &rel).await.unwrap();

        // Add member to group
        let member = Did::new(&format!(
            "did:key:z6MkMember{}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            i
        ))
        .unwrap();
        let rel = Relationship::with_entity("group", &group_name, "member", member);
        store.store_relationship("policy1", &rel).await.unwrap();
    }

    // Member of group25 should have read access
    let group25_member =
        Did::new("did:key:z6MkMember25AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    let result = engine
        .check(
            "policy1",
            "document",
            "shared_doc",
            "reader",
            &group25_member,
        )
        .await
        .unwrap();
    assert!(result, "Group 25 member should have read access");

    // Non-member should not have access
    let non_member = Did::new("did:key:z6MkMember999AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
    let result = engine
        .check("policy1", "document", "shared_doc", "reader", &non_member)
        .await
        .unwrap();
    assert!(!result, "Non-member should not have access");
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

// =============================================================================
// Cycle Detection Edge Cases
// =============================================================================

/// Test legitimate deep traversal that looks like it could be a cycle.
#[tokio::test]
async fn test_deep_traversal_not_cycle() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Multiple resources that can link to each other but don't form cycles
    let policy = Policy::new("policy1", "Deep Traversal")
        .with_resource(Resource::new("org").with_relation(Relation::direct("admin")))
        .with_resource(
            Resource::new("team")
                .with_relation(Relation::direct("org_ref"))
                .with_relation(Relation::computed(
                    "admin",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::tuple_to_userset("org_ref", "admin"),
                    ]),
                )),
        )
        .with_resource(
            Resource::new("project")
                .with_relation(Relation::direct("team_ref"))
                .with_relation(Relation::computed(
                    "admin",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::tuple_to_userset("team_ref", "admin"),
                    ]),
                )),
        )
        .with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("project_ref"))
                .with_relation(Relation::computed(
                    "admin",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::tuple_to_userset("project_ref", "admin"),
                    ]),
                )),
        );

    engine.add_policy(&policy);

    let org_admin = test_did();

    // Org admin
    let rel = Relationship::with_entity("org", "org1", "admin", org_admin.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Team in org
    let rel = Relationship::new(
        "team",
        "team1",
        "org_ref",
        Subject::entity_set("org", "org1", "admin"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Project in team
    let rel = Relationship::new(
        "project",
        "project1",
        "team_ref",
        Subject::entity_set("team", "team1", "admin"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Document in project
    let rel = Relationship::new(
        "document",
        "doc1",
        "project_ref",
        Subject::entity_set("project", "project1", "admin"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Org admin should be document admin through the chain
    let result = engine
        .check("policy1", "document", "doc1", "admin", &org_admin)
        .await
        .unwrap();
    assert!(
        result,
        "Org admin should be document admin through hierarchy"
    );
}

/// Test that actual cycles in computed usersets are detected.
#[tokio::test]
async fn test_computed_userset_cycle_detection() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Create a cycle: relation_a -> relation_b -> relation_a
    let policy = Policy::new("policy1", "Cycle Detection").with_resource(
        Resource::new("document")
            .with_relation(Relation::computed(
                "relation_a",
                RelationExpression::computed_userset("relation_b"),
            ))
            .with_relation(Relation::computed(
                "relation_b",
                RelationExpression::computed_userset("relation_a"),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // This should detect the cycle and return an error
    let result = engine
        .check("policy1", "document", "doc1", "relation_a", &user)
        .await;

    assert!(result.is_err(), "Should detect cycle in computed userset");
}

/// Test that cycle detection doesn't block parallel branches.
#[tokio::test]
async fn test_cycle_detection_allows_parallel_branches() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Diamond pattern: viewer = branch_a + branch_b, both eventually check owner
    let policy = Policy::new("policy1", "Diamond Pattern").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::computed(
                "branch_a",
                RelationExpression::computed_userset("owner"),
            ))
            .with_relation(Relation::computed(
                "branch_b",
                RelationExpression::computed_userset("owner"),
            ))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::union(vec![
                    RelationExpression::computed_userset("branch_a"),
                    RelationExpression::computed_userset("branch_b"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let owner = test_did();

    let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should work - both branches check owner but it's not a cycle
    let result = engine
        .check("policy1", "document", "doc1", "viewer", &owner)
        .await
        .unwrap();
    assert!(result, "Diamond pattern should work");
}

// =============================================================================
// Complex Expression Tests
// =============================================================================

/// Test complex nested expression: ((a + b) - c) & d
#[tokio::test]
async fn test_complex_nested_expression() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // final = ((group_a + group_b) - blocked) & approved
    let policy = Policy::new("policy1", "Complex Expression").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("group_a"))
            .with_relation(Relation::direct("group_b"))
            .with_relation(Relation::direct("blocked"))
            .with_relation(Relation::direct("approved"))
            .with_relation(Relation::computed(
                "final",
                RelationExpression::intersection(vec![
                    RelationExpression::difference(
                        RelationExpression::union(vec![
                            RelationExpression::computed_userset("group_a"),
                            RelationExpression::computed_userset("group_b"),
                        ]),
                        RelationExpression::computed_userset("blocked"),
                    ),
                    RelationExpression::computed_userset("approved"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // User in group_a and approved -> should have access
    let rel = Relationship::with_entity("document", "doc1", "group_a", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();
    let rel = Relationship::with_entity("document", "doc1", "approved", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    assert!(engine
        .check("policy1", "document", "doc1", "final", &user)
        .await
        .unwrap());

    // Block user -> should lose access
    let rel = Relationship::with_entity("document", "doc1", "blocked", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    assert!(!engine
        .check("policy1", "document", "doc1", "final", &user)
        .await
        .unwrap());
}

/// Test union short-circuit behavior.
#[tokio::test]
async fn test_union_short_circuit() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = fast_check + slow_check
    // If fast_check succeeds, slow_check shouldn't matter
    let policy = Policy::new("policy1", "Short Circuit").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("fast_check"))
            .with_relation(Relation::direct("slow_check"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::union(vec![
                    RelationExpression::computed_userset("fast_check"),
                    RelationExpression::computed_userset("slow_check"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // Only set fast_check
    let rel = Relationship::with_entity("document", "doc1", "fast_check", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should succeed via fast_check
    assert!(engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap());
}

/// Test intersection short-circuit behavior.
#[tokio::test]
async fn test_intersection_short_circuit() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = required1 & required2
    // If required1 fails, required2 shouldn't matter
    let policy = Policy::new("policy1", "Intersection Short Circuit").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("required1"))
            .with_relation(Relation::direct("required2"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::intersection(vec![
                    RelationExpression::computed_userset("required1"),
                    RelationExpression::computed_userset("required2"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // Only set required2 (required1 missing)
    let rel = Relationship::with_entity("document", "doc1", "required2", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should fail because required1 is missing
    assert!(!engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap());
}

// =============================================================================
// Policy Validation Tests
// =============================================================================

/// Test that policy validation catches invalid computed userset references.
#[test]
fn test_policy_validation_invalid_computed_userset() {
    let policy = Policy::new("policy1", "Invalid Policy").with_resource(
        Resource::new("document").with_relation(Relation::computed(
            "viewer",
            RelationExpression::computed_userset("nonexistent"),
        )),
    );

    let result = policy.validate();
    assert!(
        result.is_err(),
        "Should reject reference to nonexistent relation"
    );
}

/// Test that policy validation catches invalid tuple relation in TTU.
#[test]
fn test_policy_validation_invalid_ttu_tuple_relation() {
    let policy = Policy::new("policy1", "Invalid TTU Policy").with_resource(
        Resource::new("document").with_relation(Relation::computed(
            "viewer",
            RelationExpression::tuple_to_userset("nonexistent", "owner"),
        )),
    );

    let result = policy.validate();
    assert!(
        result.is_err(),
        "Should reject TTU with nonexistent tuple relation"
    );
}

/// Test that valid policy passes validation.
#[test]
fn test_policy_validation_valid() {
    let policy = Policy::new("policy1", "Valid Policy").with_resource(
        Resource::new("document")
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

    let result = policy.validate();
    assert!(result.is_ok(), "Valid policy should pass validation");
}
