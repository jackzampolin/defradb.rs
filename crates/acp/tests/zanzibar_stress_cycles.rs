//! Cycle detection edge cases for the Zanzibar permission model.

use std::sync::Arc;

use acp::{
    MemoryZanzibarStore, PermissionEngine, Policy, Relation, RelationExpression, Relationship,
    Resource, Subject, ZanzibarStore,
};
use zanzibar::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
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

    // Cycle detection returns false (unauthorized), not error
    // This matches Go zanzi behavior: cycles terminate the branch
    // with "not authorized" rather than failing
    let result = engine
        .check("policy1", "document", "doc1", "relation_a", &user)
        .await;

    assert!(result.is_ok(), "Cycle detection should not error");
    assert!(!result.unwrap(), "Cycle should return false (unauthorized)");
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

/// Test self-referential cycle: relation_a -> relation_a
#[tokio::test]
async fn test_self_referential_cycle() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Self-referential: relation_a points to itself
    let policy = Policy::new("policy1", "Self Cycle").with_resource(
        Resource::new("document").with_relation(Relation::computed(
            "relation_a",
            RelationExpression::computed_userset("relation_a"),
        )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // Should return false (unauthorized), not error
    let result = engine
        .check("policy1", "document", "doc1", "relation_a", &user)
        .await;

    assert!(result.is_ok(), "Self-referential cycle should not error");
    assert!(
        !result.unwrap(),
        "Self-referential cycle should return false"
    );
}

/// Test three-way cycle: A -> B -> C -> A
#[tokio::test]
async fn test_three_way_cycle() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // Three-way cycle: A -> B -> C -> A
    let policy = Policy::new("policy1", "Three Way Cycle").with_resource(
        Resource::new("document")
            .with_relation(Relation::computed(
                "relation_a",
                RelationExpression::computed_userset("relation_b"),
            ))
            .with_relation(Relation::computed(
                "relation_b",
                RelationExpression::computed_userset("relation_c"),
            ))
            .with_relation(Relation::computed(
                "relation_c",
                RelationExpression::computed_userset("relation_a"),
            )),
    );

    engine.add_policy(&policy);

    let user = test_did();

    // All three should return false
    for relation in &["relation_a", "relation_b", "relation_c"] {
        let result = engine
            .check("policy1", "document", "doc1", relation, &user)
            .await;
        assert!(
            result.is_ok(),
            "Three-way cycle on {} should not error",
            relation
        );
        assert!(
            !result.unwrap(),
            "Three-way cycle on {} should return false",
            relation
        );
    }
}

/// Test cycle within union branch - one branch cycles, another succeeds
#[tokio::test]
async fn test_cycle_in_union_branch_other_succeeds() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = cycle_branch + direct_owner
    // cycle_branch -> other_cycle -> cycle_branch (cycles)
    // but direct_owner is a direct relation that should work
    let policy = Policy::new("policy1", "Union With Cycle").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("direct_owner"))
            .with_relation(Relation::computed(
                "cycle_branch",
                RelationExpression::computed_userset("other_cycle"),
            ))
            .with_relation(Relation::computed(
                "other_cycle",
                RelationExpression::computed_userset("cycle_branch"),
            ))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::union(vec![
                    RelationExpression::computed_userset("cycle_branch"),
                    RelationExpression::computed_userset("direct_owner"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let owner = test_did();

    // Add direct owner
    let rel = Relationship::with_entity("document", "doc1", "direct_owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should succeed via direct_owner branch even though cycle_branch cycles
    let result = engine
        .check("policy1", "document", "doc1", "viewer", &owner)
        .await;

    assert!(
        result.is_ok(),
        "Union with cycle branch should not error: {:?}",
        result
    );
    assert!(
        result.unwrap(),
        "Should succeed via non-cycling branch in union"
    );
}

/// Test cycle within intersection - cycle causes overall false
#[tokio::test]
async fn test_cycle_in_intersection_branch() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // viewer = direct_owner & cycle_branch
    // Both must be true, but cycle_branch cycles so returns false
    let policy = Policy::new("policy1", "Intersection With Cycle").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("direct_owner"))
            .with_relation(Relation::computed(
                "cycle_branch",
                RelationExpression::computed_userset("cycle_branch"), // self-cycle
            ))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::intersection(vec![
                    RelationExpression::computed_userset("direct_owner"),
                    RelationExpression::computed_userset("cycle_branch"),
                ]),
            )),
    );

    engine.add_policy(&policy);

    let owner = test_did();

    // Add direct owner
    let rel = Relationship::with_entity("document", "doc1", "direct_owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should fail because cycle_branch returns false
    let result = engine
        .check("policy1", "document", "doc1", "viewer", &owner)
        .await;

    assert!(
        result.is_ok(),
        "Intersection with cycle should not error: {:?}",
        result
    );
    assert!(
        !result.unwrap(),
        "Intersection should fail when one branch cycles"
    );
}

/// Test TTU cycle detection across different object types
#[tokio::test]
async fn test_ttu_cross_resource_cycle() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // file.admin -> folder.admin (via parent TTU)
    // folder.admin -> file.admin (via child TTU)
    // This creates a cycle across different resource types
    let policy = Policy::new("policy1", "TTU Cross Cycle")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "admin",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::tuple_to_userset("parent", "admin"),
                    ]),
                )),
        )
        .with_resource(
            Resource::new("folder")
                .with_relation(Relation::direct("child"))
                .with_relation(Relation::computed(
                    "admin",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::tuple_to_userset("child", "admin"),
                    ]),
                )),
        );

    engine.add_policy(&policy);

    let user = test_did();

    // Create cycle: file1.parent -> folder1, folder1.child -> file1
    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("folder", "folder1", "admin"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    let rel = Relationship::new(
        "folder",
        "folder1",
        "child",
        Subject::entity_set("file", "file1", "admin"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should return false (cycle detected), not error
    let result = engine
        .check("policy1", "file", "file1", "admin", &user)
        .await;

    assert!(result.is_ok(), "TTU cycle should not error: {:?}", result);
    assert!(!result.unwrap(), "TTU cycle should return false");
}

/// Test that breaking a cycle with actual permission grants access
#[tokio::test]
async fn test_cycle_with_base_case() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // admin = _this + computed(admin)
    // This is technically self-referential, but _this provides a base case
    let policy = Policy::new("policy1", "Cycle With Base").with_resource(
        Resource::new("document").with_relation(Relation::computed(
            "admin",
            RelationExpression::union(vec![
                RelationExpression::this(),
                RelationExpression::computed_userset("admin"), // self-reference
            ]),
        )),
    );

    engine.add_policy(&policy);

    let admin = test_did();

    // Grant direct admin
    let rel = Relationship::with_entity("document", "doc1", "admin", admin.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // Should succeed via _this branch, even though there's a self-referential branch
    let result = engine
        .check("policy1", "document", "doc1", "admin", &admin)
        .await;

    assert!(
        result.is_ok(),
        "Cycle with base case should not error: {:?}",
        result
    );
    assert!(
        result.unwrap(),
        "Should succeed via _this even with self-referential branch"
    );
}
