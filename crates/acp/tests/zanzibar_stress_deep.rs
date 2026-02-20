//! Deep nesting and large fan-out stress tests for the Zanzibar permission model.

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
            format!("folder{}", i),
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
        let did = Did::new(format!(
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
        let member = Did::new(format!(
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
