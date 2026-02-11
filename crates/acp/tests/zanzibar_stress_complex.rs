//! Complex expression, validation, and EntitySet stress tests for the Zanzibar permission model.

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

// =============================================================================
// TTU Computed Relation vs EntitySet Relation Tests (Fix Verification)
// =============================================================================

/// Test TTU where computed_relation differs from EntitySet's stored relation.
/// This was a bug where the EntitySet's relation was used instead of the TTU's
/// computed_relation.
///
/// Scenario:
/// - TTU rule: file.reader = parent->owner
/// - EntitySet subject: folder:folder1#viewer (relation=viewer, NOT owner)
/// - Expected: Check folder1's owner relation (computed_relation), not viewer
#[tokio::test]
async fn test_ttu_computed_relation_differs_from_entityset() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // file.reader = parent->owner (computed_relation = "owner")
    // folder has both owner and viewer relations
    let policy = Policy::new("policy1", "TTU Relation Test")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "owner"),
                )),
        )
        .with_resource(
            Resource::new("folder")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("viewer")),
        );

    engine.add_policy(&policy);

    let folder_owner = test_did();
    let folder_viewer = test_did2();

    // folder1 has owner
    let rel = Relationship::with_entity("folder", "folder1", "owner", folder_owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // folder1 has viewer (different user)
    let rel = Relationship::with_entity("folder", "folder1", "viewer", folder_viewer.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    // File has parent pointing to folder1 with relation="viewer"
    // Note: The EntitySet has relation="viewer" but TTU says computed_relation="owner"
    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("folder", "folder1", "viewer"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // folder_owner should be able to read (TTU checks owner relation on folder1)
    let result = engine
        .check("policy1", "file", "file1", "reader", &folder_owner)
        .await
        .unwrap();
    assert!(
        result,
        "folder owner should read file (TTU uses computed_relation='owner')"
    );

    // folder_viewer should NOT be able to read (TTU checks owner, not viewer)
    let result = engine
        .check("policy1", "file", "file1", "reader", &folder_viewer)
        .await
        .unwrap();
    assert!(
        !result,
        "folder viewer should NOT read file (TTU uses owner, not viewer)"
    );
}

/// Test TTU with wildcard on the tuple_relation grants access.
/// When the tuple_relation has a wildcard subject, TTU should grant access
/// because anyone matches the wildcard.
#[tokio::test]
async fn test_ttu_with_wildcard_on_tuple_relation() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // file.reader = parent->viewer
    let policy = Policy::new("policy1", "TTU Wildcard Test")
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

    // File has a wildcard parent (means everyone is a parent)
    let rel = Relationship::new("file", "public_file", "parent", Subject::Wildcard);
    store.store_relationship("policy1", &rel).await.unwrap();

    // Any user should be able to read (wildcard on tuple_relation)
    let result = engine
        .check("policy1", "file", "public_file", "reader", &any_user)
        .await
        .unwrap();
    assert!(
        result,
        "Wildcard on tuple_relation should grant access via TTU"
    );
}

/// Test TTU with typed wildcard on the tuple_relation grants access.
#[tokio::test]
async fn test_ttu_with_typed_wildcard_on_tuple_relation() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    // file.reader = parent->viewer
    let policy = Policy::new("policy1", "TTU Typed Wildcard Test")
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

    // File has a typed wildcard parent (folder:*)
    let rel = Relationship::new(
        "file",
        "public_file",
        "parent",
        Subject::typed_wildcard("folder"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    // Any user should be able to read (typed wildcard on tuple_relation)
    let result = engine
        .check("policy1", "file", "public_file", "reader", &any_user)
        .await
        .unwrap();
    assert!(
        result,
        "Typed wildcard on tuple_relation should grant access via TTU"
    );
}

// =============================================================================
// EntitySet Validation Tests (Fix Verification)
// =============================================================================

/// Test that relationship validation catches invalid EntitySet references.
#[test]
fn test_relationship_validation_invalid_entityset_resource() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("viewer")));

    // Relationship with EntitySet referencing non-existent resource
    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("nonexistent_resource", "obj1", "viewer"),
    );

    let result = rel.validate(&policy);
    assert!(
        result.is_err(),
        "Should reject EntitySet with non-existent resource"
    );
}

/// Test that relationship validation catches invalid EntitySet relation.
#[test]
fn test_relationship_validation_invalid_entityset_relation() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("viewer")));

    // Relationship with EntitySet referencing non-existent relation
    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("folder", "folder1", "nonexistent_relation"),
    );

    let result = rel.validate(&policy);
    assert!(
        result.is_err(),
        "Should reject EntitySet with non-existent relation"
    );
}

/// Test that valid relationship passes validation.
#[test]
fn test_relationship_validation_valid() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::tuple_to_userset("parent", "viewer"),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("viewer")));

    // Valid relationship with EntitySet
    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("folder", "folder1", "viewer"),
    );

    let result = rel.validate(&policy);
    assert!(result.is_ok(), "Valid relationship should pass validation");

    // Valid relationship with direct entity
    let rel = Relationship::with_entity("file", "file1", "parent", test_did());
    let result = rel.validate(&policy);
    assert!(
        result.is_ok(),
        "Direct entity relationship should pass validation"
    );

    // Valid relationship with wildcard
    let rel = Relationship::new("file", "file1", "parent", Subject::Wildcard);
    let result = rel.validate(&policy);
    assert!(
        result.is_ok(),
        "Wildcard relationship should pass validation"
    );
}

/// Test that relationship validation also validates the relationship's own resource/relation.
#[test]
fn test_relationship_validation_invalid_own_relation() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("file").with_relation(Relation::direct("owner")));

    // Relationship with non-existent relation on the resource
    let rel = Relationship::with_entity("file", "file1", "nonexistent", test_did());

    let result = rel.validate(&policy);
    assert!(
        result.is_err(),
        "Should reject relationship with non-existent relation"
    );
}
