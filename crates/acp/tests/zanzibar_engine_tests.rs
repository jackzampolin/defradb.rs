//! Tests for the Zanzibar permission engine.

use std::sync::Arc;

use acp::{
    MemoryZanzibarStore, PermissionCheckRequest, PermissionEngine, Policy, Relation,
    RelationExpression, Relationship, Resource, StepResult, Subject, ZanzibarStore,
};
use zanzibar::error::Error;
use zanzibar::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

fn test_did2() -> Did {
    Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
}

#[tokio::test]
async fn test_this_expression() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    engine.add_policy(&policy);

    let did = test_did();

    let rel = Relationship::with_entity("document", "doc1", "owner", did.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "document", "doc1", "owner", &did)
        .await
        .unwrap();
    assert!(result);

    let did2 = test_did2();
    let result = engine
        .check("policy1", "document", "doc1", "owner", &did2)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn test_computed_userset() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

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

    let owner_did = test_did();
    let reader_did = test_did2();

    let rel = Relationship::with_entity("document", "doc1", "owner", owner_did.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let rel = Relationship::with_entity("document", "doc1", "reader", reader_did.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "document", "doc1", "reader", &owner_did)
        .await
        .unwrap();
    assert!(result);

    let result = engine
        .check("policy1", "document", "doc1", "reader", &reader_did)
        .await
        .unwrap();
    assert!(result);
}

#[tokio::test]
async fn test_tuple_to_userset() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

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

    let rel = Relationship::with_entity("folder", "folder1", "owner", folder_owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let rel = Relationship::new(
        "file",
        "file1",
        "parent",
        Subject::entity_set("folder", "folder1", "owner"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "file", "file1", "reader", &folder_owner)
        .await
        .unwrap();
    assert!(result);

    let non_owner = test_did2();
    let result = engine
        .check("policy1", "file", "file1", "reader", &non_owner)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn test_intersection() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

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

    let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "document", "doc1", "editor", &user)
        .await
        .unwrap();
    assert!(!result);

    let rel = Relationship::with_entity("document", "doc1", "approved", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "document", "doc1", "editor", &user)
        .await
        .unwrap();
    assert!(result);
}

#[tokio::test]
async fn test_difference() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

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

    let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap();
    assert!(result);

    let rel = Relationship::with_entity("document", "doc1", "banned", user.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let result = engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap();
    assert!(!result);
}

#[tokio::test]
async fn test_wildcard() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("viewer")));

    engine.add_policy(&policy);

    let rel = Relationship::new("document", "doc1", "viewer", Subject::Wildcard);
    store.store_relationship("policy1", &rel).await.unwrap();

    let user = test_did();
    let result = engine
        .check("policy1", "document", "doc1", "viewer", &user)
        .await
        .unwrap();
    assert!(result);

    let user2 = test_did2();
    let result = engine
        .check("policy1", "document", "doc1", "viewer", &user2)
        .await
        .unwrap();
    assert!(result);
}

#[tokio::test]
async fn test_policy_not_found() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let engine = PermissionEngine::new(store);

    let did = test_did();
    let result = engine
        .check("nonexistent", "document", "doc1", "owner", &did)
        .await;

    assert!(matches!(result, Err(Error::PolicyNotFound(_))));
}

#[tokio::test]
async fn test_relation_not_found() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store);

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    engine.add_policy(&policy);

    let did = test_did();
    let result = engine
        .check("policy1", "document", "doc1", "nonexistent", &did)
        .await;

    assert!(matches!(result, Err(Error::RelationNotFound { .. })));
}

#[tokio::test]
async fn test_cycle_detection_returns_false() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::computed(
                "reader",
                RelationExpression::computed_userset("viewer"),
            ))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::computed_userset("reader"),
            )),
    );

    engine.add_policy(&policy);

    let did = test_did();

    let result = engine
        .check("policy1", "document", "doc1", "reader", &did)
        .await;

    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[tokio::test]
async fn test_typed_wildcard_permission() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("viewer")));

    engine.add_policy(&policy);

    let rel = Relationship::new(
        "document",
        "doc1",
        "viewer",
        Subject::typed_wildcard("user"),
    );
    store.store_relationship("policy1", &rel).await.unwrap();

    let did = test_did();
    let result = engine
        .check("policy1", "document", "doc1", "viewer", &did)
        .await
        .unwrap();
    assert!(result);
}

#[tokio::test]
async fn test_check_many_batch() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

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
    let reader = test_did2();

    let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let rel = Relationship::with_entity("document", "doc2", "reader", reader.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let requests = vec![
        PermissionCheckRequest::new("policy1", "document", "doc1", "owner", &owner),
        PermissionCheckRequest::new("policy1", "document", "doc1", "reader", &owner),
        PermissionCheckRequest::new("policy1", "document", "doc2", "reader", &reader),
        PermissionCheckRequest::new("policy1", "document", "doc2", "reader", &owner),
    ];

    let results = engine.check_many(&requests).await;

    assert_eq!(results.len(), 4);
    assert!(results[0].as_ref().unwrap());
    assert!(results[1].as_ref().unwrap());
    assert!(results[2].as_ref().unwrap());
    assert!(!results[3].as_ref().unwrap());
}

#[tokio::test]
async fn test_explain_granted() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

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
    let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
    store.store_relationship("policy1", &rel).await.unwrap();

    let explanation = engine
        .explain("policy1", "document", "doc1", "reader", &owner)
        .await
        .unwrap();

    assert!(explanation.granted);
    assert_eq!(explanation.resource, "document");
    assert_eq!(explanation.object_id, "doc1");
    assert_eq!(explanation.relation, "reader");
    assert!(!explanation.trace.steps.is_empty());

    let granted_steps: Vec<_> = explanation
        .trace
        .steps
        .iter()
        .filter(|s| matches!(s.result, StepResult::Granted))
        .collect();
    assert!(!granted_steps.is_empty());
}

#[tokio::test]
async fn test_explain_denied() {
    let store = Arc::new(MemoryZanzibarStore::new());
    let mut engine = PermissionEngine::new(store.clone());

    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    engine.add_policy(&policy);

    let user = test_did();

    let explanation = engine
        .explain("policy1", "document", "doc1", "owner", &user)
        .await
        .unwrap();

    assert!(!explanation.granted);
    assert!(!explanation.trace.steps.is_empty());

    let denied_steps: Vec<_> = explanation
        .trace
        .steps
        .iter()
        .filter(|s| matches!(s.result, StepResult::Denied))
        .collect();
    assert!(!denied_steps.is_empty());
}
