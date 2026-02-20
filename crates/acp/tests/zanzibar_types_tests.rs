//! Tests for Zanzibar core types.

use acp::{
    Policy, Relation, RelationExpression, Relationship, Resource, Subject, SubjectRestriction,
};
use zanzibar::error::Error;
use zanzibar::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

#[test]
fn test_subject_entity() {
    let did = test_did();
    let subject = Subject::entity(did.clone());

    assert!(subject.is_entity());
    assert!(!subject.is_entity_set());
    assert!(!subject.is_wildcard());
    assert_eq!(subject.as_entity(), Some(&did));
}

#[test]
fn test_subject_entity_set() {
    let subject = Subject::entity_set("folder", "folder123", "owner");

    assert!(!subject.is_entity());
    assert!(subject.is_entity_set());
    assert!(!subject.is_wildcard());
    assert_eq!(subject.to_string(), "folder:folder123#owner");
}

#[test]
fn test_subject_wildcard() {
    let subject = Subject::wildcard();

    assert!(!subject.is_entity());
    assert!(!subject.is_entity_set());
    assert!(!subject.is_typed_wildcard());
    assert!(subject.is_wildcard());
    assert!(subject.is_any_wildcard());
    assert_eq!(subject.to_string(), "*");
}

#[test]
fn test_subject_typed_wildcard() {
    let subject = Subject::typed_wildcard("user");

    assert!(!subject.is_entity());
    assert!(!subject.is_entity_set());
    assert!(subject.is_typed_wildcard());
    assert!(!subject.is_wildcard());
    assert!(subject.is_any_wildcard());
    assert_eq!(subject.as_typed_wildcard_resource(), Some("user"));
    assert_eq!(subject.to_string(), "user:*");
}

#[test]
fn test_relationship_storage_key() {
    let did = test_did();
    let rel = Relationship::with_entity("document", "doc123", "owner", did);

    let key = rel.storage_key();
    assert!(key.starts_with("/rel/document/doc123/owner/"));
}

#[test]
fn test_relationship_display() {
    let did = test_did();
    let rel = Relationship::with_entity("document", "doc123", "reader", did);

    let display = rel.to_string();
    assert!(display.contains("document:doc123#reader@"));
}

#[test]
fn test_policy_builder() {
    let policy = Policy::new("policy1", "Test Policy").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::direct("reader")),
    );

    assert_eq!(policy.id, "policy1");
    assert_eq!(policy.resources.len(), 1);

    let doc = policy.get_resource("document").unwrap();
    assert_eq!(doc.relations.len(), 2);
    assert!(doc.get_relation("owner").is_some());
    assert!(doc.get_relation("reader").is_some());
}

#[test]
fn test_policy_serde() {
    let policy = Policy::new("policy1", "Test Policy")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    let json = serde_json::to_string(&policy).unwrap();
    let parsed: Policy = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.id, policy.id);
    assert_eq!(parsed.name, policy.name);
    assert_eq!(parsed.resources.len(), 1);
}

#[test]
fn test_subject_serde() {
    let subjects = vec![
        Subject::entity(test_did()),
        Subject::entity_set("folder", "f1", "owner"),
        Subject::wildcard(),
    ];

    for subject in subjects {
        let json = serde_json::to_string(&subject).unwrap();
        let parsed: Subject = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, subject);
    }
}

#[test]
fn test_relationship_serde() {
    let rel = Relationship::with_entity("document", "doc123", "owner", test_did());

    let json = serde_json::to_string(&rel).unwrap();
    let parsed: Relationship = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.resource, rel.resource);
    assert_eq!(parsed.object_id, rel.object_id);
    assert_eq!(parsed.relation, rel.relation);
    assert_eq!(parsed.subject, rel.subject);
}

#[test]
fn test_subject_restriction_entity_accepts_entity() {
    let restriction = SubjectRestriction::Entity;
    let subject = Subject::entity(test_did());
    assert!(restriction.satisfies(&subject).is_ok());
}

#[test]
fn test_subject_restriction_entity_rejects_entity_set() {
    let restriction = SubjectRestriction::Entity;
    let subject = Subject::entity_set("folder", "f1", "owner");
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_entity_rejects_wildcard() {
    let restriction = SubjectRestriction::Entity;
    let subject = Subject::wildcard();
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_entity_set_accepts_matching() {
    let restriction = SubjectRestriction::EntitySet {
        resource: "folder".to_string(),
        relation: "owner".to_string(),
    };
    let subject = Subject::entity_set("folder", "f1", "owner");
    assert!(restriction.satisfies(&subject).is_ok());
}

#[test]
fn test_subject_restriction_entity_set_rejects_wrong_resource() {
    let restriction = SubjectRestriction::EntitySet {
        resource: "folder".to_string(),
        relation: "owner".to_string(),
    };
    let subject = Subject::entity_set("document", "d1", "owner");
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_entity_set_rejects_wrong_relation() {
    let restriction = SubjectRestriction::EntitySet {
        resource: "folder".to_string(),
        relation: "owner".to_string(),
    };
    let subject = Subject::entity_set("folder", "f1", "reader");
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_entity_set_rejects_entity() {
    let restriction = SubjectRestriction::EntitySet {
        resource: "folder".to_string(),
        relation: "owner".to_string(),
    };
    let subject = Subject::entity(test_did());
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_typed_wildcard_accepts_matching() {
    let restriction = SubjectRestriction::TypedWildcard {
        resource: "user".to_string(),
    };
    let subject = Subject::typed_wildcard("user");
    assert!(restriction.satisfies(&subject).is_ok());
}

#[test]
fn test_subject_restriction_typed_wildcard_rejects_wrong_resource() {
    let restriction = SubjectRestriction::TypedWildcard {
        resource: "user".to_string(),
    };
    let subject = Subject::typed_wildcard("admin");
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_typed_wildcard_rejects_untyped() {
    let restriction = SubjectRestriction::TypedWildcard {
        resource: "user".to_string(),
    };
    let subject = Subject::wildcard();
    assert!(restriction.satisfies(&subject).is_err());
}

#[test]
fn test_subject_restriction_any_accepts_all() {
    let restriction = SubjectRestriction::Any;
    assert!(restriction.satisfies(&Subject::entity(test_did())).is_ok());
    assert!(restriction
        .satisfies(&Subject::entity_set("f", "o", "r"))
        .is_ok());
    assert!(restriction.satisfies(&Subject::wildcard()).is_ok());
    assert!(restriction
        .satisfies(&Subject::typed_wildcard("user"))
        .is_ok());
}

#[test]
fn test_relationship_validate_enforces_subject_restriction() {
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner").with_restriction(SubjectRestriction::Entity)),
    );

    let valid_rel = Relationship::with_entity("document", "doc1", "owner", test_did());
    assert!(valid_rel.validate(&policy).is_ok());

    let invalid_rel = Relationship::new("document", "doc1", "owner", Subject::wildcard());
    let result = invalid_rel.validate(&policy);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::SubjectRestrictionViolation { .. }),
        "Expected SubjectRestrictionViolation, got {:?}",
        err
    );
}

#[test]
fn test_relationship_validate_allows_when_no_restriction() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

    let rel1 = Relationship::with_entity("document", "doc1", "owner", test_did());
    assert!(rel1.validate(&policy).is_ok());

    let rel2 = Relationship::new("document", "doc1", "owner", Subject::wildcard());
    assert!(rel2.validate(&policy).is_ok());

    let rel3 = Relationship::new("document", "doc1", "owner", Subject::typed_wildcard("user"));
    assert!(rel3.validate(&policy).is_ok());
}

#[test]
fn test_relationship_validate_entity_set_restriction() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(
            Relation::direct("parent").with_restriction(SubjectRestriction::EntitySet {
                resource: "folder".to_string(),
                relation: "owner".to_string(),
            }),
        ))
        .with_resource(
            Resource::new("folder")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("reader")),
        );

    let valid_rel = Relationship::new(
        "document",
        "doc1",
        "parent",
        Subject::entity_set("folder", "f1", "owner"),
    );
    assert!(valid_rel.validate(&policy).is_ok());

    let invalid_rel = Relationship::new(
        "document",
        "doc1",
        "parent",
        Subject::entity_set("folder", "f1", "reader"),
    );
    let result = invalid_rel.validate(&policy);
    assert!(
        matches!(result, Err(Error::SubjectRestrictionViolation { .. })),
        "Expected SubjectRestrictionViolation, got {:?}",
        result
    );

    let invalid_rel2 = Relationship::with_entity("document", "doc1", "parent", test_did());
    let result2 = invalid_rel2.validate(&policy);
    assert!(
        matches!(result2, Err(Error::SubjectRestrictionViolation { .. })),
        "Expected SubjectRestrictionViolation, got {:?}",
        result2
    );
}

#[test]
fn test_dpi_valid_policy() {
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::computed(
                "reader",
                RelationExpression::union(vec![
                    RelationExpression::this(),
                    RelationExpression::computed_userset("owner"),
                ]),
            ))
            .with_relation(Relation::computed(
                "updater",
                RelationExpression::union(vec![
                    RelationExpression::this(),
                    RelationExpression::computed_userset("owner"),
                ]),
            )),
    );

    assert!(policy.validate_dpi().is_ok());
}

#[test]
fn test_dpi_missing_owner_relation() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(Resource::new("document").with_relation(Relation::direct("reader")));

    let result = policy.validate_dpi();
    assert!(
        matches!(result, Err(Error::DpiMissingOwner { .. })),
        "Expected DpiMissingOwner, got {:?}",
        result
    );
}

#[test]
fn test_dpi_expression_missing_owner() {
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::direct("contributor"))
            .with_relation(Relation::computed(
                "reader",
                RelationExpression::union(vec![
                    RelationExpression::this(),
                    RelationExpression::computed_userset("contributor"),
                ]),
            )),
    );

    let result = policy.validate_dpi();
    assert!(
        matches!(result, Err(Error::DpiExpressionMissingOwner { .. })),
        "Expected DpiExpressionMissingOwner, got {:?}",
        result
    );
}

#[test]
fn test_dpi_disallowed_intersection() {
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::direct("approved"))
            .with_relation(Relation::computed(
                "editor",
                RelationExpression::intersection(vec![
                    RelationExpression::computed_userset("owner"),
                    RelationExpression::computed_userset("approved"),
                ]),
            )),
    );

    let result = policy.validate_dpi();
    assert!(
        matches!(result, Err(Error::DpiDisallowedOperation { .. })),
        "Expected DpiDisallowedOperation, got {:?}",
        result
    );
}

#[test]
fn test_dpi_disallowed_difference() {
    let policy = Policy::new("policy1", "Test").with_resource(
        Resource::new("document")
            .with_relation(Relation::direct("owner"))
            .with_relation(Relation::direct("banned"))
            .with_relation(Relation::computed(
                "viewer",
                RelationExpression::difference(
                    RelationExpression::computed_userset("owner"),
                    RelationExpression::computed_userset("banned"),
                ),
            )),
    );

    let result = policy.validate_dpi();
    assert!(
        matches!(result, Err(Error::DpiDisallowedOperation { .. })),
        "Expected DpiDisallowedOperation, got {:?}",
        result
    );
}

#[test]
fn test_dpi_owner_via_ttu() {
    let policy = Policy::new("policy1", "Test")
        .with_resource(
            Resource::new("file")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("parent"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                        RelationExpression::tuple_to_userset("parent", "owner"),
                    ]),
                )),
        )
        .with_resource(Resource::new("folder").with_relation(Relation::direct("owner")));

    assert!(policy.validate_dpi().is_ok());
}
