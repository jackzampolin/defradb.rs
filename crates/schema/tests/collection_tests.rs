//! Tests for CollectionVersion and CollectionBuilder.
//!
//! These tests verify:
//! - Collection creation and field accessors
//! - Field lookup by name, ID, and relation
//! - Collection validation
//! - Builder pattern
//! - Serialization roundtrip

use schema::{
    CType, CollectionBuilder, CollectionVersion, FieldDescription, FieldKind, PolicyDescription,
    SchemaError,
};
use std::collections::HashMap;

// ============================================================================
// Helper Functions
// ============================================================================

fn sample_fields() -> Vec<FieldDescription> {
    vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "name", FieldKind::string()),
        FieldDescription::new("3", "age", FieldKind::int()),
    ]
}

// ============================================================================
// Basic Collection Tests
// ============================================================================

#[test]
fn test_new_collection() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
    assert_eq!(coll.name, "users");
    assert_eq!(coll.version_id, "v1");
    assert!(coll.is_active);
    assert_eq!(coll.fields.len(), 3);
}

#[test]
fn test_field_by_name() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
    let field = coll.field_by_name("name").unwrap();
    assert_eq!(field.id, "2");
}

#[test]
fn test_field_by_id() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
    let field = coll.field_by_id("3").unwrap();
    assert_eq!(field.name, "age");
}

// ============================================================================
// Field Relation Lookup Tests
// ============================================================================

#[test]
fn test_field_by_relation_name() {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "author", FieldKind::relation("users", false))
            .with_relation_name("post_author"),
    ];
    let coll = CollectionVersion::new("posts", "v1", "coll-1", fields);

    let field = coll.field_by_relation_name("post_author").unwrap();
    assert_eq!(field.name, "author");
    assert!(coll.field_by_relation_name("nonexistent").is_none());
}

#[test]
fn test_field_by_relation() {
    // Create posts collection with author field
    let posts_fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "author", FieldKind::relation("users", false))
            .with_relation_name("author_posts"),
    ];
    let posts = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);

    // Create users collection with posts field
    let users_fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
            .with_relation_name("author_posts"),
    ];
    let users = CollectionVersion::new("users", "v1", "coll-users", users_fields);

    // From posts collection, find the field in users that is part of "author_posts"
    // but not the "author" field from "posts"
    let field = users
        .field_by_relation("author_posts", "posts", "author")
        .unwrap();
    assert_eq!(field.name, "posts");

    // From users collection, find the field in posts that is part of "author_posts"
    // but not the "posts" field from "users"
    let field = posts
        .field_by_relation("author_posts", "users", "posts")
        .unwrap();
    assert_eq!(field.name, "author");

    // Nonexistent relation should return None
    assert!(posts
        .field_by_relation("nonexistent", "users", "posts")
        .is_none());
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_validate_duplicate_names_fails() {
    let fields = vec![
        FieldDescription::new("1", "name", FieldKind::string()),
        FieldDescription::new("2", "name", FieldKind::string()),
    ];
    let coll = CollectionVersion::new("users", "v1", "coll-1", fields);

    let result = coll.validate();
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SchemaError::DuplicateFieldName(_)
    ));
}

#[test]
fn test_validate_invalid_crdt_fails() {
    let fields =
        vec![FieldDescription::new("1", "title", FieldKind::string())
            .with_crdt_type(CType::PnCounter)];
    let coll = CollectionVersion::new("posts", "v1", "coll-1", fields);

    let result = coll.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_valid_collection() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
    assert!(coll.validate().is_ok());
}

// ============================================================================
// Policy Validation Tests
// ============================================================================

#[test]
fn test_validate_collection_with_valid_policy() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields())
        .with_policy(PolicyDescription::new("policy-123", "users"));
    assert!(coll.validate().is_ok());
}

#[test]
fn test_validate_collection_with_invalid_policy_path_separator() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields())
        .with_policy(PolicyDescription::new("policy/traversal", "users"));
    let result = coll.validate();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SchemaError::InvalidPolicy(_)));
}

#[test]
fn test_validate_collection_with_invalid_policy_dotdot() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields())
        .with_policy(PolicyDescription::new("policy..secret", "users"));
    let result = coll.validate();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidPolicy(_)));
    assert!(err.to_string().contains("'..'"));
}

#[test]
fn test_validate_collection_with_invalid_policy_null_byte() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields())
        .with_policy(PolicyDescription::new("policy\0123", "users"));
    let result = coll.validate();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidPolicy(_)));
    assert!(err.to_string().contains("null bytes"));
}

#[test]
fn test_validate_collection_with_empty_policy_id() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields())
        .with_policy(PolicyDescription::new("", "users"));
    let result = coll.validate();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), SchemaError::InvalidPolicy(_)));
}

#[test]
fn test_validate_collection_with_whitespace_policy_id() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields())
        .with_policy(PolicyDescription::new("   ", "users"));
    let result = coll.validate();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, SchemaError::InvalidPolicy(_)));
    assert!(err.to_string().contains("whitespace-only"));
}

// ============================================================================
// Builder Tests
// ============================================================================

#[test]
fn test_builder() {
    let coll = CollectionBuilder::new("users", "coll-1")
        .scalar("1", "_docID", FieldKind::doc_id())
        .scalar("2", "name", FieldKind::string())
        .field(
            FieldDescription::new("3", "score", FieldKind::int()).with_crdt_type(CType::PnCounter),
        )
        .build();

    assert_eq!(coll.name, "users");
    assert_eq!(coll.fields.len(), 3);
    assert!(coll.version_id.starts_with('v'));
}

// ============================================================================
// Relation Validation Tests
// ============================================================================

#[test]
fn test_relation_validation() {
    let author_field = FieldDescription::new("1", "author", FieldKind::relation("users", false))
        .with_relation_name("post_author");

    let posts = CollectionVersion::new("posts", "v1", "coll-posts", vec![author_field]);

    let mut collections = HashMap::new();
    collections.insert(
        "users".to_string(),
        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![FieldDescription::new("1", "name", FieldKind::string())],
        ),
    );

    assert!(posts.validate_with_collections(&collections).is_ok());
}

#[test]
fn test_relation_to_unknown_collection_fails() {
    let author_field = FieldDescription::new("1", "author", FieldKind::relation("unknown", false))
        .with_relation_name("post_author");

    let posts = CollectionVersion::new("posts", "v1", "coll-posts", vec![author_field]);

    let collections = HashMap::new();
    let result = posts.validate_with_collections(&collections);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SchemaError::InvalidRelation { .. }
    ));
}

// ============================================================================
// Serialization Tests
// ============================================================================

#[test]
fn test_serialization_roundtrip() {
    let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
    let json = serde_json::to_string(&coll).unwrap();
    let parsed: CollectionVersion = serde_json::from_str(&json).unwrap();
    assert_eq!(coll, parsed);
}
