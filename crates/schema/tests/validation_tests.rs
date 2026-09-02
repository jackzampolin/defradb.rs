//! Tests for schema validation.
//!
//! These tests verify:
//! - Empty and single collection schemas
//! - Duplicate collection name detection
//! - Relation primary side validation

use schema::{
    validate_schema, CollectionVersion, FieldDescription, FieldKind, PolicyDescription,
    QuerySource, SchemaError,
};
use std::collections::HashMap;

// ============================================================================
// Helper Functions
// ============================================================================

fn user_collection() -> CollectionVersion {
    CollectionVersion::new(
        "users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

fn post_collection_with_author(is_primary: bool) -> CollectionVersion {
    let mut author_field =
        FieldDescription::new("1", "author", FieldKind::relation("users", false))
            .with_relation_name("user_posts");

    if is_primary {
        author_field = author_field.as_primary();
    }

    CollectionVersion::new(
        "posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            author_field,
        ],
    )
}

fn user_collection_with_posts(is_primary: bool) -> CollectionVersion {
    let mut posts_field = FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
        .with_relation_name("user_posts");

    if is_primary {
        posts_field = posts_field.as_primary();
    }

    CollectionVersion::new(
        "users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            posts_field,
        ],
    )
}

// ============================================================================
// Basic Schema Validation Tests
// ============================================================================

#[test]
fn test_validate_empty_schema() {
    let collections = HashMap::new();
    assert!(validate_schema(&collections).is_ok());
}

#[test]
fn test_validate_single_collection() {
    let mut collections = HashMap::new();
    collections.insert("users".to_string(), user_collection());
    assert!(validate_schema(&collections).is_ok());
}

#[test]
fn test_duplicate_collection_names_fails() {
    let mut collections = HashMap::new();
    collections.insert("users".to_string(), user_collection());
    let mut dup = user_collection();
    dup.collection_id = "coll-users-2".into();
    // name stays "users" - should fail
    collections.insert("users-2".to_string(), dup);

    let result = validate_schema(&collections);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SchemaError::DuplicateCollectionName(_)
    ));
}

#[test]
fn test_unique_collection_names_ok() {
    let mut collections = HashMap::new();
    collections.insert("users".to_string(), user_collection());
    let mut other = user_collection();
    other.name = "admins".into();
    other.collection_id = "coll-admins".into();
    collections.insert("admins".to_string(), other);

    assert!(validate_schema(&collections).is_ok());
}

#[test]
fn test_multiple_active_versions_of_collection_fail() {
    let mut old_version = user_collection();
    old_version.is_active = false;

    let mut active_version = old_version.clone();
    active_version.name = "people".into();
    active_version.version_id = "v2".into();
    active_version.is_active = true;

    let old_state = vec![old_version.clone(), active_version.clone()];
    old_version.is_active = true;

    let error = schema::definition_validation::validate_collection_changes(
        &old_state,
        &[old_version, active_version],
    )
    .unwrap_err();
    assert!(error.contains("multiple versions of same collection cannot be active"));
}

// ============================================================================
// Relation Primary Validation Tests
// ============================================================================

#[test]
fn test_relation_one_primary_valid() {
    let mut collections = HashMap::new();
    collections.insert("users".to_string(), user_collection_with_posts(false));
    collections.insert("posts".to_string(), post_collection_with_author(true));

    assert!(validate_schema(&collections).is_ok());
}

#[test]
fn test_relation_both_primary_invalid() {
    let mut collections = HashMap::new();
    collections.insert("users".to_string(), user_collection_with_posts(true));
    collections.insert("posts".to_string(), post_collection_with_author(true));

    let result = validate_schema(&collections);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SchemaError::RelationPrimaryConflict { .. }
    ));
}

#[test]
fn test_relation_neither_primary_invalid() {
    let mut collections = HashMap::new();
    collections.insert("users".to_string(), user_collection_with_posts(false));
    collections.insert("posts".to_string(), post_collection_with_author(false));

    let result = validate_schema(&collections);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SchemaError::RelationPrimaryConflict { .. }
    ));
}

#[test]
fn test_single_sided_relation_no_primary_check() {
    let mut collections = HashMap::new();
    collections.insert("posts".to_string(), post_collection_with_author(false));

    let result = validate_schema(&collections);
    // Should fail because relation points to nonexistent "users" collection
    assert!(result.is_err());
}

#[test]
fn new_relation_can_target_an_existing_collection() {
    let author = FieldDescription::new("3", "author", FieldKind::named("users", false))
        .with_relation_name("user_posts")
        .as_primary();
    let posts = CollectionVersion::new("posts", "v-posts", "coll-posts", vec![author]);

    schema::definition_validation::validate_new_collections_with_existing(
        &[posts],
        &[user_collection()],
    )
    .unwrap();
}

#[test]
fn new_relation_must_target_a_known_collection() {
    let author = FieldDescription::new("3", "author", FieldKind::named("missing", true))
        .with_relation_name("user_posts")
        .as_primary();
    let posts = CollectionVersion::new("posts", "v-posts", "coll-posts", vec![author]);

    let error =
        schema::definition_validation::validate_new_collections_with_existing(&[posts], &[])
            .unwrap_err();

    assert!(error.contains("no type found for given name. Field: author, Kind: [missing]"));
}

#[test]
fn secondary_relation_requires_a_primary_counterpart() {
    let author = FieldDescription::new("3", "author", FieldKind::named("users", false))
        .with_relation_name("user_posts");
    let posts = CollectionVersion::new("posts", "v-posts", "coll-posts", vec![author]);

    let error = schema::definition_validation::validate_new_collections_with_existing(
        &[posts],
        &[user_collection()],
    )
    .unwrap_err();

    assert!(error.contains("relation missing field. Object: users, RelationName: user_posts"));
}

#[test]
fn paired_relation_cannot_have_two_primary_fields() {
    let posts_field = FieldDescription::new("3", "posts", FieldKind::named("posts", true))
        .with_relation_name("user_posts")
        .as_primary();
    let users = CollectionVersion::new("users", "v-users", "coll-users", vec![posts_field]);
    let author = FieldDescription::new("3", "author", FieldKind::named("users", false))
        .with_relation_name("user_posts")
        .as_primary();
    let posts = CollectionVersion::new("posts", "v-posts", "coll-posts", vec![author]);

    let error =
        schema::definition_validation::validate_new_collections_with_existing(&[users, posts], &[])
            .unwrap_err();

    assert!(error.contains("relation can only have a single field set as primary"));
}

#[test]
fn self_relation_must_use_self_kind() {
    let manager = FieldDescription::new("3", "manager", FieldKind::named("users", false))
        .with_relation_name("user_manager")
        .as_primary();
    let users = CollectionVersion::new("users", "v-users", "coll-users", vec![manager]);

    let error =
        schema::definition_validation::validate_new_collections_with_existing(&[users], &[])
            .unwrap_err();

    assert!(
        error.contains("must specify 'Self' kind for self referencing relations. Field: manager")
    );
}

#[test]
fn materialized_collection_can_have_policy() {
    let collection = user_collection().with_policy(PolicyDescription::new("policy", "users"));

    schema::definition_validation::validate_collection_changes(
        std::slice::from_ref(&collection),
        std::slice::from_ref(&collection),
    )
    .unwrap();
}

#[test]
fn materialized_view_cannot_have_policy() {
    let mut collection = user_collection().with_policy(PolicyDescription::new("policy", "users"));
    collection.query = Some(QuerySource::new(serde_json::json!({})));

    let error = schema::definition_validation::validate_collection_changes(
        std::slice::from_ref(&collection),
        std::slice::from_ref(&collection),
    )
    .unwrap_err();

    assert!(error.contains("materialized views do not support ACP"));
}
