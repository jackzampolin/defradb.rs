//! Tests for relation _id field generation.
//!
//! These tests verify that `add_relation_id_fields` correctly:
//! - Generates `_id` fields for non-array relations
//! - Skips `_id` fields for array relations
//! - Preserves existing `_id` fields
//! - Inherits relation_name and is_primary from relation fields

use schema::{CType, CollectionVersion, FieldDescription, FieldKind};
use std::collections::BTreeMap;

// ============================================================================
// Basic _id Field Generation Tests
// ============================================================================

#[test]
fn test_relation_id_field_name() {
    assert_eq!(
        CollectionVersion::relation_id_field_name("author"),
        "_authorID"
    );
    assert_eq!(
        CollectionVersion::relation_id_field_name("posts"),
        "_postsID"
    );
}

#[test]
fn test_add_relation_id_fields() {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "author", FieldKind::relation("users", false))
            .with_relation_name("user_posts")
            .as_primary(),
    ];
    let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

    let mut counter = 100;
    coll.add_relation_id_fields(|| {
        counter += 1;
        counter.to_string()
    })
    .unwrap();

    assert_eq!(coll.fields.len(), 3);

    // Verify _id field was added
    let id_field = coll.field_by_name("_authorID").unwrap();
    assert_eq!(id_field.id, "101");
    assert_eq!(id_field.kind, FieldKind::doc_id());
    assert_eq!(id_field.relation_name, Some("user_posts".to_string()));
    assert!(id_field.is_primary);
    assert_eq!(id_field.crdt_type, CType::LwwRegister);

    // Verify _id field is after relation field
    let author_idx = coll.fields.iter().position(|f| f.name == "author").unwrap();
    let author_id_idx = coll
        .fields
        .iter()
        .position(|f| f.name == "_authorID")
        .unwrap();
    assert_eq!(author_id_idx, author_idx + 1);
}

#[test]
fn test_add_relation_id_fields_skips_arrays() {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        // Array relation (one-to-many from the "many" side) - no _id field needed
        FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
            .with_relation_name("user_posts"),
    ];
    let mut coll = CollectionVersion::new("users", "v1", "coll-users", fields);

    coll.add_relation_id_fields(|| "999".to_string()).unwrap();

    // No _id field should be added for array relations
    assert_eq!(coll.fields.len(), 2);
    assert!(coll.field_by_name("_postsID").is_none());
}

#[test]
fn test_add_relation_id_fields_skips_existing() {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "author", FieldKind::relation("users", false))
            .with_relation_name("user_posts"),
        // _id field already exists
        FieldDescription::new("3", "_authorID", FieldKind::doc_id())
            .with_relation_name("user_posts"),
    ];
    let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

    coll.add_relation_id_fields(|| "999".to_string()).unwrap();

    // No new _id field should be added
    assert_eq!(coll.fields.len(), 3);
    // Original _id field should remain
    assert_eq!(coll.field_by_name("_authorID").unwrap().id, "3");
}

#[test]
fn test_has_relation_id_field() {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "author", FieldKind::relation("users", false)),
        FieldDescription::new("3", "_authorID", FieldKind::doc_id()),
    ];
    let coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

    assert!(coll.has_relation_id_field("author"));
    assert!(!coll.has_relation_id_field("publisher"));
}

// ============================================================================
// Finalize Relations Tests
// ============================================================================

#[test]
fn test_finalize_relations_adds_id_fields() {
    let users_fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "name", FieldKind::string()),
        // One-to-many (array) - no _id field needed
        FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
            .with_relation_name("user_posts"),
    ];
    let posts_fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "title", FieldKind::string()),
        // Many-to-one (non-array) - _id field needed
        FieldDescription::new("3", "author", FieldKind::relation("users", false))
            .with_relation_name("user_posts"),
    ];

    let mut collections = BTreeMap::new();
    collections.insert(
        "users".to_string(),
        CollectionVersion::new("users", "v1", "coll-users", users_fields),
    );
    collections.insert(
        "posts".to_string(),
        CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
    );

    let mut counter = 100;
    let mut index_counter = 10000u32;
    CollectionVersion::finalize_relations(
        &mut collections,
        || {
            counter += 1;
            counter.to_string()
        },
        || {
            index_counter += 1;
            index_counter
        },
    )
    .unwrap();

    let users = collections.get("users").unwrap();
    let posts = collections.get("posts").unwrap();

    // Users shouldn't have _id field (array relation)
    assert!(users.field_by_name("_postsID").is_none());

    // Posts should have _id field (non-array relation)
    assert!(posts.field_by_name("_authorID").is_some());

    // Posts.author should be marked as primary (other side is array)
    assert!(posts.field_by_name("author").unwrap().is_primary);

    assert!(
        posts.indexes.iter().all(|idx| {
            idx.fields
                .first()
                .is_none_or(|field| field.name != "_authorID")
        }),
        "one-to-many relation FK indexes require an explicit @index"
    );
}

#[test]
fn test_add_relation_id_fields_rejects_duplicate_id() {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "author", FieldKind::relation("users", false))
            .with_relation_name("user_posts"),
    ];
    let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

    // Generator that returns an existing field ID ("1" already exists)
    let result = coll.add_relation_id_fields(|| "1".to_string());

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        schema::SchemaError::DuplicateFieldId(id) if id == "1"
    ));
}

#[test]
fn test_finalize_relations_hashmap() {
    use std::collections::HashMap;

    let mut collections = HashMap::new();
    collections.insert(
        "users".to_string(),
        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                    .with_relation_name("user_posts"),
            ],
        ),
    );
    collections.insert(
        "posts".to_string(),
        CollectionVersion::new(
            "posts",
            "v1",
            "coll-posts",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "author", FieldKind::relation("users", false))
                    .with_relation_name("user_posts"),
            ],
        ),
    );

    let mut counter = 100;
    let mut index_counter = 10000u32;
    CollectionVersion::finalize_relations_hashmap(
        &mut collections,
        || {
            counter += 1;
            counter.to_string()
        },
        || {
            index_counter += 1;
            index_counter
        },
    )
    .unwrap();

    // Verify HashMap was updated in place
    let posts = collections.get("posts").unwrap();
    assert!(
        posts.field_by_name("_authorID").is_some(),
        "_authorID field should be added"
    );

    // Verify auto-primary was applied (author side is primary since users.posts is array)
    assert!(
        posts.field_by_name("author").unwrap().is_primary,
        "author should be marked as primary"
    );

    assert!(
        posts.indexes.iter().all(|idx| {
            idx.fields
                .first()
                .is_none_or(|field| field.name != "_authorID")
        }),
        "one-to-many relation FK indexes require an explicit @index"
    );

    // Verify users collection is also in the HashMap
    let users = collections.get("users").unwrap();
    assert!(users.field_by_name("posts").is_some());
}
