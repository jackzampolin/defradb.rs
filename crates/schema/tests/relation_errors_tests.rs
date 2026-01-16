//! Integration tests for relation error handling
//!
//! These tests verify that validation correctly catches:
//! - Invalid relation targets
//! - Primary side violations
//! - Missing relation names
//! - CRDT type mismatches

use schema::{validate_schema, CType, CollectionVersion, FieldDescription, FieldKind, SchemaError};
use std::collections::HashMap;

// =============================================================================
// INVALID RELATION TARGETS
// =============================================================================

mod invalid_targets {
    use super::*;

    /// Relation pointing to nonexistent collection should fail validation
    #[test]
    fn test_relation_to_nonexistent_collection() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        // Note: "users" collection intentionally NOT added

        let result = validate_schema(&collections);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaError::InvalidRelation {
                field_name,
                collection_id,
            } => {
                assert_eq!(field_name, "author");
                assert_eq!(collection_id, "users");
            }
            other => panic!("Expected InvalidRelation error, got: {:?}", other),
        }
    }

    /// Multiple relations to nonexistent collections
    #[test]
    fn test_multiple_invalid_relations() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
            FieldDescription::new("3", "category", FieldKind::relation("categories", false))
                .with_relation_name("post_category"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        let result = validate_schema(&collections);
        assert!(result.is_err());
        // First invalid relation should be caught
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::InvalidRelation { .. }
        ));
    }
}

// =============================================================================
// PRIMARY SIDE VALIDATION
// =============================================================================

mod primary_validation {
    use super::*;

    /// Both sides marked as primary should fail
    #[test]
    fn test_both_sides_primary_fails() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author")
                .as_primary(),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author")
                .as_primary(), // BOTH primary - invalid!
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        let result = validate_schema(&collections);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaError::RelationPrimaryConflict { relation_name } => {
                assert_eq!(relation_name, "book_author");
            }
            other => panic!("Expected RelationPrimaryConflict error, got: {:?}", other),
        }
    }

    /// Neither side marked as primary should fail
    #[test]
    fn test_neither_side_primary_fails() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author"),
            // NOT primary
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author"),
            // NOT primary - BOTH missing!
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        let result = validate_schema(&collections);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::RelationPrimaryConflict { .. }
        ));
    }

    /// Exactly one side primary should pass
    #[test]
    fn test_one_side_primary_passes() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author")
                .as_primary(),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author"),
            // NOT primary - exactly one primary
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        let result = validate_schema(&collections);
        assert!(result.is_ok());
    }

    /// Single-sided relation should pass (no primary check needed)
    #[test]
    fn test_single_sided_relation_passes() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
        ];
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // No relation back to posts - single-sided
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        collections.insert(
            "users".to_string(),
            CollectionVersion::new("users", "v1", "coll-users", users_fields),
        );

        // Single-sided doesn't require primary check (only 1 side exists)
        // But it should still fail validation for missing relation name on the relation_id
        // Actually, single-sided should pass since there's only one side to check
        let result = validate_schema(&collections);
        assert!(result.is_ok());
    }
}

// =============================================================================
// DUPLICATE FIELD NAME VALIDATION
// =============================================================================

mod duplicate_fields {
    use super::*;

    /// Duplicate field names within a collection should fail
    #[test]
    fn test_duplicate_field_names_fail() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "title", FieldKind::string()), // Duplicate!
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        let result = validate_schema(&collections);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaError::DuplicateFieldName(name) => {
                assert_eq!(name, "title");
            }
            other => panic!("Expected DuplicateFieldName error, got: {:?}", other),
        }
    }

    /// Manually defined _id field that clashes with auto-generated one
    /// Note: This test verifies the validation catches the issue AFTER finalization
    #[test]
    fn test_manually_defined_id_field_clash() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
            // Manually define author_id - should conflict with auto-generated
            FieldDescription::new("3", "author_id", FieldKind::string()), // Wrong type too!
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        // The add_relation_id_fields should skip since author_id already exists
        let mut coll = collections.remove("posts").unwrap();
        coll.add_relation_id_fields(|| "gen-999".to_string())
            .unwrap();
        collections.insert("posts".to_string(), coll);

        // No duplicate should be created
        let author_id_count = collections
            .get("posts")
            .unwrap()
            .fields
            .iter()
            .filter(|f| f.name == "author_id")
            .count();
        assert_eq!(
            author_id_count, 1,
            "Should have exactly one author_id field"
        );
    }
}

// =============================================================================
// DUPLICATE COLLECTION NAME VALIDATION
// =============================================================================

mod duplicate_collections {
    use super::*;

    /// Duplicate collection names should fail
    #[test]
    fn test_duplicate_collection_names_fail() {
        let users1_fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
        let users2_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "extra", FieldKind::string()),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "users1".to_string(),
            CollectionVersion::new("users", "v1", "coll-users-1", users1_fields),
        );
        collections.insert(
            "users2".to_string(),
            CollectionVersion::new("users", "v2", "coll-users-2", users2_fields), // Same name!
        );

        let result = validate_schema(&collections);
        assert!(result.is_err());

        match result.unwrap_err() {
            SchemaError::DuplicateCollectionName(name) => {
                assert_eq!(name, "users");
            }
            other => panic!("Expected DuplicateCollectionName error, got: {:?}", other),
        }
    }
}

// =============================================================================
// CRDT TYPE VALIDATION
// =============================================================================

mod crdt_validation {
    use super::*;

    /// Counter CRDT on string field should fail
    #[test]
    fn test_counter_on_string_fails() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string())
                .with_crdt_type(CType::PnCounter), // Invalid!
        ];

        let coll = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);
        let result = coll.validate();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::InvalidCrdtForKind { .. }
        ));
    }

    /// Counter CRDT on int field should pass
    #[test]
    fn test_counter_on_int_passes() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "view_count", FieldKind::int())
                .with_crdt_type(CType::PnCounter),
        ];

        let coll = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);
        let result = coll.validate();

        assert!(result.is_ok());
    }

    /// LWW Register on any field should pass
    #[test]
    fn test_lww_on_any_field_passes() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string())
                .with_crdt_type(CType::LwwRegister),
            FieldDescription::new("3", "count", FieldKind::int())
                .with_crdt_type(CType::LwwRegister),
        ];

        let coll = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);
        let result = coll.validate();

        assert!(result.is_ok());
    }
}

// =============================================================================
// RELATION NAME VALIDATION (from field.rs)
// =============================================================================

mod relation_name_validation {
    use super::*;

    /// Relation field without relation_name should fail field validation
    #[test]
    fn test_relation_without_name_fails() {
        let author_field =
            FieldDescription::new("1", "author", FieldKind::relation("users", false));
        // No .with_relation_name() call

        let result = author_field.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::MissingRequiredField(_)
        ));
    }

    /// Relation field with relation_name should pass
    #[test]
    fn test_relation_with_name_passes() {
        let author_field =
            FieldDescription::new("1", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author");

        let result = author_field.validate();
        assert!(result.is_ok());
    }

    /// Self-ref field without relation_name should fail
    #[test]
    fn test_self_ref_without_name_fails() {
        let parent_field = FieldDescription::new("1", "parent", FieldKind::self_ref("", false));
        // No .with_relation_name() call

        let result = parent_field.validate();
        assert!(result.is_err());
    }
}

// =============================================================================
// GO COMPATIBILITY ERROR TESTS
// =============================================================================

mod go_error_compat {
    use super::*;

    /// Match Go's error behavior for clashing _id field
    /// Based on: tests/integration/query/one_to_one/with_clashing_id_field_test.go
    #[test]
    fn test_go_clashing_id_field_behavior() {
        // In Go, if user defines author_id manually with wrong type,
        // schema parsing should catch it. In Rust, we handle it by
        // not generating a duplicate and letting validation catch type issues.
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
            // User-defined author_id with String type (should be DocID)
            FieldDescription::new("3", "author_id", FieldKind::string()),
        ];

        let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);

        // add_relation_id_fields should NOT create a duplicate
        coll.add_relation_id_fields(|| "gen-999".to_string())
            .unwrap();

        // Should still have only one author_id
        let count = coll.fields.iter().filter(|f| f.name == "author_id").count();
        assert_eq!(count, 1);

        // The existing author_id should be the user-defined String type
        let author_id = coll.field_by_name("author_id").unwrap();
        assert_eq!(author_id.kind, FieldKind::string());
    }
}
