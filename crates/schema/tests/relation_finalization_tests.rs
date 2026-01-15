//! Integration tests for relation field finalization
//!
//! These tests verify the complete finalization flow for various relationship patterns:
//! - One-to-one relations
//! - One-to-many relations
//! - Self-referential relations
//! - Multiple relations between same collections
//! - Circular relations

use schema::{CType, CollectionVersion, FieldDescription, FieldKind};
use std::collections::HashMap;

/// Helper to create a field ID generator
fn field_id_generator() -> impl FnMut() -> String {
    let mut counter = 1000;
    move || {
        counter += 1;
        format!("gen-{}", counter)
    }
}

// =============================================================================
// ONE-TO-ONE RELATION TESTS
// =============================================================================

mod one_to_one {
    use super::*;

    /// Standard one-to-one: Book has one Author, Author has one published Book
    /// Primary side is explicitly marked with @primary
    #[test]
    fn test_explicit_primary_preserved() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author")
                .as_primary(),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author"),
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

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let books = collections.get("books").unwrap();
        let authors = collections.get("authors").unwrap();

        // Primary side assertions
        let author_field = books.field_by_name("author").unwrap();
        assert!(author_field.is_primary, "author should be primary");

        let author_id = books.field_by_name("author_id").unwrap();
        assert!(author_id.is_primary, "author_id should also be primary");
        assert_eq!(author_id.relation_name, Some("book_author".to_string()));
        assert_eq!(author_id.crdt_type, CType::LwwRegister);

        // Secondary side assertions
        let published_field = authors.field_by_name("published").unwrap();
        assert!(
            !published_field.is_primary,
            "published should NOT be primary"
        );

        let published_id = authors.field_by_name("published_id").unwrap();
        assert!(
            !published_id.is_primary,
            "published_id should NOT be primary"
        );
        assert_eq!(published_id.relation_name, Some("book_author".to_string()));
    }

    /// One-to-one with auto-generated relation name (no explicit @relation)
    #[test]
    fn test_both_sides_get_id_fields() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("authors_books")
                .as_primary(),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "book", FieldKind::relation("books", false))
                .with_relation_name("authors_books"),
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

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        // Both sides should have _id fields in one-to-one
        assert!(collections
            .get("books")
            .unwrap()
            .field_by_name("author_id")
            .is_some());
        assert!(collections
            .get("authors")
            .unwrap()
            .field_by_name("book_id")
            .is_some());
    }

    /// One-to-one where one side is missing (one-sided relation)
    #[test]
    fn test_one_sided_one_to_one() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        // Note: authors collection NOT added

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let books = collections.get("books").unwrap();

        // Should still get _id field even without other side
        assert!(books.field_by_name("author_id").is_some());
    }
}

// =============================================================================
// ONE-TO-MANY RELATION TESTS
// =============================================================================

mod one_to_many {
    use super::*;

    /// Standard one-to-many: Author has many Posts, Post has one Author
    #[test]
    fn test_primary_on_non_array_side() {
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Array side - should NOT be primary
            FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                .with_relation_name("author_posts"),
        ];
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            // Non-array side - should auto-become primary
            FieldDescription::new("3", "author", FieldKind::relation("authors", false))
                .with_relation_name("author_posts"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let authors = collections.get("authors").unwrap();
        let posts = collections.get("posts").unwrap();

        // Array side should NOT have _id field or be primary
        assert!(
            authors.field_by_name("posts_id").is_none(),
            "Array side should not have _id"
        );
        assert!(!authors.field_by_name("posts").unwrap().is_primary);

        // Non-array side should have _id field and auto-set to primary
        assert!(
            posts.field_by_name("author_id").is_some(),
            "Non-array side should have _id"
        );
        assert!(posts.field_by_name("author").unwrap().is_primary);
    }

    /// One-to-many with explicit primary (should be respected)
    #[test]
    fn test_explicit_primary_on_many_side() {
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            // Explicitly marked as primary (unusual but allowed)
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("author_posts")
                .as_primary(),
        ];
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("author_posts"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        // Explicit primary should be preserved even on array side
        assert!(
            collections
                .get("authors")
                .unwrap()
                .field_by_name("posts")
                .unwrap()
                .is_primary
        );
    }

    /// One-to-many one-sided (only "many" side defined)
    #[test]
    fn test_one_sided_many_to_one() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("author_posts"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        // Note: authors collection NOT added

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let posts = collections.get("posts").unwrap();

        // Non-array side should still get _id field
        assert!(posts.field_by_name("author_id").is_some());
    }
}

// =============================================================================
// SELF-REFERENTIAL RELATION TESTS
// =============================================================================

mod self_referential {
    use super::*;

    /// Tree structure: Node has parent and children
    #[test]
    fn test_parent_child_tree() {
        let node_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Parent (non-array) - should get _id field
            FieldDescription::new("3", "parent", FieldKind::self_ref("", false))
                .with_relation_name("node_tree"),
            // Children (array) - should NOT get _id field
            FieldDescription::new("4", "children", FieldKind::self_ref("", true))
                .with_relation_name("node_tree"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "nodes".to_string(),
            CollectionVersion::new("nodes", "v1", "coll-nodes", node_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let nodes = collections.get("nodes").unwrap();

        // Parent should get _id field
        let parent_id = nodes.field_by_name("parent_id");
        assert!(parent_id.is_some(), "parent should get _id field");
        assert_eq!(
            parent_id.unwrap().relation_name,
            Some("node_tree".to_string())
        );

        // Children should NOT get _id field
        assert!(
            nodes.field_by_name("children_id").is_none(),
            "children should not get _id field"
        );
    }

    /// Linked list: Node has next pointer
    #[test]
    fn test_linked_list() {
        let node_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "value", FieldKind::int()),
            FieldDescription::new("3", "next", FieldKind::self_ref("", false))
                .with_relation_name("node_list")
                .as_primary(),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "nodes".to_string(),
            CollectionVersion::new("nodes", "v1", "coll-nodes", node_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let nodes = collections.get("nodes").unwrap();

        // next should get _id field
        assert!(nodes.field_by_name("next_id").is_some());
        assert!(nodes.field_by_name("next").unwrap().is_primary);
    }

    /// Bidirectional self-reference: Employee has manager and reports
    #[test]
    fn test_manager_reports() {
        let employee_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "manager", FieldKind::self_ref("", false))
                .with_relation_name("employee_hierarchy"),
            FieldDescription::new("4", "reports", FieldKind::self_ref("", true))
                .with_relation_name("employee_hierarchy"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "employees".to_string(),
            CollectionVersion::new("employees", "v1", "coll-employees", employee_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let employees = collections.get("employees").unwrap();

        // manager (non-array) gets _id
        assert!(employees.field_by_name("manager_id").is_some());
        // reports (array) does NOT get _id
        assert!(employees.field_by_name("reports_id").is_none());
    }
}

// =============================================================================
// MULTIPLE RELATIONS TESTS
// =============================================================================

mod multiple_relations {
    use super::*;

    /// Multiple relations from same collection to same target
    #[test]
    fn test_author_and_reviewer() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author")
                .as_primary(),
            FieldDescription::new("4", "reviewer", FieldKind::relation("users", false))
                .with_relation_name("post_reviewer")
                .as_primary(),
        ];
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
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

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let posts = collections.get("posts").unwrap();

        // Both should get separate _id fields
        let author_id = posts.field_by_name("author_id").unwrap();
        let reviewer_id = posts.field_by_name("reviewer_id").unwrap();

        assert_eq!(author_id.relation_name, Some("post_author".to_string()));
        assert_eq!(reviewer_id.relation_name, Some("post_reviewer".to_string()));

        // Each should be primary independently
        assert!(author_id.is_primary);
        assert!(reviewer_id.is_primary);
    }

    /// Multiple relations between different collection pairs
    #[test]
    fn test_multiple_relation_pairs() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author")
                .as_primary(),
            FieldDescription::new("3", "category", FieldKind::relation("categories", false))
                .with_relation_name("post_category")
                .as_primary(),
        ];
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("post_author"),
        ];
        let categories_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("post_category"),
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
        collections.insert(
            "categories".to_string(),
            CollectionVersion::new("categories", "v1", "coll-categories", categories_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let posts = collections.get("posts").unwrap();

        // Both relations should get _id fields
        assert!(posts.field_by_name("author_id").is_some());
        assert!(posts.field_by_name("category_id").is_some());

        // Array sides should NOT get _id fields
        assert!(collections
            .get("users")
            .unwrap()
            .field_by_name("posts_id")
            .is_none());
        assert!(collections
            .get("categories")
            .unwrap()
            .field_by_name("posts_id")
            .is_none());
    }
}

// =============================================================================
// CIRCULAR RELATION TESTS
// =============================================================================

mod circular {
    use super::*;

    /// Two-way circular: A -> B -> A (simplified for Phase 1)
    #[test]
    fn test_simple_circular() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author")
                .as_primary(),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "favorite_book", FieldKind::relation("books", false))
                .with_relation_name("author_favorite")
                .as_primary(),
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

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        // Both relations are independent and should both work
        assert!(collections
            .get("books")
            .unwrap()
            .field_by_name("author_id")
            .is_some());
        assert!(collections
            .get("authors")
            .unwrap()
            .field_by_name("favorite_book_id")
            .is_some());
    }
}

// =============================================================================
// FIELD ORDERING TESTS
// =============================================================================

mod field_ordering {
    use super::*;

    /// Verify _id fields are inserted immediately after their relation fields
    #[test]
    fn test_id_field_position_preserved() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author")
                .as_primary(),
            FieldDescription::new("4", "content", FieldKind::string()),
            FieldDescription::new("5", "category", FieldKind::relation("categories", false))
                .with_relation_name("post_category")
                .as_primary(),
            FieldDescription::new("6", "tags", FieldKind::string_array()),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let posts = collections.get("posts").unwrap();
        let field_names: Vec<&str> = posts.fields.iter().map(|f| f.name.as_str()).collect();

        // Find positions
        let author_pos = field_names.iter().position(|&n| n == "author").unwrap();
        let author_id_pos = field_names.iter().position(|&n| n == "author_id").unwrap();
        let content_pos = field_names.iter().position(|&n| n == "content").unwrap();
        let category_pos = field_names.iter().position(|&n| n == "category").unwrap();
        let category_id_pos = field_names
            .iter()
            .position(|&n| n == "category_id")
            .unwrap();
        let tags_pos = field_names.iter().position(|&n| n == "tags").unwrap();

        // Verify ordering
        assert_eq!(
            author_id_pos,
            author_pos + 1,
            "author_id should follow author"
        );
        assert!(
            content_pos > author_id_pos,
            "content should come after author_id"
        );
        assert_eq!(
            category_id_pos,
            category_pos + 1,
            "category_id should follow category"
        );
        assert!(
            tags_pos > category_id_pos,
            "tags should come after category_id"
        );
    }
}

// =============================================================================
// GO COMPATIBILITY TESTS
// =============================================================================

mod go_compatibility {
    use super::*;

    /// Match Go's finalizeRelations behavior for standard one-to-many
    #[test]
    fn test_matches_go_one_to_many() {
        // This replicates the exact Go test case from
        // tests/integration/query/one_to_many/simple_test.go
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "verified", FieldKind::bool()),
            FieldDescription::new("5", "published", FieldKind::relation("books", true))
                .with_relation_name("author_book"),
        ];
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "rating", FieldKind::float64()),
            FieldDescription::new("4", "author", FieldKind::relation("authors", false))
                .with_relation_name("author_book"),
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("Author", "v1", "coll-authors", authors_fields),
        );
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("Book", "v1", "coll-books", books_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let authors = collections.get("authors").unwrap();
        let books = collections.get("books").unwrap();

        // Go behavior: non-array side (Book.author) is auto-set to primary
        assert!(books.field_by_name("author").unwrap().is_primary);

        // Go behavior: non-array side gets _id field
        let author_id = books.field_by_name("author_id").unwrap();
        assert_eq!(author_id.kind, FieldKind::doc_id());
        assert_eq!(author_id.crdt_type, CType::LwwRegister);

        // Go behavior: array side does NOT get _id field
        assert!(authors.field_by_name("published_id").is_none());
    }

    /// Match Go's finalizeRelations behavior for one-to-one with explicit @primary
    #[test]
    fn test_matches_go_one_to_one_primary() {
        // This replicates Go test from tests/integration/query/one_to_one/simple_test.go
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "rating", FieldKind::float64()),
            FieldDescription::new("4", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author"),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "verified", FieldKind::bool()),
            FieldDescription::new("5", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author")
                .as_primary(), // @primary in Go SDL
        ];

        let mut collections = HashMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("Book", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("Author", "v1", "coll-authors", authors_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator());

        let books = collections.get("books").unwrap();
        let authors = collections.get("authors").unwrap();

        // Go behavior: explicit @primary is preserved
        assert!(authors.field_by_name("published").unwrap().is_primary);
        assert!(!books.field_by_name("author").unwrap().is_primary);

        // Go behavior: both sides get _id fields in one-to-one
        assert!(authors.field_by_name("published_id").is_some());
        assert!(books.field_by_name("author_id").is_some());
    }
}
