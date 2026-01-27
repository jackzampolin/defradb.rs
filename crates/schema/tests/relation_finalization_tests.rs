//! Integration tests for relation field finalization
//!
//! These tests verify the complete finalization flow for various relationship patterns:
//! - One-to-one relations
//! - One-to-many relations
//! - Self-referential relations
//! - Multiple relations between same collections
//! - Circular relations

use schema::{CType, CollectionVersion, FieldDescription, FieldKind};
use std::collections::BTreeMap;

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

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let books = collections.get("books").unwrap();
        let authors = collections.get("authors").unwrap();

        // Primary side assertions
        let author_field = books.field_by_name("author").unwrap();
        assert!(author_field.is_primary, "author should be primary");

        let author_id_field = books.field_by_name("_authorID").unwrap();
        assert!(
            author_id_field.is_primary,
            "_authorID should also be primary"
        );
        assert_eq!(
            author_id_field.relation_name,
            Some("book_author".to_string())
        );
        assert_eq!(author_id_field.crdt_type, CType::LwwRegister);

        // Secondary side assertions
        let published_field = authors.field_by_name("published").unwrap();
        assert!(
            !published_field.is_primary,
            "published should NOT be primary"
        );

        let published_id_field = authors.field_by_name("_publishedID").unwrap();
        assert!(
            !published_id_field.is_primary,
            "_publishedID should NOT be primary"
        );
        assert_eq!(
            published_id_field.relation_name,
            Some("book_author".to_string())
        );
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        // Both sides should have _id fields in one-to-one
        assert!(collections
            .get("books")
            .unwrap()
            .field_by_name("_authorID")
            .is_some());
        assert!(collections
            .get("authors")
            .unwrap()
            .field_by_name("_bookID")
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        // Note: authors collection NOT added

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let books = collections.get("books").unwrap();

        // Should still get _id field even without other side
        assert!(books.field_by_name("_authorID").is_some());
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let authors = collections.get("authors").unwrap();
        let posts = collections.get("posts").unwrap();

        // Array side should NOT have _id field or be primary
        assert!(
            authors.field_by_name("_postsID").is_none(),
            "Array side should not have _id"
        );
        assert!(!authors.field_by_name("posts").unwrap().is_primary);

        // Non-array side should have _id field and auto-set to primary
        assert!(
            posts.field_by_name("_authorID").is_some(),
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

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

        let mut collections = BTreeMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        // Note: authors collection NOT added

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let posts = collections.get("posts").unwrap();

        // Non-array side should still get _id field
        assert!(posts.field_by_name("_authorID").is_some());
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "nodes".to_string(),
            CollectionVersion::new("nodes", "v1", "coll-nodes", node_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let nodes = collections.get("nodes").unwrap();

        // Parent should get _id field
        let parent_id_field = nodes.field_by_name("_parentID");
        assert!(parent_id_field.is_some(), "parent should get _id field");
        assert_eq!(
            parent_id_field.unwrap().relation_name,
            Some("node_tree".to_string())
        );

        // Children should NOT get _id field
        assert!(
            nodes.field_by_name("_childrenID").is_none(),
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "nodes".to_string(),
            CollectionVersion::new("nodes", "v1", "coll-nodes", node_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let nodes = collections.get("nodes").unwrap();

        // next should get _id field
        assert!(nodes.field_by_name("_nextID").is_some());
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "employees".to_string(),
            CollectionVersion::new("employees", "v1", "coll-employees", employee_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let employees = collections.get("employees").unwrap();

        // manager (non-array) gets _id
        assert!(employees.field_by_name("_managerID").is_some());
        // reports (array) does NOT get _id
        assert!(employees.field_by_name("_reportsID").is_none());
    }

    /// Self-referential one-to-one: Person has spouse (both sides non-array)
    /// This tests the fix for self-referential lookup where the collection
    /// was removed from the map during processing
    #[test]
    fn test_self_ref_one_to_one_spouse() {
        let person_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // spouse: Person @relation(name: "marriage") @primary
            FieldDescription::new("3", "spouse", FieldKind::relation("persons", false))
                .with_relation_name("marriage")
                .as_primary(),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "persons".to_string(),
            CollectionVersion::new("persons", "v1", "coll-persons", person_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let persons = collections.get("persons").unwrap();

        // spouse should get _id field
        assert!(
            persons.field_by_name("_spouseID").is_some(),
            "spouse should get _id field"
        );

        // spouse should remain primary (explicit marking)
        assert!(
            persons.field_by_name("spouse").unwrap().is_primary,
            "spouse should be primary"
        );
    }

    /// Self-referential one-to-one with auto-primary determination
    /// When no explicit primary is set and other side is non-array,
    /// the field should be auto-marked as primary since the "other side"
    /// (same collection) doesn't have an inverse field for this relation
    #[test]
    fn test_self_ref_one_to_one_successor() {
        let leader_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // successor: Leader @relation(name: "succession")
            // No @primary - should auto-determine based on other side
            FieldDescription::new("3", "successor", FieldKind::relation("leaders", false))
                .with_relation_name("succession"),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "leaders".to_string(),
            CollectionVersion::new("leaders", "v1", "coll-leaders", leader_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let leaders = collections.get("leaders").unwrap();

        // successor should get _id field
        assert!(
            leaders.field_by_name("_successorID").is_some(),
            "successor should get _id field"
        );

        // successor should be auto-marked as primary since there's no inverse field
        // (other side doesn't exist, so this side becomes primary)
        assert!(
            leaders.field_by_name("successor").unwrap().is_primary,
            "successor should be auto-marked as primary"
        );
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        collections.insert(
            "users".to_string(),
            CollectionVersion::new("users", "v1", "coll-users", users_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let posts = collections.get("posts").unwrap();

        // Both should get separate _id fields
        let author_id_field = posts.field_by_name("_authorID").unwrap();
        let reviewer_id_field = posts.field_by_name("_reviewerID").unwrap();

        assert_eq!(
            author_id_field.relation_name,
            Some("post_author".to_string())
        );
        assert_eq!(
            reviewer_id_field.relation_name,
            Some("post_reviewer".to_string())
        );

        // Each should be primary independently
        assert!(author_id_field.is_primary);
        assert!(reviewer_id_field.is_primary);
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

        let mut collections = BTreeMap::new();
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

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let posts = collections.get("posts").unwrap();

        // Both relations should get _id fields
        assert!(posts.field_by_name("_authorID").is_some());
        assert!(posts.field_by_name("_categoryID").is_some());

        // Array sides should NOT get _id fields
        assert!(collections
            .get("users")
            .unwrap()
            .field_by_name("_postsID")
            .is_none());
        assert!(collections
            .get("categories")
            .unwrap()
            .field_by_name("_postsID")
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        // Both relations are independent and should both work
        assert!(collections
            .get("books")
            .unwrap()
            .field_by_name("_authorID")
            .is_some());
        assert!(collections
            .get("authors")
            .unwrap()
            .field_by_name("_favorite_bookID")
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let posts = collections.get("posts").unwrap();
        let field_names: Vec<&str> = posts.fields.iter().map(|f| f.name.as_str()).collect();

        // Find positions
        let author_pos = field_names.iter().position(|&n| n == "author").unwrap();
        let author_id_pos = field_names.iter().position(|&n| n == "_authorID").unwrap();
        let content_pos = field_names.iter().position(|&n| n == "content").unwrap();
        let category_pos = field_names.iter().position(|&n| n == "category").unwrap();
        let category_id_pos = field_names
            .iter()
            .position(|&n| n == "_categoryID")
            .unwrap();
        let tags_pos = field_names.iter().position(|&n| n == "tags").unwrap();

        // Verify ordering
        assert_eq!(
            author_id_pos,
            author_pos + 1,
            "_authorID should follow author"
        );
        assert!(
            content_pos > author_id_pos,
            "content should come after _authorID"
        );
        assert_eq!(
            category_id_pos,
            category_pos + 1,
            "_categoryID should follow category"
        );
        assert!(
            tags_pos > category_id_pos,
            "tags should come after _categoryID"
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("Author", "v1", "coll-authors", authors_fields),
        );
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("Book", "v1", "coll-books", books_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let authors = collections.get("authors").unwrap();
        let books = collections.get("books").unwrap();

        // Go behavior: non-array side (Book.author) is auto-set to primary
        assert!(books.field_by_name("author").unwrap().is_primary);

        // Go behavior: non-array side gets _id field
        let author_id_field = books.field_by_name("_authorID").unwrap();
        assert_eq!(author_id_field.kind, FieldKind::doc_id());
        assert_eq!(author_id_field.crdt_type, CType::LwwRegister);

        // Go behavior: array side does NOT get _id field
        assert!(authors.field_by_name("_publishedID").is_none());
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

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("Book", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("Author", "v1", "coll-authors", authors_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let books = collections.get("books").unwrap();
        let authors = collections.get("authors").unwrap();

        // Go behavior: explicit @primary is preserved
        assert!(authors.field_by_name("published").unwrap().is_primary);
        assert!(!books.field_by_name("author").unwrap().is_primary);

        // Go behavior: both sides get _id fields in one-to-one
        assert!(authors.field_by_name("_publishedID").is_some());
        assert!(books.field_by_name("_authorID").is_some());
    }
}

// =============================================================================
// MANY-TO-MANY RELATION TESTS (via junction collections)
// =============================================================================

mod many_to_many {
    use super::*;

    /// Standard many-to-many: Students <-> Courses via Enrollment junction
    /// Enrollment has two non-array relations, both should get _id fields
    #[test]
    fn test_junction_table_basic() {
        let students_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Array side of student-enrollment relation
            FieldDescription::new("3", "enrollments", FieldKind::relation("enrollments", true))
                .with_relation_name("student_enrollment"),
        ];
        let courses_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            // Array side of course-enrollment relation
            FieldDescription::new("3", "enrollments", FieldKind::relation("enrollments", true))
                .with_relation_name("course_enrollment"),
        ];
        let enrollments_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "grade", FieldKind::string()),
            // Non-array side: enrollment -> student
            FieldDescription::new("3", "student", FieldKind::relation("students", false))
                .with_relation_name("student_enrollment"),
            // Non-array side: enrollment -> course
            FieldDescription::new("4", "course", FieldKind::relation("courses", false))
                .with_relation_name("course_enrollment"),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "students".to_string(),
            CollectionVersion::new("students", "v1", "coll-students", students_fields),
        );
        collections.insert(
            "courses".to_string(),
            CollectionVersion::new("courses", "v1", "coll-courses", courses_fields),
        );
        collections.insert(
            "enrollments".to_string(),
            CollectionVersion::new("enrollments", "v1", "coll-enrollments", enrollments_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let students = collections.get("students").unwrap();
        let courses = collections.get("courses").unwrap();
        let enrollments = collections.get("enrollments").unwrap();

        // Junction table (enrollments) should have _id fields for both relations
        assert!(
            enrollments.field_by_name("_studentID").is_some(),
            "enrollment should have _studentID"
        );
        assert!(
            enrollments.field_by_name("_courseID").is_some(),
            "enrollment should have _courseID"
        );

        // Both non-array fields on junction table should be primary
        // (since the other side of each relation is an array)
        assert!(
            enrollments.field_by_name("student").unwrap().is_primary,
            "enrollment.student should be primary"
        );
        assert!(
            enrollments.field_by_name("course").unwrap().is_primary,
            "enrollment.course should be primary"
        );

        // Array sides should NOT have _id fields
        assert!(
            students.field_by_name("_enrollmentsID").is_none(),
            "students should not have _enrollmentsID"
        );
        assert!(
            courses.field_by_name("_enrollmentsID").is_none(),
            "courses should not have _enrollmentsID"
        );
    }

    /// Many-to-many with additional metadata: Posts <-> Tags via PostTag junction
    #[test]
    fn test_junction_table_with_metadata() {
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "post_tags", FieldKind::relation("post_tags", true))
                .with_relation_name("post_posttag"),
        ];
        let tags_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "post_tags", FieldKind::relation("post_tags", true))
                .with_relation_name("tag_posttag"),
        ];
        // Junction table with metadata fields
        let post_tags_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "post", FieldKind::relation("posts", false))
                .with_relation_name("post_posttag"),
            FieldDescription::new("3", "tag", FieldKind::relation("tags", false))
                .with_relation_name("tag_posttag"),
            // Metadata: when was this tag applied
            FieldDescription::new("4", "applied_at", FieldKind::datetime()),
            // Metadata: who applied the tag
            FieldDescription::new("5", "applied_by", FieldKind::string()),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        collections.insert(
            "tags".to_string(),
            CollectionVersion::new("tags", "v1", "coll-tags", tags_fields),
        );
        collections.insert(
            "post_tags".to_string(),
            CollectionVersion::new("post_tags", "v1", "coll-post-tags", post_tags_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let post_tags = collections.get("post_tags").unwrap();

        // Junction table should have _id fields for both foreign keys
        assert!(post_tags.field_by_name("_postID").is_some());
        assert!(post_tags.field_by_name("_tagID").is_some());

        // Both should be primary (other sides are arrays)
        assert!(post_tags.field_by_name("post").unwrap().is_primary);
        assert!(post_tags.field_by_name("tag").unwrap().is_primary);

        // Verify _id fields are positioned correctly (after their relation fields)
        let field_names: Vec<&str> = post_tags.fields.iter().map(|f| f.name.as_str()).collect();
        let post_pos = field_names.iter().position(|&n| n == "post").unwrap();
        let post_id_pos = field_names.iter().position(|&n| n == "_postID").unwrap();
        let tag_pos = field_names.iter().position(|&n| n == "tag").unwrap();
        let tag_id_pos = field_names.iter().position(|&n| n == "_tagID").unwrap();

        assert_eq!(post_id_pos, post_pos + 1, "_postID should follow post");
        assert_eq!(tag_id_pos, tag_pos + 1, "_tagID should follow tag");
    }

    /// Self-referential many-to-many: Users following Users via Follows junction
    #[test]
    fn test_self_ref_many_to_many() {
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Users I follow
            FieldDescription::new("3", "following", FieldKind::relation("follows", true))
                .with_relation_name("user_following"),
            // Users following me
            FieldDescription::new("4", "followers", FieldKind::relation("follows", true))
                .with_relation_name("user_follower"),
        ];
        // Junction table for follower relationship
        let follows_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            // The user doing the following
            FieldDescription::new("2", "follower", FieldKind::relation("users", false))
                .with_relation_name("user_follower"),
            // The user being followed
            FieldDescription::new("3", "followee", FieldKind::relation("users", false))
                .with_relation_name("user_following"),
            // When the follow happened
            FieldDescription::new("4", "followed_at", FieldKind::datetime()),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "users".to_string(),
            CollectionVersion::new("users", "v1", "coll-users", users_fields),
        );
        collections.insert(
            "follows".to_string(),
            CollectionVersion::new("follows", "v1", "coll-follows", follows_fields),
        );

        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        let users = collections.get("users").unwrap();
        let follows = collections.get("follows").unwrap();

        // Junction table should have _id fields for both foreign keys
        assert!(
            follows.field_by_name("_followerID").is_some(),
            "follows should have _followerID"
        );
        assert!(
            follows.field_by_name("_followeeID").is_some(),
            "follows should have _followeeID"
        );

        // Both junction fields should be primary
        assert!(follows.field_by_name("follower").unwrap().is_primary);
        assert!(follows.field_by_name("followee").unwrap().is_primary);

        // Array sides on users should NOT have _id fields
        assert!(users.field_by_name("_followingID").is_none());
        assert!(users.field_by_name("_followersID").is_none());
    }
}

// =============================================================================
// FINALIZE-THEN-VALIDATE INTEGRATION TESTS
// =============================================================================

mod finalize_validate_integration {
    use super::*;
    use schema::validate_schema;
    use std::collections::HashMap;

    /// Verify that finalize_relations runs BEFORE validate_schema
    /// A one-to-many relation with no explicit primary should:
    /// 1. Have auto-primary applied by finalization
    /// 2. Then pass validation (exactly one primary)
    #[test]
    fn test_finalize_then_validate_one_to_many() {
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                .with_relation_name("user_posts"),
            // Note: No explicit primary on array side
        ];
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts"),
            // Note: No explicit primary - should be auto-set by finalization
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

        // Step 1: Finalize relations (auto-sets primary on non-array side)
        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        // Verify auto-primary was applied
        assert!(
            collections
                .get("posts")
                .unwrap()
                .field_by_name("author")
                .unwrap()
                .is_primary,
            "finalize_relations should auto-mark author as primary"
        );

        // Step 2: Validate schema (should pass because exactly one side is primary)
        let hashmap_collections: HashMap<String, CollectionVersion> =
            collections.into_iter().collect();
        let result = validate_schema(&hashmap_collections);
        assert!(
            result.is_ok(),
            "validate_schema should pass after finalization: {:?}",
            result
        );
    }

    /// Verify validation fails when run BEFORE finalization for one-to-one
    /// (both sides non-array, neither marked primary)
    #[test]
    fn test_validate_without_finalize_fails_one_to_one() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author"),
            // Note: No explicit primary
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author"),
            // Note: No explicit primary - one-to-one requires exactly one
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

        // Validate WITHOUT finalization - should fail (neither side is primary)
        let result = validate_schema(&collections);
        assert!(
            result.is_err(),
            "validate_schema should fail for one-to-one without explicit primary and no finalization"
        );
    }

    /// Verify the correct workflow: finalize → validate for one-to-one with explicit primary
    #[test]
    fn test_finalize_then_validate_one_to_one_explicit() {
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author")
                .as_primary(), // Explicit primary
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author"),
            // Not primary
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        // Step 1: Finalize
        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        // Verify explicit primary is preserved
        assert!(
            collections
                .get("books")
                .unwrap()
                .field_by_name("author")
                .unwrap()
                .is_primary
        );
        assert!(
            !collections
                .get("authors")
                .unwrap()
                .field_by_name("published")
                .unwrap()
                .is_primary
        );

        // Step 2: Validate
        let hashmap_collections: HashMap<String, CollectionVersion> =
            collections.into_iter().collect();
        let result = validate_schema(&hashmap_collections);
        assert!(result.is_ok(), "validation should pass: {:?}", result);
    }

    /// Verify junction tables pass finalize → validate workflow
    #[test]
    fn test_finalize_then_validate_junction_table() {
        let students_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "enrollments", FieldKind::relation("enrollments", true))
                .with_relation_name("student_enrollment"),
        ];
        let courses_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "enrollments", FieldKind::relation("enrollments", true))
                .with_relation_name("course_enrollment"),
        ];
        let enrollments_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "student", FieldKind::relation("students", false))
                .with_relation_name("student_enrollment"),
            FieldDescription::new("3", "course", FieldKind::relation("courses", false))
                .with_relation_name("course_enrollment"),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "students".to_string(),
            CollectionVersion::new("students", "v1", "coll-students", students_fields),
        );
        collections.insert(
            "courses".to_string(),
            CollectionVersion::new("courses", "v1", "coll-courses", courses_fields),
        );
        collections.insert(
            "enrollments".to_string(),
            CollectionVersion::new("enrollments", "v1", "coll-enrollments", enrollments_fields),
        );

        // Finalize
        CollectionVersion::finalize_relations(&mut collections, field_id_generator()).unwrap();

        // Both junction fields should be auto-marked as primary
        let enrollments = collections.get("enrollments").unwrap();
        assert!(enrollments.field_by_name("student").unwrap().is_primary);
        assert!(enrollments.field_by_name("course").unwrap().is_primary);

        // Validate
        let hashmap_collections: HashMap<String, CollectionVersion> =
            collections.into_iter().collect();
        let result = validate_schema(&hashmap_collections);
        assert!(
            result.is_ok(),
            "junction table should pass validation: {:?}",
            result
        );
    }
}
