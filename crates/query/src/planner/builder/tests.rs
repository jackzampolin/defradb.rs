use super::*;
use crate::mapper::{Field, Filter};
use crate::planner::index_selection::IndexScanType;
use schema::{FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription};

fn map<const N: usize>(
    entries: [(String, serde_json::Value); N],
) -> serde_json::Map<String, serde_json::Value> {
    entries.into_iter().collect()
}

fn make_test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
}

fn make_test_collection_with_index() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
    .with_index(IndexDescription {
        id: 1,
        name: "name_idx".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
    })
    .with_index(IndexDescription {
        id: 2,
        name: "age_idx".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "age".to_string(),
            descending: false,
        }],
    })
}

fn make_users_collection() -> CollectionVersion {
    CollectionVersion::new(
        "users",
        "v1",
        "coll-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // One-to-many relation to posts (array)
            FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                .with_relation_name("author_posts"),
        ],
    )
}

fn make_posts_collection() -> CollectionVersion {
    CollectionVersion::new(
        "posts",
        "v1",
        "coll-posts",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            // Many-to-one relation to users (singular)
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("author_posts")
                .as_primary(),
            // Auto-generated FK field (Go naming: _{fieldname}ID)
            FieldDescription::new("4", "_authorID", FieldKind::doc_id())
                .with_relation_name("author_posts")
                .as_primary(),
        ],
    )
}

#[test]
fn test_planner_new() {
    let planner = Planner::new(vec![make_test_collection()]);
    assert!(planner.collection("Users").is_some());
    assert!(planner.collection("Posts").is_none());
}

#[tokio::test]
async fn test_plan_simple_select() {
    let planner = Planner::new(vec![make_test_collection()]);

    let select = Select::new("Users")
        .with_field(Field::new("_docID"))
        .with_field(Field::new("name"));

    let plan = planner.plan(&select).unwrap();
    assert_eq!(plan.kind(), "selectNode");
}

#[tokio::test]
async fn test_plan_with_limit() {
    let planner = Planner::new(vec![make_test_collection()]);

    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_limit(10);

    let plan = planner.plan(&select).unwrap();
    assert_eq!(plan.kind(), "limitNode");
}

#[tokio::test]
async fn test_plan_unknown_collection() {
    let planner = Planner::new(vec![make_test_collection()]);

    let select = Select::new("Posts").with_field(Field::new("title"));

    let result = planner.plan(&select);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_plan_with_filter() {
    let planner = Planner::new(vec![make_test_collection()]);

    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Alice"}),
    )]));

    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(filter);

    let plan = planner.plan(&select).unwrap();
    assert_eq!(plan.kind(), "selectNode");
}

#[test]
fn test_build_mapping() {
    let planner = Planner::new(vec![make_test_collection()]);
    let collection = planner.collection("Users").unwrap();

    let select = Select::new("Users")
        .with_field(Field::new("_docID"))
        .with_field(Field::new("name"));

    let mapping = planner.build_mapping(&select, collection).unwrap();

    assert!(mapping.has_field("_docID"));
    assert!(mapping.has_field("name"));
    assert!(!mapping.has_field("age"));
}

#[test]
fn test_build_mapping_with_alias() {
    let planner = Planner::new(vec![make_test_collection()]);
    let collection = planner.collection("Users").unwrap();

    let select = Select::new("Users").with_field(Field::with_alias("name", "userName"));

    let mapping = planner.build_mapping(&select, collection).unwrap();

    assert!(mapping.has_field("name"));
    // Should have render key "userName"
    assert_eq!(mapping.render_keys.len(), 1);
    assert_eq!(mapping.render_keys[0].key, "userName");
}

// === Index-Aware Planning Tests ===

#[tokio::test]
async fn test_plan_uses_index_for_eq_filter() {
    let planner = Planner::new(vec![make_test_collection_with_index()]);

    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Alice"}),
    )]));

    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // Should use index
    assert!(result.uses_index());
    assert_eq!(result.index_scan.as_ref().unwrap().index_name, "name_idx");

    // Plan should have indexScanNode at the leaf
    // (wrapped by selectNode for field projection)
    assert_eq!(result.plan.kind(), "selectNode");
}

#[tokio::test]
async fn test_plan_uses_index_for_range_filter() {
    let planner = Planner::new(vec![make_test_collection_with_index()]);

    let filter = Filter::from_conditions(map([(
        "age".to_string(),
        serde_json::json!({"_gte": 18, "_lt": 65}),
    )]));

    let select = Select::new("Users")
        .with_field(Field::new("age"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // Should use age index
    assert!(result.uses_index());
    assert_eq!(result.index_scan.as_ref().unwrap().index_name, "age_idx");

    // Verify it's a range scan
    match &result.index_scan.as_ref().unwrap().scan_type {
        IndexScanType::RangeScan { .. } => {}
        _ => panic!("expected RangeScan"),
    }
}

#[tokio::test]
async fn test_plan_no_index_without_filter() {
    let planner = Planner::new(vec![make_test_collection_with_index()]);

    let select = Select::new("Users").with_field(Field::new("name"));

    let result = planner.plan_with_index_info(&select).unwrap();

    // No filter, so no index should be used
    assert!(!result.uses_index());
}

#[tokio::test]
async fn test_plan_no_index_for_non_indexed_field() {
    // Collection without indexes
    let planner = Planner::new(vec![make_test_collection()]);

    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Alice"}),
    )]));

    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // No indexes available, so shouldn't use index
    assert!(!result.uses_index());
}

#[tokio::test]
async fn test_plan_uses_index_for_ne_filter() {
    let planner = Planner::new(vec![make_test_collection_with_index()]);

    // _ne uses full index scan (matching Go behavior)
    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_ne": "Alice"}),
    )]));

    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // _ne uses full index scan
    assert!(result.uses_index());
}

#[tokio::test]
async fn test_plan_uses_index_for_in_filter() {
    let planner = Planner::new(vec![make_test_collection_with_index()]);

    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_in": ["Alice", "Bob"]}),
    )]));

    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // _in can use index
    assert!(result.uses_index());
    assert_eq!(result.index_scan.as_ref().unwrap().index_name, "name_idx");

    match &result.index_scan.as_ref().unwrap().scan_type {
        IndexScanType::InScan { values, .. } => {
            assert_eq!(values.len(), 2);
        }
        _ => panic!("expected InScan"),
    }
}

#[tokio::test]
async fn test_plan_result_uses_index_method() {
    let planner = Planner::new(vec![make_test_collection_with_index()]);

    // With index
    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Alice"}),
    )]));
    let select = Select::new("Users")
        .with_field(Field::new("name"))
        .with_filter(filter);
    let result = planner.plan_with_index_info(&select).unwrap();
    assert!(result.uses_index());

    // Without index (no filter)
    let select_no_filter = Select::new("Users").with_field(Field::new("name"));
    let result_no_filter = planner.plan_with_index_info(&select_no_filter).unwrap();
    assert!(!result_no_filter.uses_index());
}

// ========================================================================
// Join Planning Tests
// ========================================================================

#[tokio::test]
async fn test_plan_with_one_to_one_relation() {
    // Query: posts { title, author { name } }
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Build nested select for author - field name is "author" (relation field), collection is "users"
    let author_select = Select::new("users")
        .with_field_name("author")
        .with_field(Field::new("name"));

    let select = Select::new("posts")
        .with_field(Field::new("title"))
        .with_select(author_select);

    let plan = planner.plan(&select).unwrap();

    // After plan_with_index_info: ScanNode → TypeJoinOne → SelectNode
    // Outermost is SelectNode (Go DefraDB plan order: joins before select)
    assert_eq!(plan.kind(), "selectNode");
    let source = plan.source().unwrap();
    assert_eq!(source.kind(), "typeIndexJoin");
}

#[tokio::test]
async fn test_plan_with_one_to_many_relation() {
    // Query: users { name, posts { title } }
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Build nested select for posts - field name is "posts" (relation field), collection is "posts"
    let posts_select = Select::new("posts")
        .with_field_name("posts")
        .with_field(Field::new("title"));

    let select = Select::new("users")
        .with_field(Field::new("name"))
        .with_select(posts_select);

    let plan = planner.plan(&select).unwrap();

    // After plan_with_index_info: ScanNode → TypeJoinMany → SelectNode
    // Outermost is SelectNode (Go DefraDB plan order: joins before select)
    assert_eq!(plan.kind(), "selectNode");
    let source = plan.source().unwrap();
    assert_eq!(source.kind(), "typeIndexJoin");
}

#[tokio::test]
async fn test_plan_relation_unknown_field() {
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Try to select a non-existent relation field
    let nested = Select::new("users")
        .with_field_name("nonexistent")
        .with_field(Field::new("name"));

    let select = Select::new("posts")
        .with_field(Field::new("title"))
        .with_select(nested);

    let result = planner.plan(&select);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_plan_relation_with_limit() {
    // Query: users { name, posts { title } } limit 5
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    let posts_select = Select::new("posts")
        .with_field_name("posts")
        .with_field(Field::new("title"));

    let select = Select::new("users")
        .with_field(Field::new("name"))
        .with_select(posts_select)
        .with_limit(5);

    let plan = planner.plan(&select).unwrap();

    // The outermost node should be a LimitNode
    assert_eq!(plan.kind(), "limitNode");

    // The source should be SelectNode (which wraps the join)
    let source = plan.source().unwrap();
    assert_eq!(source.kind(), "selectNode");

    // SelectNode's source should be the join
    let join = source.source().unwrap();
    assert_eq!(join.kind(), "typeIndexJoin");
}

// ========================================================================
// Nested Relation Filter Tests
// ========================================================================

#[tokio::test]
async fn test_plan_with_nested_filter_on_type_join_many() {
    // Query: users { name, posts(filter: { title: { _eq: "Hello" } }) { title } }
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Build nested select with filter
    let filter = Filter::from_conditions(map([(
        "title".to_string(),
        serde_json::json!({"_eq": "Hello"}),
    )]));

    let posts_select = Select::new("posts")
        .with_field_name("posts")
        .with_field(Field::new("title"))
        .with_filter(filter);

    let select = Select::new("users")
        .with_field(Field::new("name"))
        .with_select(posts_select);

    let plan = planner.plan(&select).unwrap();

    // The outermost node should be selectNode (wraps the join)
    assert_eq!(plan.kind(), "selectNode");

    // SelectNode's source should be typeIndexJoin
    let source = plan.source().unwrap();
    assert_eq!(source.kind(), "typeIndexJoin");
}

#[tokio::test]
async fn test_plan_with_nested_filter_on_type_join_one() {
    // Query: posts { title, author(filter: { name: { _eq: "Alice" } }) { name } }
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Build nested select with filter
    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Alice"}),
    )]));

    let author_select = Select::new("users")
        .with_field_name("author")
        .with_field(Field::new("name"))
        .with_filter(filter);

    let select = Select::new("posts")
        .with_field(Field::new("title"))
        .with_select(author_select);

    let plan = planner.plan(&select).unwrap();

    // The outermost node should be selectNode (wraps the join)
    assert_eq!(plan.kind(), "selectNode");

    // SelectNode's source should be typeIndexJoin
    let source = plan.source().unwrap();
    assert_eq!(source.kind(), "typeIndexJoin");
}

#[tokio::test]
async fn test_plan_nested_filter_with_parent_filter() {
    // Query: users(filter: { name: { _eq: "Bob" } }) {
    //   name,
    //   posts(filter: { title: { _like: "Hello%" } }) { title }
    // }
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Parent filter
    let parent_filter = Filter::from_conditions(map([(
        "name".to_string(),
        serde_json::json!({"_eq": "Bob"}),
    )]));

    // Child filter
    let child_filter = Filter::from_conditions(map([(
        "title".to_string(),
        serde_json::json!({"_like": "Hello%"}),
    )]));

    let posts_select = Select::new("posts")
        .with_field_name("posts")
        .with_field(Field::new("title"))
        .with_filter(child_filter);

    let select = Select::new("users")
        .with_field(Field::new("name"))
        .with_select(posts_select)
        .with_filter(parent_filter);

    let plan = planner.plan(&select).unwrap();

    // The outermost node should be selectNode (wraps the join)
    assert_eq!(plan.kind(), "selectNode");

    // SelectNode's source should be typeIndexJoin
    let source = plan.source().unwrap();
    assert_eq!(source.kind(), "typeIndexJoin");
}

#[tokio::test]
async fn test_plan_nested_filter_references_unselected_field_fails_at_planning() {
    // Query: users { posts(filter: { author_id: { _eq: "user-1" } }) { title } }
    // The filter references "author_id" but the select only includes "title"
    // This should fail at planning time with a clear error message
    let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

    // Filter references "author_id" which is NOT in the select list
    let filter = Filter::from_conditions(map([(
        "author_id".to_string(),
        serde_json::json!({"_eq": "user-1"}),
    )]));

    let posts_select = Select::new("posts")
        .with_field_name("posts")
        .with_field(Field::new("title")) // Only selecting "title", not "author_id"
        .with_filter(filter);

    let select = Select::new("users")
        .with_field(Field::new("name"))
        .with_select(posts_select);

    let result = planner.plan(&select);

    // Should fail at planning time
    let err = match result {
        Ok(_) => panic!("Expected error but got Ok"),
        Err(e) => e,
    };

    // Error message should indicate the filter field is not in the select list
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("author_id"),
        "Error should mention the field name: {}",
        err_msg
    );
    assert!(
        err_msg.contains("select list") || err_msg.contains("posts"),
        "Error should mention select list or collection: {}",
        err_msg
    );
}

// === Secondary Relation ID Field Tests ===

/// Book collection - secondary side of Book-Author relation
/// author: Author (NO @primary - secondary side, doesn't store FK)
fn make_book_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Book",
        "v1",
        "coll-book",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Secondary relation to Author (no @primary)
            FieldDescription::new("3", "author", FieldKind::relation("Author", false))
                .with_relation_name("book_author"),
            // Auto-generated _authorID field (added by add_relation_id_fields)
            // Even though this is secondary, the _authorID field exists for querying
            FieldDescription::new("4", "_authorID", FieldKind::doc_id())
                .with_relation_name("book_author"),
        ],
    )
}

/// Author collection - primary side of Book-Author relation
/// published: Book @primary (stores the FK _publishedID)
fn make_author_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Author",
        "v1",
        "coll-author",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Primary relation to Book (@primary - stores FK)
            FieldDescription::new("3", "published", FieldKind::relation("Book", false))
                .with_relation_name("book_author")
                .as_primary(),
            // Auto-generated _publishedID field (primary side stores FK)
            FieldDescription::new("4", "_publishedID", FieldKind::doc_id())
                .with_relation_name("book_author")
                .as_primary(),
        ],
    )
}

#[test]
fn test_secondary_relation_id_field_detection() {
    // Verify the string slicing for extracting relation name from _authorID
    let field_name = "_authorID";
    assert!(field_name.starts_with('_'));
    assert!(field_name.ends_with("ID"));

    // Extract relation name
    let relation_name = &field_name[1..field_name.len() - 2];
    assert_eq!(relation_name, "author");

    // Verify Book collection has author field
    let book = make_book_collection();
    let author_field = book.field_by_name("author");
    assert!(author_field.is_some(), "Book should have 'author' field");

    let author_field = author_field.unwrap();
    assert!(
        author_field.kind.is_relation(),
        "'author' should be a relation field"
    );
    assert!(
        !author_field.is_primary,
        "'author' on Book should NOT be primary (secondary side)"
    );

    // Verify Author collection has published field
    let author = make_author_collection();
    let published_field = author.field_by_name("published");
    assert!(
        published_field.is_some(),
        "Author should have 'published' field"
    );

    let published_field = published_field.unwrap();
    assert!(
        published_field.kind.is_relation(),
        "'published' should be a relation field"
    );
    assert!(
        published_field.is_primary,
        "'published' on Author SHOULD be primary (primary side)"
    );
}

// ========================================================================
// Index + Relation Filter Tests (Go parity: typeIndexJoin for FK filters)
// ========================================================================

/// User collection for index-relation tests (one-to-many: User has [Device])
fn make_user_with_name_index() -> CollectionVersion {
    CollectionVersion::new(
        "User",
        "v1",
        "coll-user",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // One-to-many relation to Device (array)
            FieldDescription::new("3", "devices", FieldKind::relation("Device", true))
                .with_relation_name("owner_devices"),
        ],
    )
    .with_index(IndexDescription {
        id: 1,
        name: "User_name_ASC".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
    })
}

/// Device collection: `owner: User @index` creates index on `_ownerID`
fn make_device_with_owner_index() -> CollectionVersion {
    CollectionVersion::new(
        "Device",
        "v1",
        "coll-device",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "model", FieldKind::string()),
            FieldDescription::new("3", "manufacturer", FieldKind::string()),
            // Many-to-one relation to User
            FieldDescription::new("4", "owner", FieldKind::relation("User", false))
                .with_relation_name("owner_devices")
                .as_primary(),
            // Auto-generated FK field
            FieldDescription::new("5", "_ownerID", FieldKind::doc_id())
                .with_relation_name("owner_devices")
                .as_primary(),
        ],
    )
    // `owner: User @index` creates a non-unique index on _ownerID
    .with_index(IndexDescription {
        id: 1,
        name: "Device__ownerID_ASC".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "_ownerID".to_string(),
            descending: false,
        }],
    })
}

#[tokio::test]
async fn test_plan_fk_filter_eq_null_uses_index() {
    // Go: TestQueryWithIndexOnOneToMany_IfIndexedRelationIsNil_EqNilFilterShouldUseIndex
    // Filter: {_ownerID: {_eq: null}} — direct FK filter should use _ownerID index
    let planner = Planner::new(vec![
        make_user_with_name_index(),
        make_device_with_owner_index(),
    ]);

    let filter = Filter::from_conditions(map([(
        "_ownerID".to_string(),
        serde_json::json!({"_eq": null}),
    )]));

    let select = Select::new("Device")
        .with_field(Field::new("model"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();
    assert!(
        result.uses_index(),
        "Filter on _ownerID should use index on _ownerID"
    );
    assert_eq!(
        result.index_scan.as_ref().unwrap().index_name,
        "Device__ownerID_ASC"
    );
}

#[tokio::test]
async fn test_plan_fk_filter_ne_null_uses_index() {
    // Go: TestQueryWithIndexOnOneToMany_IfIndexedRelationIsNil_NeNilFilterShouldUseIndex
    // Filter: {_ownerID: {_neq: null}} — should use full index scan
    let planner = Planner::new(vec![
        make_user_with_name_index(),
        make_device_with_owner_index(),
    ]);

    let filter = Filter::from_conditions(map([(
        "_ownerID".to_string(),
        serde_json::json!({"_ne": null}),
    )]));

    let select = Select::new("Device")
        .with_field(Field::new("model"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();
    assert!(
        result.uses_index(),
        "Filter on _ownerID with _ne should use index"
    );
    assert_eq!(
        result.index_scan.as_ref().unwrap().index_name,
        "Device__ownerID_ASC"
    );
}

#[tokio::test]
async fn test_plan_relation_filter_still_uses_fk_index() {
    // Go: TestQueryWithIndexOnManyToOne_IfFilterOnIndexedRelation_ShouldFilterWithExplain
    // Filter: {owner: {name: {_eq: "Keenan"}}} — relation filter.
    // Even with a relation filter, the FK index on _ownerID should still be usable.
    // Go produces typeIndexJoin where root uses _ownerID index.
    // The `has_relation_filter` guard must not block FK-based index selection.
    let planner = Planner::new(vec![
        make_user_with_name_index(),
        make_device_with_owner_index(),
    ]);

    let filter = Filter::from_conditions(map([(
        "owner".to_string(),
        serde_json::json!({"name": {"_eq": "Keenan"}}),
    )]));

    let select = Select::new("Device")
        .with_field(Field::new("model"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // The plan should still produce a typeIndexJoin (join node)
    // even though we have a relation filter
    let plan = result.plan;
    assert_eq!(plan.kind(), "selectNode");
    let source = plan.source().unwrap();
    assert_eq!(
        source.kind(),
        "typeIndexJoin",
        "Relation filter should produce a typeIndexJoin node"
    );
}

#[tokio::test]
async fn test_plan_mixed_scalar_and_relation_filter_uses_index() {
    // Go: TestQueryWithIndex_WithMultipleScalarsAndRelationFilter_ShouldApplyAllAsAnd
    // Filter has both scalar (_ownerID or model) and relation (owner: {...}) conditions.
    // Scalar conditions on indexed fields should still use the index.
    // Add a model index for this test
    let mut device = make_device_with_owner_index();
    device.indexes.push(IndexDescription {
        id: 2,
        name: "Device_model_ASC".to_string(),
        unique: false,
        fields: vec![IndexedFieldDescription {
            name: "model".to_string(),
            descending: false,
        }],
    });

    let planner = Planner::new(vec![make_user_with_name_index(), device]);

    let filter = Filter::from_conditions(map([
        ("model".to_string(), serde_json::json!({"_eq": "iPhone"})),
        (
            "owner".to_string(),
            serde_json::json!({"name": {"_eq": "Keenan"}}),
        ),
    ]));

    let select = Select::new("Device")
        .with_field(Field::new("model"))
        .with_filter(filter);

    let result = planner.plan_with_index_info(&select).unwrap();

    // Should use an index for the scalar part (model)
    // even though there's also a relation filter
    assert!(
        result.uses_index(),
        "Mixed scalar+relation filter should use index for scalar part"
    );
}

// === Secondary Relation ID Field Tests ===

#[tokio::test]
async fn test_plan_secondary_relation_id_field() {
    // Test that selecting _authorID on Book (secondary side) creates a TypeJoin
    let planner = Planner::new(vec![make_book_collection(), make_author_collection()]);

    // Query: Book { name _authorID }
    let select = Select::new("Book")
        .with_field(Field::new("name"))
        .with_field(Field::new("_authorID"));

    let result = planner.plan(&select);
    assert!(
        result.is_ok(),
        "Planning should succeed: {:?}",
        result.err()
    );

    let plan = result.unwrap();
    // The plan should have a TypeJoinOne node somewhere in the tree
    // since we need to do a reverse lookup for _authorID

    // For now, just verify the plan was created
    // A more thorough test would execute the plan with test data
    assert!(
        plan.kind() == "selectNode" || plan.kind() == "typeJoinOne",
        "Plan should be selectNode or typeJoinOne, got: {}",
        plan.kind()
    );
}
