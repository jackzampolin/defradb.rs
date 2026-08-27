use integration_test::{DefraClient, TestCluster};

/// Mirrors the Go `query/simple` fixture
/// (`tests/integration/query/simple/utils.go`).
fn add_users_schema(node: &DefraClient) {
    node.schema_add(
        r#"
        type Users {
            Name: String
            Email: String
            Age: Int
            HeightM: Float
            Verified: Boolean
            CreatedAt: DateTime
        }
        "#,
    )
    .expect("add schema");
}

fn add_user(node: &DefraClient, name: &str, age: i64) {
    node.query(&format!(
        r#"mutation {{ add_Users(input: {{Name: "{name}", Age: {age}}}) {{ _docID }} }}"#
    ))
    .unwrap_or_else(|e| panic!("add user {name}: {e}"));
}

/// Mirrors the Go `query/one_to_many` fixture
/// (`tests/integration/query/one_to_many/utils.go`).
fn add_book_author_schema(node: &DefraClient) {
    node.schema_add(
        r#"
        type Book {
            name: String
            rating: Float
            author: Author
        }

        type Author {
            name: String
            age: Int
            verified: Boolean
            published: [Book]
        }
        "#,
    )
    .expect("add schema");
}

fn add_author(node: &DefraClient, name: &str, age: i64, verified: bool) -> String {
    let result = node
        .query(&format!(
            r#"mutation {{ add_Author(input: {{name: "{name}", age: {age}, verified: {verified}}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("add author {name}: {e}"));
    result["add_Author"][0]["_docID"]
        .as_str()
        .expect("author _docID")
        .to_string()
}

fn add_book(node: &DefraClient, name: &str, rating: f64, author: &str) {
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{name: "{name}", rating: {rating}, author: "{author}"}}) {{ _docID }} }}"#
    ))
    .unwrap_or_else(|e| panic!("add book {name}: {e}"));
}

fn count_for(result: &serde_json::Value, author: &str) -> i64 {
    result["Author"]
        .as_array()
        .expect("Author array")
        .iter()
        .find(|a| a["name"] == author)
        .unwrap_or_else(|| panic!("author {author} missing"))["COUNT"]
        .as_i64()
        .expect("numeric COUNT")
}

/// Go: TestQuerySimple_WithCountWithGroupBy_OnSingleField
/// Three docs, two distinct ages -> 2 groups.
async fn on_single_field_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_users_schema(&node);
    add_user(&node, "John", 32);
    add_user(&node, "Bob", 32);
    add_user(&node, "Alice", 19);

    let result = node
        .query(r#"query { COUNT(Users: {groupBy: [Age]}) }"#)
        .expect("count query");

    assert_eq!(result["COUNT"], serde_json::json!(2));
}

/// Go: TestQuerySimple_WithCountWithGroupBy_OnMultipleFields
/// Three distinct (Age, Name) pairs -> 3 groups. Guard: also passes when
/// groupBy is dropped, so it only protects against over-grouping.
async fn on_multiple_fields_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_users_schema(&node);
    add_user(&node, "John", 32);
    add_user(&node, "Bob", 32);
    add_user(&node, "John", 19);

    let result = node
        .query(r#"query { COUNT(Users: {groupBy: [Age, Name]}) }"#)
        .expect("count query");

    assert_eq!(result["COUNT"], serde_json::json!(3));
}

/// Go: TestQuerySimple_WithCountWithGroupBy_AllSameGroupValue
/// Three docs sharing one age -> 1 group.
async fn all_same_group_value_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_users_schema(&node);
    add_user(&node, "John", 32);
    add_user(&node, "Bob", 32);
    add_user(&node, "Alice", 32);

    let result = node
        .query(r#"query { COUNT(Users: {groupBy: [Age]}) }"#)
        .expect("count query");

    assert_eq!(result["COUNT"], serde_json::json!(1));
}

/// Go: TestQuerySimple_WithCountWithGroupByAndFilter
/// Filter keeps ages 32, 32, 25 -> 2 groups.
async fn group_by_and_filter_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_users_schema(&node);
    add_user(&node, "John", 32);
    add_user(&node, "Bob", 32);
    add_user(&node, "Alice", 19);
    add_user(&node, "Chris", 25);

    let result = node
        .query(r#"query { COUNT(Users: {groupBy: [Age], filter: {Age: {_gt: 20}}}) }"#)
        .expect("count query");

    assert_eq!(result["COUNT"], serde_json::json!(2));
}

/// Go: TestQueryOneToMany_WithCountWithGroupBy_AllSameName
/// Three books sharing one name -> 1 group.
async fn relation_all_same_name_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_book_author_schema(&node);
    let john = add_author(&node, "John Grisham", 65, true);
    add_book(&node, "Painted House", 4.9, &john);
    add_book(&node, "Painted House", 4.8, &john);
    add_book(&node, "Painted House", 4.7, &john);

    let result = node
        .query(
            r#"
            query {
                Author {
                    name
                    COUNT(published: {groupBy: [name]})
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(count_for(&result, "John Grisham"), 1);
}

/// Go: TestQueryOneToMany_WithCountWithGroupBy_CountingDistinctNames
/// John has three books under two distinct names, Cornelia has one.
async fn relation_counting_distinct_names_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_book_author_schema(&node);
    let john = add_author(&node, "John Grisham", 65, true);
    let cornelia = add_author(&node, "Cornelia Funke", 62, false);
    add_book(&node, "Painted House", 4.9, &john);
    add_book(&node, "Painted House", 4.8, &john);
    add_book(&node, "A Time for Mercy", 4.5, &john);
    add_book(&node, "Theif Lord", 4.7, &cornelia);

    let result = node
        .query(
            r#"
            query {
                Author {
                    name
                    COUNT(published: {groupBy: [name]})
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(count_for(&result, "John Grisham"), 2);
    assert_eq!(count_for(&result, "Cornelia Funke"), 1);
}

/// Four books under John, three distinct names (one book has no name at all,
/// which keys as a single null group). A grouped target must not share the
/// plain target's join, so both counts must hold whichever is written first.
async fn setup_mixed_names(node: &DefraClient) {
    add_book_author_schema(node);
    let john = add_author(node, "John Grisham", 65, true);
    add_book(node, "Painted House", 4.9, &john);
    add_book(node, "Painted House", 4.8, &john);
    add_book(node, "A Time for Mercy", 4.5, &john);
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{rating: 4.2, author: "{john}"}}) {{ _docID }} }}"#
    ))
    .expect("add unnamed book");
}

async fn grouped_count_plain_first_test(cluster: TestCluster) {
    let node = cluster.client(0);
    setup_mixed_names(&node).await;

    let result = node
        .query(
            r#"
            query {
                Author {
                    name
                    a: COUNT(published: {})
                    b: COUNT(published: {groupBy: [name]})
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(
        result["Author"],
        serde_json::json!([{"name": "John Grisham", "a": 4, "b": 3}])
    );
}

async fn grouped_count_grouped_first_test(cluster: TestCluster) {
    let node = cluster.client(0);
    setup_mixed_names(&node).await;

    let result = node
        .query(
            r#"
            query {
                Author {
                    name
                    b: COUNT(published: {groupBy: [name]})
                    a: COUNT(published: {})
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(
        result["Author"],
        serde_json::json!([{"name": "John Grisham", "b": 3, "a": 4}])
    );
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_on_single_field() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    on_single_field_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_on_multiple_fields() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    on_multiple_fields_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_all_same_group_value() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    all_same_group_value_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_group_by_and_filter() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    group_by_and_filter_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_relation_all_same_name() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    relation_all_same_name_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_relation_counting_distinct_names() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    relation_counting_distinct_names_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_grouped_count_plain_first() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    grouped_count_plain_first_test(cluster).await;
}

#[tokio::test]
async fn rust_aggregate_groupby_1595_grouped_count_grouped_first() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    grouped_count_grouped_first_test(cluster).await;
}
