//! Cursor pagination over composite indexes.
//!
//! Validates the composite prefix rule from Go's `isUnsupportedCursorCompositePrefix`:
//! - A non-unique composite index requires ordering by ALL its fields (cursor errors otherwise).
//! - A unique composite index allows ordering by a prefix of its fields.

use integration_test::TestCluster;
use serde_json::Value;

const USER_COMPOSITE_SCHEMA: &str = "type User { name: String  age: Int  score: Int }";

async fn seed_composite_users(node: &integration_test::DefraClient, users: &[(&str, i32, i32)]) {
    for (name, age, score) in users {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: {age}, score: {score} }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed user");
    }
}

#[tokio::test]
async fn rust_composite_index_full_field_order_succeeds() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    node.index_create("User", &["age", "score"], Some("idx_age_score"), false)
        .expect("create composite index");

    seed_composite_users(
        &node,
        &[("alice", 20, 90), ("bob", 30, 80), ("carol", 40, 70)],
    )
    .await;

    // Order by both fields — satisfies the composite prefix rule for a non-unique index.
    let result: Value = node
        .query(
            r#"{ _cursor { User(first: 2, order: [{age: ASC}, {score: ASC}]) { name age score } } }"#,
        )
        .expect("composite full-field cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users in first page");
    assert_eq!(users[0]["name"], "alice");
    assert_eq!(users[1]["name"], "bob");
}

#[tokio::test]
async fn rust_non_unique_composite_prefix_only_errors() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    // Non-unique composite index on (age, score).
    node.index_create("User", &["age", "score"], Some("idx_age_score"), false)
        .expect("create composite index");

    seed_composite_users(&node, &[("alice", 20, 90)]).await;

    // Order only by `age` — partial coverage of a non-unique composite index.
    // Go's rule: !index.Unique && len(ordering) < len(index.Fields) → no supporting index.
    let err = node
        .query(r#"{ _cursor { User(first: 2, order: [{age: ASC}]) { name } } }"#)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("no supporting index"),
        "expected no-supporting-index error for non-unique composite prefix, got: {err}"
    );
}

#[tokio::test]
async fn rust_unique_composite_prefix_succeeds() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    // Unique composite index on (age, score).
    node.index_create("User", &["age", "score"], Some("idx_age_score"), true)
        .expect("create unique composite index");

    seed_composite_users(&node, &[("alice", 20, 90), ("bob", 30, 80)]).await;

    // Order only by `age` — partial coverage is allowed for UNIQUE composite indexes.
    let result: Value = node
        .query(r#"{ _cursor { User(first: 2, order: [{age: ASC}]) { name } } }"#)
        .expect("unique composite prefix cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users");
}
