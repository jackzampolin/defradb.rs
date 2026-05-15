//! Error paths: missing index, invalid token, conflicting args, multiple collections.

use super::common::{setup_indexed_cluster, setup_unindexed_cluster, USER_SCHEMA};

#[tokio::test]
async fn rust_no_supporting_index_errors() {
    // With no index on `age`, ordering by age in a cursor query must fail.
    let (_cluster, node) = setup_unindexed_cluster().await;
    node.query(r#"mutation { add_User(input: { name: "a", age: 1 }) { _docID } }"#)
        .expect("seed user");

    let err = node
        .query(r#"{ _cursor { User(first: 1, order: [{age: ASC}]) { name } } }"#)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("no supporting index"),
        "expected no-supporting-index error, got: {err}"
    );
}

#[tokio::test]
async fn rust_invalid_cursor_token_errors() {
    let (_cluster, node) = setup_indexed_cluster().await;
    node.query(r#"mutation { add_User(input: { name: "a", age: 1 }) { _docID } }"#)
        .expect("seed user");

    let err = node
        .query(r#"{ _cursor { User(first: 1, after: "!!!not-base64!!!", order: [{age: ASC}]) { name } } }"#)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("invalid cursor"),
        "expected invalid cursor error, got: {err}"
    );
}

#[tokio::test]
async fn rust_forward_backward_conflict_errors() {
    let (_cluster, node) = setup_indexed_cluster().await;

    let err = node
        .query(r#"{ _cursor { User(first: 5, last: 3, order: [{age: ASC}]) { name } } }"#)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("forward parameters"),
        "expected forward/backward conflict error, got: {err}"
    );
}

#[tokio::test]
async fn rust_multiple_collections_in_cursor_errors() {
    use integration_test::TestCluster;

    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_SCHEMA).expect("add User schema");
    node.schema_add("type Book { title: String }")
        .expect("add Book schema");
    node.index_create("User", &["age"], Some("idx_age"), false)
        .expect("create age index");

    let err = node
        .query(
            r#"{ _cursor {
            User(first: 1, order: [{age: ASC}]) { name }
            Book(first: 1) { title }
        } }"#,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("cannot contain multiple"),
        "expected multiple-queries error, got: {err}"
    );
}
