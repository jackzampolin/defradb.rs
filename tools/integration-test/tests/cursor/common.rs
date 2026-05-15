//! Shared schema and seed helpers for cursor tests.

use integration_test::TestCluster;

/// A simple schema with no inline @index directive.
/// Use `node.index_create()` after `schema_add()` to attach indexes programmatically.
pub(super) const USER_SCHEMA: &str = "type User { name: String  age: Int }";

/// Seed users into a freshly-started node.
///
/// Adds the given schema (with any programmatic indexes already set up by the
/// caller before calling this function), then inserts the provided name/age pairs.
pub(super) async fn seed_users(node: &integration_test::DefraClient, users: &[(&str, i32)]) {
    for (name, age) in users {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: {age} }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed user");
    }
}

/// Build a fresh single-Rust-node cluster with the User schema deployed and an
/// index on `age`, returning the cluster and client together.
pub(super) async fn setup_indexed_cluster() -> (TestCluster, integration_test::DefraClient) {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_SCHEMA).expect("add User schema");
    node.index_create("User", &["age"], Some("idx_age"), false)
        .expect("create age index");
    (cluster, node)
}

/// Build a fresh single-Rust-node cluster with the User schema deployed and NO
/// index on `age`.
pub(super) async fn setup_unindexed_cluster() -> (TestCluster, integration_test::DefraClient) {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_SCHEMA).expect("add User schema");
    (cluster, node)
}
