//! Regression: secondary indexes are rebuilt when a lens migration is
//! registered, so indexed queries reflect lens-transformed field values.
//!
//! Originally written to probe whether Go DefraDB issue #4736 reproduces in
//! Rust (defradb.rs#979). It does not — the *incremental* lens mode that #4736
//! requires is not implemented in Rust, and the index/lens interaction here is
//! kept consistent by `maybe_reindex_after_migration` in
//! `crates/db/src/migration/reindex.rs`. The tests pin that behavior across
//! single-field, composite, and multi-document scenarios.

use integration_test::{DefraClient, TestCluster};
use serde_json::Value;

const SCHEMA: &str = "type Users { name: String  verified: Boolean }";
const PATCH_V1_TO_V2: &str =
    r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"placeholder","Kind":"String"}}]"#;

async fn build_cluster() -> TestCluster {
    TestCluster::builder()
        .rust_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap()
}

fn version_id(client: &DefraClient) -> String {
    client
        .collection_describe_version("Users")
        .expect("describe Users")["VersionID"]
        .as_str()
        .expect("VersionID")
        .to_string()
}

fn set_default_config(dst: &str, value: Value) -> String {
    let lens = integration_test::wasm_lens::wasm_lens_defra();
    lens.build().expect("build set_default lens");
    serde_json::json!({
        "Lenses": [{
            "Path": lens.module_path(),
            "Arguments": {"dst": dst, "value": value}
        }]
    })
    .to_string()
}

#[tokio::test]
async fn single_field_index_rebuilt_after_lens_migration() {
    let cluster = build_cluster().await;
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema add");
    let v1 = version_id(&node);

    node.query(r#"mutation { add_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create John");

    node.index_create("Users", &["verified"], Some("idx_verified"), false)
        .expect("create idx_verified");

    node.collection_patch(PATCH_V1_TO_V2).expect("patch to v2");
    let v2 = version_id(&node);

    let cfg = set_default_config("verified", Value::Bool(true));
    node.lens_set(&v1, &v2, &cfg).expect("lens_set v1->v2");

    let result = node
        .query(r#"query { Users(filter: {verified: {_eq: true}}) { name verified } }"#)
        .expect("indexed query");
    let arr = result["Users"].as_array().expect("Users array");
    assert_eq!(
        arr.len(),
        1,
        "indexed filter on lens-set field returned {} rows; expected 1",
        arr.len()
    );
    assert_eq!(arr[0]["name"].as_str(), Some("John"));
    assert_eq!(arr[0]["verified"], Value::Bool(true));
}

#[tokio::test]
async fn multi_doc_collection_fully_reindexed_after_lens_migration() {
    let cluster = build_cluster().await;
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema add");
    let v1 = version_id(&node);

    for name in ["Alice", "Bob", "Charlie"] {
        node.query(&format!(
            r#"mutation {{ add_Users(input: {{name: "{}"}}) {{ _docID }} }}"#,
            name
        ))
        .expect("create doc");
    }

    node.index_create("Users", &["verified"], Some("idx_verified"), false)
        .expect("create idx_verified");

    node.collection_patch(PATCH_V1_TO_V2).expect("patch to v2");
    let v2 = version_id(&node);

    let cfg = set_default_config("verified", Value::Bool(true));
    node.lens_set(&v1, &v2, &cfg).expect("lens_set v1->v2");

    let result = node
        .query(r#"query { Users(filter: {verified: {_eq: true}}) { name } }"#)
        .expect("indexed query");
    let arr = result["Users"].as_array().expect("Users array");
    assert_eq!(
        arr.len(),
        3,
        "expected 3 reindexed docs, got {} — per-doc reindex loop dropped entries",
        arr.len()
    );
    let mut names: Vec<String> = arr
        .iter()
        .map(|d| d["name"].as_str().expect("name").to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

#[tokio::test]
async fn composite_index_rebuilt_after_lens_migration() {
    let cluster = build_cluster().await;
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema add");
    let v1 = version_id(&node);

    node.query(r#"mutation { add_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create John");

    node.index_create(
        "Users",
        &["name", "verified"],
        Some("idx_name_verified"),
        false,
    )
    .expect("create composite index");

    node.collection_patch(PATCH_V1_TO_V2).expect("patch to v2");
    let v2 = version_id(&node);

    let cfg = set_default_config("verified", Value::Bool(true));
    node.lens_set(&v1, &v2, &cfg).expect("lens_set v1->v2");

    let result = node
        .query(
            r#"query { Users(filter: {name: {_eq: "John"}, verified: {_eq: true}}) { name verified } }"#,
        )
        .expect("indexed query");
    let arr = result["Users"].as_array().expect("Users array");
    assert_eq!(
        arr.len(),
        1,
        "composite-index query returned {} rows; expected 1",
        arr.len()
    );
    assert_eq!(arr[0]["name"].as_str(), Some("John"));
    assert_eq!(arr[0]["verified"], Value::Bool(true));
}
