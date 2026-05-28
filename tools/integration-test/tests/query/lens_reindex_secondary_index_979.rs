//! Regression: secondary indexes are rebuilt when a lens migration is
//! registered, so indexed queries reflect lens-transformed field values.
//!
//! Originally written to probe whether Go DefraDB issue #4736 reproduces in
//! Rust (defradb.rs#979). It does not — the *incremental* lens mode that #4736
//! requires is not implemented in Rust, and the index/lens interaction here is
//! kept consistent by `maybe_reindex_after_migration` in
//! `crates/db/src/migration/reindex.rs`. The test pins that behavior so a
//! future change cannot silently regress it.

use integration_test::TestCluster;
use serde_json::Value;

#[tokio::test]
async fn lens_migration_rebuilds_secondary_index() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_development()
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);

    node.schema_add("type Users { name: String  verified: Boolean }")
        .expect("schema add v1");

    let v1 = node
        .collection_describe_version("Users")
        .expect("describe v1")["VersionID"]
        .as_str()
        .expect("v1 id")
        .to_string();

    node.query(r#"mutation { add_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create John at v1");

    node.index_create("Users", &["verified"], Some("idx_verified"), false)
        .expect("create idx_verified");

    node.collection_patch(
        r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"placeholder","Kind":11}}]"#,
    )
    .expect("patch to v2");

    let v2 = node
        .collection_describe_version("Users")
        .expect("describe v2")["VersionID"]
        .as_str()
        .expect("v2 id")
        .to_string();

    let lens = integration_test::wasm_lens::wasm_lens_defra();
    lens.build().expect("build set_default lens");
    let lens_config = serde_json::json!({
        "Lenses": [{
            "Path": lens.module_path(),
            "Arguments": {"dst": "verified", "value": true}
        }]
    })
    .to_string();
    node.lens_set(&v1, &v2, &lens_config)
        .expect("lens_set v1->v2");

    let scan = node
        .query("query { Users { name verified } }")
        .expect("scan query");
    let scan_arr = scan["Users"].as_array().expect("scan users");
    assert_eq!(scan_arr.len(), 1);
    assert_eq!(scan_arr[0]["name"].as_str(), Some("John"));
    assert_eq!(scan_arr[0]["verified"], Value::Bool(true));

    let indexed = node
        .query(r#"query { Users(filter: {verified: {_eq: true}}) { name verified } }"#)
        .expect("indexed query");
    let indexed_arr = indexed["Users"].as_array().expect("indexed users");

    assert_eq!(
        indexed_arr.len(),
        1,
        "indexed filter on lens-set field must match full scan: \
         scan {} row(s), indexed {} row(s). If this fails, the reindex hook \
         after `set_migration` has regressed.",
        scan_arr.len(),
        indexed_arr.len()
    );
    assert_eq!(indexed_arr[0]["name"].as_str(), Some("John"));
    assert_eq!(indexed_arr[0]["verified"], Value::Bool(true));
}
