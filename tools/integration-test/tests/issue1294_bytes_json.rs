//! #1294 — `NormalValue::Bytes` must render as one JSON shape on create and query.
//!
//! Forces Blob as `NormalValue::Bytes` via harness env `DEFRA_TEST_BLOB_AS_BYTES=1`
//! and asserts create+query both return lowercase hex.

use integration_test::TestCluster;

const HOOK_ENV: &str = "DEFRA_TEST_BLOB_AS_BYTES";

#[tokio::test]
async fn bytes_create_and_query_return_lowercase_hex() {
    // Isolated binary: process-global env only affects this test process.
    // Child `defra` inherits the env for the harness hook.
    std::env::set_var(HOOK_ENV, "1");

    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);

    client
        .schema_add("type BlobUser { data: Blob }")
        .expect("schema add");

    let created = client
        .query(r#"mutation { add_BlobUser(input: {data: "00FF"}) { _docID data } }"#)
        .expect("create mutation");

    let create_data = &created["add_BlobUser"][0]["data"];
    assert_eq!(
        create_data, "00ff",
        "create mutation must JSON-encode NormalValue::Bytes as lowercase hex \
         (not a number array and not base64); got {create_data:?}"
    );

    let queried = client
        .query("query { BlobUser { _docID data } }")
        .expect("query");

    let query_data = &queried["BlobUser"][0]["data"];
    assert_eq!(
        query_data, "00ff",
        "query must JSON-encode NormalValue::Bytes as lowercase hex; got {query_data:?}"
    );

    assert_eq!(
        create_data, query_data,
        "create and query must agree on Bytes JSON encoding"
    );

    std::env::remove_var(HOOK_ENV);
}
