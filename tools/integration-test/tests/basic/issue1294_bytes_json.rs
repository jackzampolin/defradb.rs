//! #1294 — `NormalValue::Bytes` must render as one JSON shape on create and query.
//!
//! Production GraphQL Blob fields are stored as hex **strings**, so the three
//! `normal_value_to_json` Bytes arms are not hit on the happy path (see L2 spike
//! in agent-ops worklog). This test enables the harness env
//! `DEFRA_TEST_BLOB_AS_BYTES=1` so Blob inputs are hex-decoded into
//! `NormalValue::Bytes`, then asserts create mutation and query both return
//! lowercase hex (`"00ff"` for input `"00FF"`).
//!
//! ## Long-term suite widening (not in this PR)
//!
//! - Natural product paths that materialize `Bytes` without a test hook (CBOR
//!   loads, lens transforms, P2P payload maps via `Document::to_map`).
//! - Commit `delta` / `signature.value` remain separate contracts; do not fold
//!   them into the Blob/`Bytes` assertion here.
//! - Go cross-runtime: Blob hex string already matches; re-check if storage ever
//!   switches Blob from String to Bytes without a GraphQL scalar change.

use integration_test::TestCluster;

const HOOK_ENV: &str = "DEFRA_TEST_BLOB_AS_BYTES";

#[tokio::test]
async fn bytes_create_and_query_return_lowercase_hex() {
    // Safety: serial within this process; integration tests run as separate
    // processes per binary. Child `defra` inherits the env for the harness hook.
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
        create_data,
        "00ff",
        "create mutation must JSON-encode NormalValue::Bytes as lowercase hex \
         (not a number array and not base64); got {create_data:?}"
    );

    let queried = client
        .query("query { BlobUser { _docID data } }")
        .expect("query");

    let query_data = &queried["BlobUser"][0]["data"];
    assert_eq!(
        query_data,
        "00ff",
        "query must JSON-encode NormalValue::Bytes as lowercase hex; got {query_data:?}"
    );

    assert_eq!(
        create_data, query_data,
        "create and query must agree on Bytes JSON encoding"
    );

    std::env::remove_var(HOOK_ENV);
}
