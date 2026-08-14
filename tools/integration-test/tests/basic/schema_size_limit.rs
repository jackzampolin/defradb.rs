//! `--max-schema-size` must reject an oversized schema on the wire (#1296).
//!
//! The in-process tests in `crates/http` prove the router applies the cap. This
//! proves the whole chain: the flag reaches a real node, and a real HTTP client
//! gets a 413 back. It is also the first consumer of the harness's
//! `with_extra_rust_args`, which exists precisely so a flag with no typed
//! builder method can still be put under test (#1422).
//!
//! Run with:
//!   cargo test -p integration-test --test basic -- schema_size_limit

use integration_test::TestCluster;

/// Small enough that an ordinary schema exceeds it, large enough that a
/// minimal one does not.
const MAX_SCHEMA_SIZE: usize = 256;

fn oversized_schema() -> String {
    let fields: String = (0..64).map(|i| format!("f{i}: String ")).collect();
    let sdl = format!("type Big {{ {fields} }}");
    assert!(
        sdl.len() > MAX_SCHEMA_SIZE,
        "test fixture must exceed the configured cap"
    );
    sdl
}

async fn post_schema(url: &str, sdl: String) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("{url}/api/v1/schema"))
        .header("content-type", "application/json")
        .body(sdl)
        .send()
        .await
        .expect("schema request")
        .status()
}

async fn node_with_schema_cap() -> TestCluster {
    TestCluster::builder()
        .rust_nodes(1)
        .with_extra_rust_args(["--max-schema-size", &MAX_SCHEMA_SIZE.to_string()])
        .build()
        .await
        .expect("cluster starts")
}

#[tokio::test]
async fn rust_oversized_schema_is_rejected_on_the_wire() {
    let cluster = node_with_schema_cap().await;

    assert_eq!(
        post_schema(cluster.api_url(0), oversized_schema()).await,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "--max-schema-size was set but an oversized schema was not rejected"
    );
}

/// The other half of the pair. Without it the test above would pass just as
/// well against a node that rejected every schema, or one that never started.
#[tokio::test]
async fn rust_schema_within_the_limit_is_not_rejected() {
    let cluster = node_with_schema_cap().await;
    let small = "type S { n: Int }".to_string();
    assert!(small.len() < MAX_SCHEMA_SIZE);

    assert_ne!(
        post_schema(cluster.api_url(0), small).await,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "a schema under the cap must not be rejected as too large"
    );
}
