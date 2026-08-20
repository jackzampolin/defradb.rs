//! Go's policy wire path must answer the same on both runtimes.
//!
//! Go registers AddDACPolicy at `/acp/document/policy` and its client posts
//! there. Rust served only `/acp/policy`, so a Go-compatible client's policy
//! registration 404'd. The golden case is the unauthenticated request: both
//! runtimes reject it for a missing creator, at the same path, with the same
//! status.

use integration_test::{for_each_runtime, TestCluster, USER_ACP_POLICY};

async fn go_policy_path_test(cluster: TestCluster) {
    let api_url = cluster.api_url(0);

    let response = reqwest::Client::new()
        .post(format!("{api_url}/api/v1/acp/document/policy"))
        .body(USER_ACP_POLICY)
        .send()
        .await
        .expect("send policy request");

    let status = response.status().as_u16();
    let body = response.text().await.expect("read body");

    assert_ne!(
        status, 404,
        "Go's policy path must be registered: body={body}"
    );
    assert_eq!(
        status, 400,
        "an unauthenticated policy registration is rejected: body={body}"
    );
    assert!(
        body.contains("creator"),
        "the rejection must name the missing creator: body={body}"
    );
}

for_each_runtime!(go_policy_path, go_policy_path_test, .with_acp_local());
