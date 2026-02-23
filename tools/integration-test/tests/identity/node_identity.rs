use integration_test::{for_each_runtime, TestCluster};

async fn node_identity_test(cluster: TestCluster) {
    let node = cluster.client(0);

    // Query node identity
    let identity = node.node_identity().expect("node-identity");

    // Verify we got a non-empty JSON response
    let id_str = serde_json::to_string(&identity).unwrap();
    assert!(
        !id_str.is_empty(),
        "node-identity should return non-empty response"
    );

    // The response should contain identity information (peer ID or public key)
    assert!(
        identity.is_object() || identity.is_string(),
        "node-identity should return an object or string, got: {}",
        id_str
    );
}

for_each_runtime!(node_identity, node_identity_test);
