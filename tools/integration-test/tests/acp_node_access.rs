use integration_test::{generate_identity, TestCluster};

async fn acp_node_access_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // Check initial node ACP status
    let status1 = node.acp_node_status().expect("acp node status");
    let status_str = serde_json::to_string(&status1).unwrap();
    assert!(
        !status_str.is_empty(),
        "acp node status should return non-empty response"
    );

    // Generate 2 identities
    let admin = generate_identity(&binary).expect("admin identity");
    let user = generate_identity(&binary).expect("user identity");

    // Add admin relationship
    node.acp_node_relationship_add("admin", &admin.did)
        .expect("add admin relationship");

    // Add user relationship
    node.acp_node_relationship_add("admin", &user.did)
        .expect("add user relationship");

    // Delete user relationship
    node.acp_node_relationship_delete("admin", &user.did)
        .expect("delete user relationship");

    // Delete admin relationship
    node.acp_node_relationship_delete("admin", &admin.did)
        .expect("delete admin relationship");

    // Disable ACP node
    node.acp_node_disable().expect("acp node disable");

    // Check status after disable
    let status2 = node
        .acp_node_status()
        .expect("acp node status after disable");
    let status2_str = serde_json::to_string(&status2).unwrap();
    assert!(
        !status2_str.is_empty(),
        "status after disable should return response"
    );

    // Re-enable ACP node
    node.acp_node_reenable().expect("acp node re-enable");

    // Check status after re-enable
    let status3 = node
        .acp_node_status()
        .expect("acp node status after re-enable");
    let status3_str = serde_json::to_string(&status3).unwrap();
    assert!(
        !status3_str.is_empty(),
        "status after re-enable should return response"
    );
}

#[tokio::test]
#[ignore]
async fn rust_acp_node_access() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_node_access_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_acp_node_access() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_node_access_test(cluster).await;
}
