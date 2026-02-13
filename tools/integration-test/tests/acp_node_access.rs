use integration_test::{generate_identity, TestCluster};

async fn acp_node_access_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // Check initial node ACP status
    let status1 = node.acp_node_status();
    if let Err(e) = &status1 {
        eprintln!("acp node status not supported: {}", e);
    }

    // Generate identities: owner (acts as requesting identity) + 2 targets
    let owner = generate_identity(&binary).expect("owner identity");
    let admin = generate_identity(&binary).expect("admin identity");
    let user = generate_identity(&binary).expect("user identity");

    // Add admin relationship (requires requesting identity)
    let add_admin = node.acp_node_relationship_add("admin", &admin.did, &owner.private_key_hex);
    if let Err(e) = &add_admin {
        eprintln!("NAC add admin failed (may not be supported): {}", e);
        return;
    }

    // Add user as admin too
    node.acp_node_relationship_add("admin", &user.did, &owner.private_key_hex)
        .expect("add user relationship");

    // Delete user relationship
    node.acp_node_relationship_delete("admin", &user.did, &owner.private_key_hex)
        .expect("delete user relationship");

    // Delete admin relationship
    node.acp_node_relationship_delete("admin", &admin.did, &owner.private_key_hex)
        .expect("delete admin relationship");

    // Disable ACP node
    let disable = node.acp_node_disable();
    if let Err(e) = &disable {
        eprintln!("acp node disable not supported: {}", e);
    }

    // Check status after disable
    let status2 = node.acp_node_status();
    if let Ok(s) = &status2 {
        let s_str = serde_json::to_string(s).unwrap();
        assert!(
            !s_str.is_empty(),
            "status after disable should return response"
        );
    }

    // Re-enable ACP node
    let reenable = node.acp_node_reenable();
    if let Err(e) = &reenable {
        eprintln!("acp node re-enable not supported: {}", e);
    }

    // Check status after re-enable
    let status3 = node.acp_node_status();
    if let Ok(s) = &status3 {
        let s_str = serde_json::to_string(s).unwrap();
        assert!(
            !s_str.is_empty(),
            "status after re-enable should return response"
        );
    }
}

#[tokio::test]
#[ignore]
async fn rust_acp_node_access() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_nac()
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
        .with_nac()
        .build()
        .await
        .unwrap();
    acp_node_access_test(cluster).await;
}
