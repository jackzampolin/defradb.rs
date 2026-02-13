use std::time::Duration;

use integration_test::{
    generate_identity, poll_until, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

/// Test ACP-protected documents replicate correctly across P2P nodes.
///
/// ACP relationships are node-local: they do NOT replicate with documents.
/// This test verifies:
/// 1. Both public and protected docs replicate to the receiving node
/// 2. On the originating node, ACP enforces access (Alice sees both, anonymous sees only public)
/// 3. On the receiving node, all replicated docs are visible (ACP state is not replicated)
async fn acp_p2p_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let binary_path = node0.binary_path().to_path_buf();

    let alice = generate_identity(&binary_path).expect("failed to generate Alice identity");

    // Wait for P2P listeners
    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    // Get node1 multiaddr
    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Add ACP policy on both nodes as Alice
    let policy0 = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("failed to add ACP policy on node0");
    let policy_id = policy0["PolicyID"]
        .as_str()
        .or_else(|| policy0["policyID"].as_str())
        .expect("missing PolicyID");

    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("failed to add ACP policy on node1");

    // Deploy schema with @policy on both nodes
    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("failed to add schema on node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("failed to add schema on node1");

    // Connect peers and set up sync
    node0.p2p_connect(&[addr1]).unwrap();
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Create a public document (no identity) on node 0
    node0
        .query(r#"mutation { create_User(input: {name: "Public", age: 99}) { _docID } }"#)
        .expect("failed to create public document");

    // Create a protected document (as Alice) on node 0
    node0
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Protected", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("failed to create protected document");

    // Verify ACP enforcement on originating node (node 0)
    let anon_node0 = node0
        .query("query { User { name age } }")
        .expect("anon query on node0 failed");
    let anon_node0_users = anon_node0["User"].as_array().expect("anon_node0 not array");
    assert_eq!(
        anon_node0_users.len(),
        1,
        "anonymous should see only public doc on originating node, got {:?}",
        anon_node0_users
    );
    assert_eq!(anon_node0_users[0]["name"], "Public");

    let alice_node0 = node0
        .query_with_identity("query { User { name age } }", &alice.private_key_hex)
        .expect("Alice query on node0 failed");
    let alice_node0_users = alice_node0["User"]
        .as_array()
        .expect("alice_node0 not array");
    assert_eq!(
        alice_node0_users.len(),
        2,
        "Alice should see both docs on originating node, got {:?}",
        alice_node0_users
    );

    // Wait for both documents to replicate to node 1
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref.query("query { User { _docID name } }").unwrap();
            result["User"]
                .as_array()
                .map(|arr| arr.len() >= 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "docs did not replicate to node1",
    )
    .await;

    // Verify replicated documents on node 1
    let node1_result = node1
        .query("query { User { name age } }")
        .expect("query on node1 failed");
    let node1_users = node1_result["User"]
        .as_array()
        .expect("node1 result not array");
    assert_eq!(
        node1_users.len(),
        2,
        "node1 should have both replicated docs, got {:?}",
        node1_users
    );
    let names: Vec<&str> = node1_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(names.contains(&"Public"));
    assert!(names.contains(&"Protected"));
}

#[tokio::test]
#[ignore]
async fn rust_rust_acp_p2p() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_go_acp_p2p() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_rust_acp_p2p() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_test(cluster).await;
}
