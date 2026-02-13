use std::time::Duration;

use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// P2P replication preserving Source Hub ACP.
///
/// Two Rust DefraDB nodes connected to the same Source Hub.
/// A document created on node 0 replicates to node 1.
/// The owner can read on both nodes; anonymous cannot read on either.
#[tokio::test]
#[ignore]
async fn rust_sourcehub_p2p_acp() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_source_hub()
        .with_p2p()
        .build()
        .await
        .expect("failed to build source hub p2p cluster");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let binary = node0.binary_path().to_path_buf();

    let jack = generate_identity(&binary).expect("Jack identity");

    // Add policy on Source Hub via node 0
    let policy_result = node0
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID");

    // Deploy schema on both nodes
    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("schema on node0");
    node1
        .schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("schema on node1");

    // Connect nodes for P2P
    let info0 = node0.p2p_info().expect("p2p info node0");
    let peer_addrs: Vec<String> = info0["addresses"]
        .as_array()
        .expect("p2p addresses")
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    for addr in &peer_addrs {
        let _ = node1.p2p_connect(&[addr]);
    }

    // Set up replication
    node0
        .p2p_collection_add(&["User"])
        .expect("p2p collection add node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("p2p collection add node1");

    // Wait for peers to connect
    cluster
        .wait_for_log(0, "peer_connected", Duration::from_secs(15))
        .await
        .expect("peer connection");

    // Create document as Jack on node 0
    node0
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Jack", age: 30}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create user on node0");

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Jack can read on node 1 (same DID, same policy on Source Hub)
    let jack_on_node1 = node1
        .query_with_identity("query { User { _docID name } }", &jack.private_key_hex)
        .expect("Jack query on node1");
    let users = jack_on_node1["User"].as_array().expect("users array");
    assert_eq!(users.len(), 1, "Jack should see replicated doc on node 1");
    assert_eq!(users[0]["name"], "Jack");

    // Anonymous cannot read on node 1 (Source Hub ACP enforced)
    let anon_on_node1 = node1
        .query("query { User { _docID name } }")
        .expect("anon query on node1");
    let anon_users = anon_on_node1["User"].as_array().expect("anon users array");
    assert_eq!(
        anon_users.len(),
        0,
        "anonymous should NOT see docs on node 1"
    );
}
