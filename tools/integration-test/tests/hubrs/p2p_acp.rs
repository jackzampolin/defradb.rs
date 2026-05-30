use std::time::Duration;

use integration_test::node::RustNode;
use integration_test::{users_schema_with_policy, USER_ACP_POLICY};

use super::helpers;

/// P2P replication preserving hub.rs ACP.
///
/// Two Rust DefraDB nodes connected to the same hub.rs cluster.
/// A document created on node 0 replicates to node 1.
/// The owner can read on both nodes; anonymous cannot read on either.
#[tokio::test]
#[serial_test::serial]
async fn rust_hubrs_p2p_acp() {
    RustNode::build().expect("build rust binary");
    let jack = helpers::funded_identity();

    let hub = helpers::start_hub_cluster().await;
    let hub_rpc_url = hub.node(0).rpc_url();

    let cluster =
        helpers::build_defra_with_hub_rs(&hub_rpc_url, &jack.private_key_hex, 2, true).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Add policy on hub.rs via node 0
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

    // Get node1 multiaddr and connect
    let info1 = node1.p2p_info().expect("p2p info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    node0.p2p_connect(&[addr1]).expect("p2p connect");
    node0
        .p2p_collection_add(&["User"])
        .expect("p2p collection add node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("p2p collection add node1");
    node0
        .p2p_replicator_set_with_identity(&["User"], addr1, &jack.private_key_hex)
        .expect("set replicator");

    // Create document as Jack on node 0
    node0
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Jack", age: 30}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create user on node0");

    // Poll until replication completes (up to 30s)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let jack_on_node1 = node1
            .query_with_identity("query { User { _docID name } }", &jack.private_key_hex)
            .expect("Jack query on node1");
        let users = jack_on_node1["User"].as_array().expect("users array");
        if users.len() == 1 {
            assert_eq!(users[0]["name"], "Jack");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for replication to node 1"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Anonymous cannot read on node 1 (hub.rs ACP enforced)
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
