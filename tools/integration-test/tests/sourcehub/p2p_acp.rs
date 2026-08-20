use std::time::Duration;

use integration_test::node::{DefraNode, RustNode};
use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// P2P replication preserving Source Hub ACP.
///
/// Two Rust DefraDB nodes connected to the same Source Hub.
/// A document created on node 0 replicates to node 1.
/// The owner can read on both nodes; anonymous cannot read on either.
///
/// The cluster is started with Jack's identity so SourceHub transactions work.
#[tokio::test]
#[serial_test::serial]
async fn rust_sourcehub_p2p_acp() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build_with_features(&["sourcehub"]).expect("build sourcehub-enabled rust binary");
    let jack = generate_identity(&binary).expect("Jack identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .skip_build()
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .with_p2p()
        .build()
        .await
        .expect("failed to build source hub p2p cluster");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Add policy on Source Hub via node 0
    let policy_result = node0
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID");

    // Deploy schema on both nodes (node1 also needs the policy stored locally)
    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("schema on node0");

    // Node 1 needs the policy in its local store for ACP evaluation.
    // We can't create a second on-chain policy (same content = same ID),
    // but adding it via the API caches it locally on node1.
    node1
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy on node1");
    tokio::time::sleep(Duration::from_secs(2)).await;

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
