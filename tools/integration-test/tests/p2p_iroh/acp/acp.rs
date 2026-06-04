//! ACP-protected document replication over iroh transport.
//!
//! Local (DAC) ACP is node-local: a document is gated only on the node where its
//! owner was registered (the creating node). A replicated copy is NOT gated on the
//! peer — the unregistered doc is public there (matches Go). Verifies:
//! - Both public and protected docs replicate
//! - On the originating node: owner sees protected docs, anonymous sees only public
//! - On the receiving node: the replicated protected doc is public to everyone
//! - Relationship replication is covered by the DAC-specific ACP P2P tests
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_acp -- --ignored

use std::time::Duration;

use integration_test::{
    generate_identity, poll_until, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};
use serial_test::serial;

/// ACP-protected documents replicate correctly over iroh transport.
#[tokio::test]
#[serial]
async fn iroh_acp_replication() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_acp_local()
        .build()
        .await
        .unwrap();

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let binary_path = node0.binary_path().to_path_buf();

    let alice = generate_identity(&binary_path).expect("failed to generate Alice identity");

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Add ACP policy on both nodes as Alice
    let policy0 = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy on node0");
    let policy_id = policy0["PolicyID"]
        .as_str()
        .or_else(|| policy0["policyID"].as_str())
        .expect("missing PolicyID");

    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy on node1");

    // Deploy schema with @policy on both nodes
    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema on node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema on node1");

    // Connect and set up replication
    node0.p2p_connect(&[addr1]).unwrap();
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Create a public document (no identity) on node0
    node0
        .query(r#"mutation { add_User(input: {name: "Public", age: 99}) { _docID } }"#)
        .expect("create public document");

    // Create a protected document (as Alice) on node0
    node0
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Protected", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create protected document");

    // Verify ACP enforcement on originating node
    let anon_node0 = node0
        .query("query { User { name age } }")
        .expect("anon query on node0");
    let anon_users = anon_node0["User"].as_array().expect("not array");
    assert_eq!(
        anon_users.len(),
        1,
        "anonymous should see only public doc on originating node, got {:?}",
        anon_users
    );
    assert_eq!(anon_users[0]["name"], "Public");
    assert_eq!(anon_users[0]["age"], 99, "public doc age mismatch");

    let alice_node0 = node0
        .query_with_identity("query { User { name age } }", &alice.private_key_hex)
        .expect("Alice query on node0");
    let alice_users = alice_node0["User"].as_array().expect("not array");
    assert_eq!(
        alice_users.len(),
        2,
        "Alice should see both docs on originating node, got {:?}",
        alice_users
    );
    let alice_names: Vec<&str> = alice_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(
        alice_names.contains(&"Public"),
        "Alice should see Public doc on node0"
    );
    assert!(
        alice_names.contains(&"Protected"),
        "Alice should see Protected doc on node0"
    );

    // Wait for both documents to replicate to node1.
    // Poll with Alice's identity: she sees her protected doc + public doc = 2.
    // (Anonymous can't see protected docs — ACP is registered during merge.)
    let node1_ref = &node1;
    let alice_key_clone = alice.private_key_hex.clone();
    poll_until(
        || {
            let result = node1_ref
                .query_with_identity("query { User { _docID name } }", &alice_key_clone)
                .unwrap_or_default();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "docs did not replicate to node1",
    )
    .await;

    // Verify: Alice sees both docs on node1
    let alice_node1 = node1
        .query_with_identity("query { User { name age } }", &alice.private_key_hex)
        .expect("Alice query on node1");
    let alice_node1_users = alice_node1["User"].as_array().expect("not array");
    assert_eq!(
        alice_node1_users.len(),
        2,
        "Alice should see 2 docs on node1, got {:?}",
        alice_node1_users
    );
    let names: Vec<&str> = alice_node1_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(
        names.contains(&"Public"),
        "node1 should have Public doc, got {:?}",
        names
    );
    assert!(
        names.contains(&"Protected"),
        "node1 should have Protected doc, got {:?}",
        names
    );

    // Verify field values replicated correctly
    for user in alice_node1_users {
        match user["name"].as_str().unwrap() {
            "Public" => assert_eq!(user["age"], 99, "Public doc age mismatch"),
            "Protected" => assert_eq!(user["age"], 42, "Protected doc age mismatch"),
            other => panic!("unexpected user name on node1: {}", other),
        }
    }

    // Local ACP is node-local; the replicated protected doc is public on the peer
    // (matches Go). The owner is registered only on node0, so on node1 the
    // unregistered protected doc is public and anonymous sees BOTH docs.
    let anon_node1 = node1
        .query("query { User { name } }")
        .expect("anon query on node1");
    let anon_node1_users = anon_node1["User"].as_array().expect("not array");
    assert_eq!(
        anon_node1_users.len(),
        2,
        "anonymous sees both docs on the peer (Local ACP is node-local; protected doc is public there), got {:?}",
        anon_node1_users
    );
    let anon_node1_names: Vec<&str> = anon_node1_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(anon_node1_names.contains(&"Public"));
    assert!(anon_node1_names.contains(&"Protected"));
}

/// Multiple identities with ACP: owner vs non-owner access over iroh.
#[tokio::test]
#[serial]
async fn iroh_acp_multi_identity() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_acp_local()
        .build()
        .await
        .unwrap();

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let binary_path = node0.binary_path().to_path_buf();

    let alice = generate_identity(&binary_path).expect("Alice identity");
    let bob = generate_identity(&binary_path).expect("Bob identity");

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Set up ACP policy on node0
    let policy0 = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy0["PolicyID"]
        .as_str()
        .or_else(|| policy0["policyID"].as_str())
        .expect("missing PolicyID");

    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy on node1");

    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema node1");

    // Set up replication
    node0.p2p_connect(&[addr1]).unwrap();
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Alice creates a protected doc
    node0
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Alice Secret", age: 30}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create Alice's doc");

    // Bob creates a protected doc
    node0
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Bob Secret", age: 25}) { _docID } }"#,
            &bob.private_key_hex,
        )
        .expect("create Bob's doc");

    // Create a public doc
    node0
        .query(r#"mutation { add_User(input: {name: "Public", age: 99}) { _docID } }"#)
        .expect("create public doc");

    // On node0: Alice sees her doc + public (2), Bob sees his doc + public (2), anon sees 1
    let alice_result = node0
        .query_with_identity("query { User { name } }", &alice.private_key_hex)
        .expect("Alice query");
    let alice_names: Vec<&str> = alice_result["User"]
        .as_array()
        .expect("Alice result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(alice_names.len(), 2, "Alice sees her doc + public");
    assert!(
        alice_names.contains(&"Alice Secret"),
        "Alice should see her own doc, got {:?}",
        alice_names
    );
    assert!(
        alice_names.contains(&"Public"),
        "Alice should see public doc, got {:?}",
        alice_names
    );
    assert!(
        !alice_names.contains(&"Bob Secret"),
        "Alice should NOT see Bob's doc, got {:?}",
        alice_names
    );

    let bob_result = node0
        .query_with_identity("query { User { name } }", &bob.private_key_hex)
        .expect("Bob query");
    let bob_names: Vec<&str> = bob_result["User"]
        .as_array()
        .expect("Bob result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(bob_names.len(), 2, "Bob sees his doc + public");
    assert!(
        bob_names.contains(&"Bob Secret"),
        "Bob should see his own doc, got {:?}",
        bob_names
    );
    assert!(
        bob_names.contains(&"Public"),
        "Bob should see public doc, got {:?}",
        bob_names
    );
    assert!(
        !bob_names.contains(&"Alice Secret"),
        "Bob should NOT see Alice's doc, got {:?}",
        bob_names
    );

    let anon_result = node0.query("query { User { name } }").expect("anon query");
    let anon_names: Vec<&str> = anon_result["User"]
        .as_array()
        .expect("anon result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(anon_names.len(), 1, "anonymous sees only public");
    assert_eq!(
        anon_names[0], "Public",
        "anonymous should only see public doc"
    );

    // Wait for all three docs to replicate to node1. Local ACP is node-local: the
    // protected docs are NOT registered on the peer, so they are public there and
    // any caller sees all three once replicated. Poll anonymously for len == 3.
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID } }")
                .unwrap_or_default();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 3)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "docs did not replicate to node1",
    )
    .await;

    // On node1 (the peer): Local ACP is node-local; the replicated protected docs are
    // public there (matches Go). Alice, Bob, and anonymous all see ALL THREE docs.
    let expect_all_three = |label: &str, value: &serde_json::Value| {
        let names: Vec<&str> = value["User"]
            .as_array()
            .unwrap_or_else(|| panic!("{label} node1 result not array"))
            .iter()
            .filter_map(|u| u["name"].as_str())
            .collect();
        assert_eq!(
            names.len(),
            3,
            "{label} sees all three docs on the peer (Local ACP is node-local), got {:?}",
            names
        );
        assert!(names.contains(&"Alice Secret"));
        assert!(names.contains(&"Bob Secret"));
        assert!(names.contains(&"Public"));
    };

    let node1_alice = node1
        .query_with_identity("query { User { name } }", &alice.private_key_hex)
        .expect("Alice query on node1");
    expect_all_three("Alice", &node1_alice);

    let node1_bob = node1
        .query_with_identity("query { User { name } }", &bob.private_key_hex)
        .expect("Bob query on node1");
    expect_all_three("Bob", &node1_bob);

    let node1_anon = node1
        .query("query { User { name } }")
        .expect("anon query on node1");
    expect_all_three("anonymous", &node1_anon);
}
