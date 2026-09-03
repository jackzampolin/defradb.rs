use std::time::Duration;

use integration_test::node::{DefraNode, RustNode};
use integration_test::{
    for_each_runtime, generate_identity, poll_until, users_schema_with_policy, TestCluster,
    USER_ACP_POLICY,
};

async fn encrypted_acp_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let jack = generate_identity(&binary).expect("jack identity");
    let watchdog = generate_identity(&binary).expect("watchdog identity");
    let rogue = generate_identity(&binary).expect("rogue identity");

    // Deploy ACP policy + schema (encryption is enabled at node level)
    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("add schema");

    // Jack creates an encrypted, ACP-protected document
    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret Tweet", age: 42}) { _docID name age } }"#,
            &jack.private_key_hex,
        )
        .expect("create encrypted doc");
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    let query = "query { User { _docID name age } }";

    // Owner reads encrypted doc -> ALLOW, returns decrypted data
    let jack_result = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack query");
    let jack_users = jack_result["User"].as_array().expect("jack result array");
    assert_eq!(jack_users.len(), 1, "jack should see 1 encrypted doc");
    assert_eq!(
        jack_users[0]["name"], "Secret Tweet",
        "jack sees decrypted name"
    );
    assert_eq!(jack_users[0]["age"], 42, "jack sees decrypted age");

    // Rogue reads encrypted doc -> DENY (ACP blocks before decryption)
    let rogue_result = node
        .query_with_identity(query, &rogue.private_key_hex)
        .expect("rogue query");
    let rogue_users = rogue_result["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(rogue_users, 0, "rogue should see 0 (ACP denies)");

    // Anonymous reads encrypted doc -> DENY
    let anon_result = node.query(query).expect("anon query");
    let anon_users = anon_result["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(anon_users, 0, "anonymous should see 0 (ACP denies)");

    // Grant watchdog "reader" -> watchdog can read encrypted doc
    node.acp_relationship_add(
        "User",
        &doc_id,
        "reader",
        &watchdog.did,
        &jack.private_key_hex,
    )
    .expect("grant watchdog reader");

    let watchdog_result = node
        .query_with_identity(query, &watchdog.private_key_hex)
        .expect("watchdog query after grant");
    let watchdog_users = watchdog_result["User"]
        .as_array()
        .expect("watchdog result array");
    assert_eq!(watchdog_users.len(), 1, "watchdog should see 1 after grant");
    assert_eq!(
        watchdog_users[0]["name"], "Secret Tweet",
        "watchdog sees decrypted name"
    );

    // Revoke watchdog "reader" -> watchdog can no longer read
    node.acp_relationship_delete(
        "User",
        &doc_id,
        "reader",
        &watchdog.did,
        &jack.private_key_hex,
    )
    .expect("revoke watchdog reader");

    let watchdog_result2 = node
        .query_with_identity(query, &watchdog.private_key_hex)
        .expect("watchdog query after revoke");
    let watchdog_count = watchdog_result2["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        watchdog_count, 0,
        "watchdog should see 0 after revoke (can no longer decrypt)"
    );

    // Jack updates encrypted doc -> ALLOW, re-encrypts
    let update = node.query_with_identity(
        &format!(
            r#"mutation {{ update_User(docID: "{}", input: {{name: "Updated Secret"}}) {{ _docID name }} }}"#,
            doc_id
        ),
        &jack.private_key_hex,
    );
    assert!(update.is_ok(), "jack should update encrypted doc");

    // Verify jack sees updated encrypted data
    let jack_result2 = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack query after update");
    let jack_users2 = jack_result2["User"].as_array().expect("array");
    assert_eq!(jack_users2[0]["name"], "Updated Secret");

    // Rogue tries to update encrypted doc -> DENY
    let rogue_update = node.query_with_identity(
        &format!(
            r#"mutation {{ update_User(docID: "{}", input: {{name: "Hacked"}}) {{ _docID }} }}"#,
            doc_id
        ),
        &rogue.private_key_hex,
    );
    if let Ok(result) = rogue_update {
        let updated = result["update_User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(updated, 0, "rogue should NOT update encrypted doc");
    }

    // Verify data unchanged by rogue attempt
    let jack_result3 = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack query final");
    assert_eq!(jack_result3["User"][0]["name"], "Updated Secret");
}

for_each_runtime!(encrypted_acp, encrypted_acp_test, .with_acp_local().with_encryption());

/// Go oracle: `TestDocEncryptionACP_BranchableCollection_AuthorizedPeerCanFetch`.
///
/// An encrypted document in a `@branchable` collection must merge on a peer
/// whose node identity holds `reader` on the collection object. No replicator:
/// the peer pulls through the ACP read gate, exactly as the oracle does.
#[tokio::test]
async fn rust_encrypted_branchable_collection_grant_merges_on_peer() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("jack identity");
    let node1_id = generate_identity(&binary).expect("node1 identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .skip_build()
        .with_p2p()
        .with_acp_local()
        .with_encryption()
        // Only the grantee needs a node identity. A `--identity` on node 0 would
        // become the default request identity and make the "anonymous" create
        // an owned document, which is not the oracle's public-doc scenario.
        .with_node_identity(1, &node1_id.private_key_hex)
        .build()
        .await
        .expect("build cluster");

    let timeout = Duration::from_secs(15);
    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", timeout)
            .await
            .expect("P2P listener did not start");
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let policy = node0
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy on node0");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    node1
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy on node1");

    let schema = format!(
        r#"type Users @branchable @policy(id: "{policy_id}", resource: "users") {{ name: String  age: Int }}"#
    );
    for node in [&node0, &node1] {
        node.schema_add_with_identity(&schema, &jack.private_key_hex)
            .expect("add schema");
    }

    let described = node0
        .collection_describe_version("Users")
        .expect("describe Users");
    let described = described
        .as_array()
        .and_then(|versions| versions.first())
        .cloned()
        .unwrap_or(described);
    let collection_id = described["CollectionID"]
        .as_str()
        .expect("missing CollectionID")
        .to_string();

    // The collection id stands in for the docID, exactly as the Go client does.
    for node in [&node0, &node1] {
        node.acp_relationship_add(
            "Users",
            &collection_id,
            "reader",
            &node1_id.did,
            &jack.private_key_hex,
        )
        .expect("grant node1 reader on the collection object");
    }

    let info0 = node0.p2p_info().expect("node0 p2p info");
    let addr0 = info0
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node0 has no P2P address");
    node1
        .p2p_connect(&[addr0])
        .expect("node1 connects to node0");
    node0
        .p2p_collection_add(&["Users"])
        .expect("node0 subscribes");
    node1
        .p2p_collection_add(&["Users"])
        .expect("node1 subscribes");

    node0
        .query(
            r#"mutation { add_Users(input: {name: "Fred", age: 33}, encrypt: true) { _docID } }"#,
        )
        .expect("create encrypted doc on node0");

    let node1_ref = &node1;
    let jack_key = jack.private_key_hex.clone();
    poll_until(
        || {
            node1_ref
                .query_with_identity("query { Users { name } }", &jack_key)
                .ok()
                .map(|v| v["Users"] == serde_json::json!([{"name": "Fred"}]))
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
        "encrypted branchable doc did not merge on the granted peer",
    )
    .await;
}
