use std::time::Duration;

use integration_test::{
    for_each_p2p_topology, for_each_runtime, generate_identity, poll_until,
    users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

/// 02-24: P2P replication does not grant cross-node access to ACP-protected documents.
///
/// When a document replicates from node0 to node1, the ACP state does NOT replicate.
/// An identity that was not granted access on node1 must not be able to read the
/// protected document on node1 even after replication.
///
/// This tests the merge-denial property: receiving a replicated block does not
/// automatically grant read permission to any identity on the receiving node.
async fn p2p_merge_denial_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let binary = node0.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let info1 = node1.p2p_info().expect("get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Add ACP policy + schema on both nodes using Alice's identity
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

    // Alice grants Bob reader on node0
    let data = node0
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Protected", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create protected doc on node0");
    let doc_id = data["create_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    node0
        .acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader on node0");

    // Bob sees the doc on node0 (grant is local to node0)
    let bob_node0 = node0
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob query on node0");
    assert_eq!(
        bob_node0["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "Bob must see the document on node0 after explicit grant"
    );

    // Wait for the document to replicate to node1.
    // Use Alice's identity: after the ACP fix, anonymous queries cannot see
    // ACP-registered documents, so we poll as the owner.
    let node1_ref = &node1;
    let alice_key = alice.private_key_hex.clone();
    poll_until(
        || {
            node1_ref
                .query_with_identity("query { User { _docID } }", &alice_key)
                .ok()
                .and_then(|v| v["User"].as_array().map(|a| !a.is_empty()))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "document did not replicate to node1",
    )
    .await;

    // Bob queries node1 — the ACP grant from node0 did NOT replicate.
    // Bob must see 0 documents on node1 (merge-denial: replication ≠ access grant).
    let bob_node1 = node1
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob query on node1");
    let bob_node1_count = bob_node1["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(
        bob_node1_count, 0,
        "ACP relationships must not replicate — Bob must not access the document on node1 without an explicit grant there"
    );

    // Alice can still read the document on node1 (she's the owner, not relying on grant)
    let alice_node1 = node1
        .query_with_identity("query { User { _docID name } }", &alice.private_key_hex)
        .expect("Alice query on node1");
    assert_eq!(
        alice_node1["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "Alice must read her document on node1 as owner"
    );
}

for_each_p2p_topology!(p2p_merge_denial, p2p_merge_denial_test, .with_p2p().with_acp_local());

/// 02-28: Policy transition guard — revoking access before a schema policy change
/// correctly blocks the formerly-authorized user.
///
/// This tests the guard activation sequence: grant → revoke → verify denial.
/// It simulates a "policy transition" scenario where access is explicitly revoked
/// and then the revocation is verified to be effective.
async fn policy_transition_guard_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");
    let carol = generate_identity(&binary).expect("Carol identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    let data = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Guarded", age: 5}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["create_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    let count = |key: &str| -> usize {
        node.query_with_identity("query { User { _docID name } }", key)
            .expect("query")["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    };

    // Phase 1: grant Bob and Carol access
    node.acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");
    node.acp_relationship_add(
        "User",
        &doc_id,
        "writer",
        &carol.did,
        &alice.private_key_hex,
    )
    .expect("grant Carol writer");

    assert_eq!(count(&bob.private_key_hex), 1, "Bob sees doc after grant");
    assert_eq!(
        count(&carol.private_key_hex),
        1,
        "Carol sees doc after grant"
    );

    // Phase 2: transition guard — revoke both before policy change
    node.acp_relationship_delete("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob reader");
    node.acp_relationship_delete(
        "User",
        &doc_id,
        "writer",
        &carol.did,
        &alice.private_key_hex,
    )
    .expect("revoke Carol writer");

    // Guard must be effective immediately — both must now see 0 documents
    assert_eq!(
        count(&bob.private_key_hex),
        0,
        "Bob must be blocked immediately after revocation (transition guard)"
    );
    assert_eq!(
        count(&carol.private_key_hex),
        0,
        "Carol must be blocked immediately after revocation (transition guard)"
    );

    // Phase 3: verify guard does not affect owner (Alice) — she still sees the doc
    assert_eq!(count(&alice.private_key_hex), 1, "Alice (owner) unaffected");

    // Phase 4: re-grant Bob only — Carol remains blocked
    node.acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("re-grant Bob reader");

    assert_eq!(
        count(&bob.private_key_hex),
        1,
        "Bob regains access after re-grant"
    );
    assert_eq!(
        count(&carol.private_key_hex),
        0,
        "Carol remains blocked — transition guard still active for her"
    );

    // Phase 5: verify Carol cannot write (mutation denial after guard)
    let carol_write = node.query_with_identity(
        &format!(
            r#"mutation {{ update_User(docID: "{}", input: {{age: 99}}) {{ _docID }} }}"#,
            doc_id
        ),
        &carol.private_key_hex,
    );
    match carol_write {
        Err(_) => {}
        Ok(val) => {
            let updated = val["update_User"].as_array().map(|a| a.len()).unwrap_or(0);
            assert_eq!(
                updated, 0,
                "Carol must not update after transition guard revoked her writer access"
            );
        }
    }

    // Document content is still intact
    let final_read = node
        .query_with_identity("query { User { _docID name age } }", &alice.private_key_hex)
        .expect("final read");
    let users = final_read["User"].as_array().expect("User array");
    assert_eq!(
        users.len(),
        1,
        "document must survive all transition operations"
    );
    assert_eq!(users[0]["name"], "Guarded", "document content unchanged");
    assert_eq!(
        users[0]["age"], 5,
        "age unchanged after Carol's blocked write"
    );
}

for_each_runtime!(policy_transition_guard, policy_transition_guard_test, .with_acp_local());
