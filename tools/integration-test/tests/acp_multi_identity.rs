use integration_test::{
    for_each_runtime, generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

async fn acp_multi_identity_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");
    let carol = generate_identity(&binary).expect("Carol identity");
    let dave = generate_identity(&binary).expect("Dave identity");
    let eve = generate_identity(&binary).expect("Eve identity");

    // Alice adds policy + schema
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

    // Alice creates 3 documents
    let d1 = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Alpha", age: 10}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create Alpha");
    let id1 = d1["create_User"][0]["_docID"].as_str().expect("id1");

    let d2 = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Beta", age: 20}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create Beta");
    let id2 = d2["create_User"][0]["_docID"].as_str().expect("id2");

    let d3 = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Gamma", age: 30}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create Gamma");
    let id3 = d3["create_User"][0]["_docID"].as_str().expect("id3");

    let query = "query { User { _docID name age } }";

    // Visibility check: Alice=3, others=0
    let count = |key: &str| -> usize {
        node.query_with_identity(query, key).expect("query")["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    };
    assert_eq!(count(&alice.private_key_hex), 3, "Alice should see 3");
    assert_eq!(count(&bob.private_key_hex), 0, "Bob should see 0");
    assert_eq!(count(&carol.private_key_hex), 0, "Carol should see 0");
    assert_eq!(count(&dave.private_key_hex), 0, "Dave should see 0");
    assert_eq!(count(&eve.private_key_hex), 0, "Eve should see 0");

    // Grant Bob "reader" on Alpha
    node.acp_relationship_add("User", id1, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader on Alpha");
    assert_eq!(
        count(&bob.private_key_hex),
        1,
        "Bob should see 1 after grant"
    );

    // Grant Carol "writer" on Beta
    node.acp_relationship_add("User", id2, "writer", &carol.did, &alice.private_key_hex)
        .expect("grant Carol writer on Beta");
    assert_eq!(count(&carol.private_key_hex), 1, "Carol should see 1");

    // Grant Dave "reader" on all 3 docs
    node.acp_relationship_add("User", id1, "reader", &dave.did, &alice.private_key_hex)
        .expect("grant Dave reader on Alpha");
    node.acp_relationship_add("User", id2, "reader", &dave.did, &alice.private_key_hex)
        .expect("grant Dave reader on Beta");
    node.acp_relationship_add("User", id3, "reader", &dave.did, &alice.private_key_hex)
        .expect("grant Dave reader on Gamma");
    assert_eq!(count(&dave.private_key_hex), 3, "Dave should see 3");

    // Revoke Bob's "reader" on Alpha via acp_relationship_delete
    node.acp_relationship_delete("User", id1, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob reader on Alpha");
    assert_eq!(
        count(&bob.private_key_hex),
        0,
        "Bob should see 0 after revoke"
    );

    // Carol updates Beta (as writer)
    node.query_with_identity(
        &format!(
            r#"mutation {{ update_User(docID: "{}", input: {{age: 99}}) {{ _docID }} }}"#,
            id2
        ),
        &carol.private_key_hex,
    )
    .expect("Carol update Beta");

    // Alice sees the update
    let alice_result = node
        .query_with_identity(query, &alice.private_key_hex)
        .expect("Alice query after update");
    let beta_doc = alice_result["User"]
        .as_array()
        .expect("array")
        .iter()
        .find(|d| d["name"] == "Beta")
        .expect("Beta doc");
    assert_eq!(
        beta_doc["age"], 99,
        "Beta age should be 99 after Carol's update"
    );

    // Final visibility check
    assert_eq!(count(&alice.private_key_hex), 3, "Alice final=3");
    assert_eq!(count(&bob.private_key_hex), 0, "Bob final=0");
    assert_eq!(count(&carol.private_key_hex), 1, "Carol final=1");
    assert_eq!(count(&dave.private_key_hex), 3, "Dave final=3");
    assert_eq!(count(&eve.private_key_hex), 0, "Eve final=0");
}

for_each_runtime!(acp_multi_identity, acp_multi_identity_test, .with_acp_local());
