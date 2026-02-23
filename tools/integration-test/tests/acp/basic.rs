use integration_test::{
    for_each_runtime, generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

async fn acp_basic_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();

    // Generate two identities: Alice (owner) and Bob (outsider)
    let alice = generate_identity(&binary_path).expect("failed to generate Alice identity");
    let bob = generate_identity(&binary_path).expect("failed to generate Bob identity");

    // Add ACP policy as Alice
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("failed to add ACP policy");

    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("missing PolicyID in policy add result");

    // Deploy schema with @policy directive as Alice
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("failed to add schema with policy");

    // Create a protected document as Alice
    let data = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Secret", age: 42}) { _docID name age } }"#,
            &alice.private_key_hex,
        )
        .expect("failed to create protected document");

    let doc_id = data["create_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    // Alice queries -> sees the document
    let alice_result = node
        .query_with_identity("query { User { _docID name age } }", &alice.private_key_hex)
        .expect("Alice query failed");

    let alice_users = alice_result["User"]
        .as_array()
        .expect("Alice result not array");
    assert_eq!(alice_users.len(), 1, "Alice should see 1 document");
    assert_eq!(alice_users[0]["name"], "Secret");

    // Bob queries -> sees 0 documents (ACP blocks)
    let bob_result = node
        .query_with_identity("query { User { _docID name age } }", &bob.private_key_hex)
        .expect("Bob query failed");

    let bob_users = bob_result["User"].as_array().expect("Bob result not array");
    assert_eq!(
        bob_users.len(),
        0,
        "Bob should see 0 documents before grant"
    );

    // Alice grants Bob "reader" relationship on the document
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("failed to add reader relationship");

    // Bob queries again -> now sees the document
    let bob_result2 = node
        .query_with_identity("query { User { _docID name age } }", &bob.private_key_hex)
        .expect("Bob query after grant failed");

    let bob_users2 = bob_result2["User"]
        .as_array()
        .expect("Bob result2 not array");
    assert_eq!(bob_users2.len(), 1, "Bob should see 1 document after grant");
    assert_eq!(bob_users2[0]["name"], "Secret");
}

for_each_runtime!(acp_basic, acp_basic_test, .with_acp_local());
