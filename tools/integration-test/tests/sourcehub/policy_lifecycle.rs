use integration_test::node::{DefraNode, RustNode};
use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// Full on-chain policy lifecycle test.
///
/// 1. Create policy on Source Hub -> get policy ID
/// 2. Verify policy exists on-chain via LCD query
/// 3. Use policy ID in DefraDB schema
/// 4. Create documents governed by policy
/// 5. Grant/revoke relationships (on-chain transactions)
/// 6. Verify access changes propagate
///
/// The node is started with Alice's identity so SourceHub transactions work.
#[tokio::test]
async fn rust_sourcehub_policy_lifecycle() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let alice = generate_identity(&binary).expect("Alice identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&alice.private_key_hex)
        .build()
        .await
        .expect("failed to build cluster");

    let node = cluster.client(0);
    let sh = cluster.source_hub().expect("source hub not available");

    let bob = generate_identity(&binary).expect("Bob identity");

    // Step 1: Create policy on-chain
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("create policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID")
        .to_string();

    // Step 2: Verify policy exists on Source Hub via LCD
    let client = reqwest::Client::new();
    let policy_url = format!(
        "{}/sourcenetwork/sourcehub/acp/policy/{}",
        sh.lcd_url, policy_id
    );
    let resp = client
        .get(&policy_url)
        .send()
        .await
        .expect("LCD policy query");
    assert!(
        resp.status().is_success(),
        "policy should exist on-chain (HTTP {})",
        resp.status()
    );

    // Step 3: Deploy schema with policy
    let schema = users_schema_with_policy(&policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    // Step 4: Create a document as Alice
    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Alice", age: 25}) { _docID name } }"#,
            &alice.private_key_hex,
        )
        .expect("create user");
    let doc_id = data["add_User"][0]["_docID"].as_str().expect("_docID");

    // Bob initially cannot see the document
    let bob_before = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob query before grant");
    assert_eq!(
        bob_before["User"].as_array().unwrap().len(),
        0,
        "Bob should see 0 docs before grant"
    );

    // Step 5: Grant Bob reader (on-chain relationship tx)
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    // Step 6: Bob can now read
    let bob_after = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob query after grant");
    let bob_users = bob_after["User"].as_array().unwrap();
    assert_eq!(bob_users.len(), 1, "Bob should see 1 doc after grant");
    assert_eq!(bob_users[0]["name"], "Alice");

    // Revoke Bob's reader access
    node.acp_relationship_delete("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob reader");

    // Bob can no longer read
    let bob_revoked = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob query after revoke");
    assert_eq!(
        bob_revoked["User"].as_array().unwrap().len(),
        0,
        "Bob should see 0 docs after revoke"
    );
}
