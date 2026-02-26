use integration_test::node::{DefraNode, RustNode};
use integration_test::{generate_identity, users_schema_with_policy, USER_ACP_POLICY};

use super::helpers;

/// Full on-chain policy lifecycle test via hub.rs.
///
/// 1. Create policy on hub.rs -> get policy ID
/// 2. Verify policy exists on-chain via getPolicy precompile query
/// 3. Use policy ID in DefraDB schema
/// 4. Create documents governed by policy
/// 5. Grant/revoke relationships (on-chain transactions via EVM)
/// 6. Verify access changes propagate
#[tokio::test]
async fn rust_hubrs_policy_lifecycle() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let alice = generate_identity(&binary).expect("Alice identity");

    let hub = helpers::start_hub_cluster().await;
    let hub_rpc_url = hub.node(0).rpc_url();

    let cluster =
        helpers::build_defra_with_hub_rs(&hub_rpc_url, &alice.private_key_hex, 1, false).await;
    let node = cluster.client(0);

    let bob = generate_identity(&binary).expect("Bob identity");

    // Create policy on-chain
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("create policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID")
        .to_string();

    // Verify policy exists on-chain via EVM precompile query
    let exists = helpers::policy_exists_on_chain(&hub_rpc_url, &policy_id).await;
    assert!(exists, "policy should exist on-chain after creation");

    // Deploy schema with policy
    let schema = users_schema_with_policy(&policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    // Create a document as Alice
    let data = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Alice", age: 25}) { _docID name } }"#,
            &alice.private_key_hex,
        )
        .expect("create user");
    let doc_id = data["create_User"][0]["_docID"].as_str().expect("_docID");

    // Bob initially cannot see the document
    let bob_before = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob query before grant");
    assert_eq!(
        bob_before["User"].as_array().unwrap().len(),
        0,
        "Bob should see 0 docs before grant"
    );

    // Grant Bob reader (on-chain relationship tx via EVM)
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    // Bob can now read
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
