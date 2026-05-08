use integration_test::node::RustNode;
use integration_test::{users_schema_with_policy, USER_ACP_POLICY};

use super::helpers;

/// Smoke test proving DefraDB -> hub.rs ACP pipeline works end-to-end.
///
/// 1. Starts a hub.rs devnet + 1 Rust DefraDB node connected to it
/// 2. Creates an ACP policy (on-chain via EVM precompile)
/// 3. Creates a protected document as Jack (owner)
/// 4. Jack sees the document, anonymous sees nothing
#[tokio::test]
#[serial_test::serial]
async fn rust_hubrs_smoke() {
    RustNode::build().expect("build rust binary");
    let jack = helpers::funded_identity();

    let hub = helpers::start_hub_cluster().await;
    let hub_rpc_url = hub.node(0).rpc_url();

    let cluster =
        helpers::build_defra_with_hub_rs(&hub_rpc_url, &jack.private_key_hex, 1, false).await;
    let node = cluster.client(0);

    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("failed to add ACP policy via hub.rs");

    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("missing PolicyID in policy add result");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("failed to add schema with policy");

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Jack", age: 30}) { _docID name age } }"#,
            &jack.private_key_hex,
        )
        .expect("failed to create document");

    let _doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID in create result");

    let jack_result = node
        .query_with_identity("query { User { _docID name age } }", &jack.private_key_hex)
        .expect("Jack query failed");

    let jack_users = jack_result["User"]
        .as_array()
        .expect("Jack result not array");
    assert_eq!(jack_users.len(), 1, "Jack should see 1 document");
    assert_eq!(jack_users[0]["name"], "Jack");

    let anon_result = node
        .query("query { User { _docID name age } }")
        .expect("anonymous query failed");

    let anon_users = anon_result["User"]
        .as_array()
        .expect("anon result not array");
    assert_eq!(
        anon_users.len(),
        0,
        "anonymous should see 0 documents (hub.rs ACP)"
    );
}
