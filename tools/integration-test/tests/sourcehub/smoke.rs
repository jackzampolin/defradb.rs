use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

use integration_test::node::{DefraNode, RustNode};

/// Smoke test proving DefraDB -> Source Hub ACP pipeline works end-to-end.
///
/// 1. Starts a Source Hub devnet + 1 Rust DefraDB node connected to it
/// 2. Creates an ACP policy (on-chain via MsgCreatePolicy)
/// 3. Creates a protected document as Jack (owner)
/// 4. Jack sees the document, anonymous sees nothing
///
/// The node must be started with Jack's identity so the SourceHub TxSigner
/// can create bearer tokens for Jack's DID.
#[tokio::test]
#[serial_test::serial]
async fn rust_sourcehub_smoke() {
    // Pre-generate Jack's identity so the node starts with his key
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("failed to generate Jack identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .build()
        .await
        .expect("failed to build source hub cluster");

    let node = cluster.client(0);

    // Add ACP policy — this submits MsgCreatePolicy on Source Hub
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("failed to add ACP policy via Source Hub");

    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("missing PolicyID in policy add result");

    // Deploy schema with @policy directive
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("failed to add schema with policy");

    // Create a protected document as Jack
    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Jack", age: 30}) { _docID name age } }"#,
            &jack.private_key_hex,
        )
        .expect("failed to create document");

    let _doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID in create result");

    // Jack queries -> sees 1 document
    let jack_result = node
        .query_with_identity("query { User { _docID name age } }", &jack.private_key_hex)
        .expect("Jack query failed");

    let jack_users = jack_result["User"]
        .as_array()
        .expect("Jack result not array");
    assert_eq!(jack_users.len(), 1, "Jack should see 1 document");
    assert_eq!(jack_users[0]["name"], "Jack");

    // Anonymous query -> sees 0 documents (ACP enforced via Source Hub)
    let anon_result = node
        .query("query { User { _docID name age } }")
        .expect("anonymous query failed");

    let anon_users = anon_result["User"]
        .as_array()
        .expect("anon result not array");
    assert_eq!(
        anon_users.len(),
        0,
        "anonymous should see 0 documents (Source Hub ACP)"
    );
}
