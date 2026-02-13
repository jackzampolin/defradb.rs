use integration_test::{
    generate_ed25519_identity, generate_identity, users_schema_with_policy, TestCluster,
    USER_ACP_POLICY,
};

async fn identity_types_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // Generate secp256k1 identity (default)
    let secp = generate_identity(&binary).expect("secp256k1 identity");
    assert!(
        secp.private_key_hex.len() == 64,
        "secp256k1 key should be 64 hex chars, got {}",
        secp.private_key_hex.len()
    );
    assert!(
        secp.did.starts_with("did:key:zQ3s"),
        "secp256k1 DID should start with did:key:zQ3s, got {}",
        secp.did
    );

    // Generate ed25519 identity
    let ed = generate_ed25519_identity(&binary).expect("ed25519 identity");
    assert!(
        ed.private_key_hex.len() == 128,
        "ed25519 key should be 128 hex chars, got {}",
        ed.private_key_hex.len()
    );
    assert!(
        ed.did.starts_with("did:key:z6Mk"),
        "ed25519 DID should start with did:key:z6Mk, got {}",
        ed.did
    );

    // Set up ACP with secp256k1 identity as owner
    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &secp.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &secp.private_key_hex)
        .expect("add schema");

    // secp256k1 identity creates a protected document
    let d = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "CrossKey", age: 42}) { _docID } }"#,
            &secp.private_key_hex,
        )
        .expect("create doc");
    let doc_id = d["create_User"][0]["_docID"].as_str().expect("doc_id");

    let query = "query { User { _docID name age } }";

    // ed25519 identity can't see it yet
    let ed_result = node
        .query_with_identity(query, &ed.private_key_hex)
        .expect("ed25519 query before grant");
    let ed_count = ed_result["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(ed_count, 0, "ed25519 should see 0 before grant");

    // Grant ed25519 identity "reader" using its DID
    node.acp_relationship_add("User", doc_id, "reader", &ed.did, &secp.private_key_hex)
        .expect("grant ed25519 reader");

    // ed25519 identity can now see the document
    let ed_result2 = node
        .query_with_identity(query, &ed.private_key_hex)
        .expect("ed25519 query after grant");
    let ed_count2 = ed_result2["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(ed_count2, 1, "ed25519 should see 1 after grant");
    assert_eq!(ed_result2["User"][0]["name"], "CrossKey");
}

#[tokio::test]
#[ignore]
async fn rust_identity_types() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    identity_types_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_identity_types() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    identity_types_test(cluster).await;
}
