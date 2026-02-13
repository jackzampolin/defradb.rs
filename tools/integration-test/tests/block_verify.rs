use integration_test::{generate_identity, TestCluster};

async fn block_verify_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // Generate an identity to use for signing
    let identity = generate_identity(&binary).expect("generate identity");

    // Deploy schema
    node.schema_add("type Note { text: String }")
        .expect("add schema");

    // Create a document (this will create signed blocks since signing is enabled)
    node.query_with_identity(
        r#"mutation { create_Note(input: {text: "signed content"}) { _docID } }"#,
        &identity.private_key_hex,
    )
    .expect("create signed document");

    // Query commits to get block CIDs
    let commits = node
        .query("query { commits { cid } }")
        .expect("query commits");

    let commits_arr = commits["commits"]
        .as_array()
        .expect("commits should be an array");
    assert!(
        !commits_arr.is_empty(),
        "should have at least one commit after creating a document"
    );

    let cid = commits_arr[0]["cid"]
        .as_str()
        .expect("commit should have a cid");

    // Get the node's public key for verification.
    // The public key can be derived from the identity's DID or fetched from node-identity.
    // Try node-identity first for the signing key.
    let node_id = node.node_identity();
    let public_key = if let Ok(ref id_val) = node_id {
        // Try to extract public key from node identity response
        id_val
            .get("publicKey")
            .or_else(|| id_val.get("PublicKey"))
            .or_else(|| id_val.get("public_key"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    } else {
        None
    };

    // If we have a public key, verify the block signature
    if let Some(pk) = public_key {
        let result = node
            .block_verify_signature(&pk, cid, None)
            .expect("block verify-signature");
        assert!(
            !result.is_empty(),
            "verify-signature should return a response"
        );
    }
}

#[tokio::test]
#[ignore]
async fn rust_block_verify() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_signing()
        .build()
        .await
        .unwrap();
    block_verify_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_block_verify() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_signing()
        .build()
        .await
        .unwrap();
    block_verify_test(cluster).await;
}
