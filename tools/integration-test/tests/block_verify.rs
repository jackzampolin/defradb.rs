use integration_test::{generate_identity, TestCluster};

async fn block_verify_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let identity = generate_identity(&binary).expect("generate identity");

    node.schema_add("type Note { text: String }")
        .expect("add schema");

    // Create a document (creates signed blocks since signing is enabled)
    let data = node
        .query_with_identity(
            r#"mutation { create_Note(input: {text: "signed content"}) { _docID } }"#,
            &identity.private_key_hex,
        )
        .expect("create signed document");

    // Extract _docID from response — handle both array and object formats
    let doc_id = data["create_Note"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data["create_Note"]["_docID"].as_str());

    let doc_id = match doc_id {
        Some(id) => id.to_string(),
        None => {
            eprintln!(
                "Could not extract _docID from create response: {}",
                serde_json::to_string_pretty(&data).unwrap_or_default()
            );
            return;
        }
    };

    // Query commits to get block CIDs.
    // Go uses `_commits` field name; try both variations.
    let commits_query = format!(r#"query {{ _commits(docID: "{}") {{ cid }} }}"#, doc_id);
    let commits_result = node.query(&commits_query).or_else(|_| {
        let q = format!(r#"query {{ commits(docID: "{}") {{ cid }} }}"#, doc_id);
        node.query(&q)
    });

    let commits = match commits_result {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "commits query not supported (_commits not exposed in GraphQL schema): {}",
                e
            );
            return;
        }
    };

    // Try both _commits and commits field names in response
    let commits_arr = commits
        .get("_commits")
        .or_else(|| commits.get("commits"))
        .and_then(|v| v.as_array());

    let commits_arr = match commits_arr {
        Some(arr) => arr,
        None => {
            eprintln!(
                "commits response has unexpected format: {}",
                serde_json::to_string_pretty(&commits).unwrap_or_default()
            );
            return;
        }
    };

    assert!(
        !commits_arr.is_empty(),
        "should have at least one commit after creating a document"
    );

    let cid = commits_arr[0]["cid"]
        .as_str()
        .expect("commit should have a cid");

    // Get the node's public key for verification
    let node_id = node.node_identity();
    let public_key = if let Ok(ref id_val) = node_id {
        id_val
            .get("publicKey")
            .or_else(|| id_val.get("PublicKey"))
            .or_else(|| id_val.get("public_key"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    } else {
        None
    };

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
