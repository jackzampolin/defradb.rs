use crypto::Key;
use defra_core::signing::{SigningConfig, SigningKeyType};
use defra_node::{EmbeddedNode, QueryRequest};
use identity::{Identity as _, RawIdentity};

fn register_local_node_identity() -> (String, String) {
    let private_key = crypto::generate_ed25519().expect("generate ed25519 key");
    let identity =
        RawIdentity::from_ed25519(private_key.clone()).expect("build raw identity from key");
    let did = identity.did().expect("derive DID").to_string();
    let public_key_bytes = identity.public_key_bytes();
    let public_key_hex = hex::encode(&public_key_bytes);

    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Ed25519,
            private_key_bytes: private_key.raw().to_vec(),
            public_key_bytes,
            public_key_hex: public_key_hex.clone(),
            remote_signer: None,
            signing_authorization: None,
        },
    );

    (did, public_key_hex)
}

#[tokio::test]
async fn embedded_transaction_mutation_uses_node_signer() {
    defra_core::signing::clear_identity_store();
    let (did, expected_identity) = register_local_node_identity();

    let node = EmbeddedNode::builder()
        .with_node_identity_did(&did)
        .build()
        .await
        .expect("build signed node");
    node.add_schema("type Widget { name: String }")
        .await
        .expect("add schema");

    let txn = node.runner().begin_txn(false).await.expect("begin txn");
    let create = node
        .execute_request_in_txn(
            QueryRequest::new(r#"mutation { create_Widget(input: {name: "gadget"}) { _docID } }"#),
            &txn,
        )
        .await;
    assert!(
        create.errors.is_empty(),
        "create failed: {:?}",
        create.errors
    );
    let create_data = create.data.as_ref().expect("create response data");
    let doc_id = create_data
        .pointer("/add_Widget/0/_docID")
        .or_else(|| create_data.pointer("/create_Widget/0/_docID"))
        .or_else(|| create_data.pointer("/create_Widget/_docID"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("created document id missing from {create_data}"))
        .to_string();
    node.runner().commit_txn(&txn).await.expect("commit txn");

    let commits = node
        .execute(&format!(
            r#"query {{ _commits(docID: "{doc_id}", filter: {{fieldName: {{_eq: "_C"}}}}) {{ signature {{ type identity }} }} }}"#
        ))
        .await;
    assert!(
        commits.errors.is_empty(),
        "commit query failed: {:?}",
        commits.errors
    );
    let rows = commits
        .data
        .as_ref()
        .and_then(|data| data.get("_commits"))
        .and_then(serde_json::Value::as_array)
        .expect("commit rows");
    assert!(
        rows.iter().any(|row| {
            row.pointer("/signature/type")
                .and_then(serde_json::Value::as_str)
                == Some("EdDSA")
                && row
                    .pointer("/signature/identity")
                    .and_then(serde_json::Value::as_str)
                    == Some(expected_identity.as_str())
        }),
        "composite commit was not signed by the configured node: {rows:?}"
    );

    node.shutdown().await;
    defra_core::signing::clear_identity_store();
}
