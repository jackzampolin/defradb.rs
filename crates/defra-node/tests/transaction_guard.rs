use std::sync::Arc;

use defra_node::{EmbeddedNode, QueryRequest, TransactionHandle};

const CREATE: &str = r#"mutation { add_Users(input: {name: "Alice"}) { _docID } }"#;
const READ: &str = "{ Users { name } }";

async fn node() -> EmbeddedNode {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema("type Users { name: String }")
        .await
        .unwrap();
    node
}

async fn assert_discarded(node: &EmbeddedNode, handle: &TransactionHandle) {
    assert!(node
        .execute_request_in_txn(QueryRequest::new(READ), handle)
        .await
        .has_errors());
    let response = node.execute(READ).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(response.data.unwrap()["Users"], serde_json::json!([]));
}

#[tokio::test]
async fn dropped_guard_invalidates_saved_handle_and_discards_writes() {
    let node = node().await;
    let txn = node.begin_transaction_guard(false).await.unwrap();
    let response = txn.execute(CREATE).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let saved = txn.handle().clone();
    drop(txn);
    assert_discarded(&node, &saved).await;
}

#[tokio::test]
async fn aborted_task_discards_owned_transaction() {
    let node = Arc::new(node().await);
    let owner = node.clone();
    let (ready, handle) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let txn = owner.begin_transaction_guard(false).await.unwrap();
        let response = txn.execute(CREATE).await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        ready.send(txn.handle().clone()).unwrap();
        std::future::pending::<()>().await;
        drop(txn);
    });
    let saved = handle.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_discarded(&node, &saved).await;
}

#[tokio::test]
async fn explicit_commit_and_rollback_consume_the_guard() {
    let node = node().await;
    let txn = node.begin_transaction_guard(false).await.unwrap();
    assert!(!txn.execute(CREATE).await.has_errors());
    let saved = txn.handle().clone();
    txn.rollback().await.unwrap();
    assert_discarded(&node, &saved).await;

    let txn = node.begin_transaction_guard(false).await.unwrap();
    assert!(!txn.execute(CREATE).await.has_errors());
    drop(txn.handle().clone());
    assert!(!txn.execute(READ).await.has_errors());
    txn.commit().await.unwrap();
    let response = node.execute(READ).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(
        response.data.unwrap()["Users"],
        serde_json::json!([{"name": "Alice"}])
    );
}

#[tokio::test]
async fn owned_transaction_keeps_node_signing_context() {
    use crypto::Key;
    use defra_core::signing::{SigningConfig, SigningKeyType};
    use identity::{Identity, RawIdentity};

    let key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_ed25519(key.clone()).unwrap();
    let did = identity.did().unwrap().to_string();
    let public_key_bytes = identity.public_key_bytes();
    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Ed25519,
            private_key_bytes: key.raw().to_vec(),
            public_key_hex: hex::encode(&public_key_bytes),
            public_key_bytes,
            remote_signer: None,
            signing_authorization: None,
        },
    );
    let node = EmbeddedNode::builder()
        .with_node_identity_did(&did)
        .build()
        .await
        .unwrap();
    node.add_schema("type Users { name: String }")
        .await
        .unwrap();
    let txn = node.begin_transaction_guard(false).await.unwrap();
    let response = txn.execute(CREATE).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let doc_id = response.data.unwrap()["add_Users"][0]["_docID"]
        .as_str()
        .unwrap()
        .to_owned();
    let query = format!(
        r#"{{ _commits(docID: "{doc_id}", filter: {{fieldName: {{_eq: "_C"}}}}) {{ cid }} }}"#
    );
    let response = txn.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let cid = response.data.unwrap()["_commits"][0]["cid"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        node.verified_block_signer_did_in_txn(&cid, txn.handle())
            .await
            .unwrap(),
        did
    );
    txn.commit().await.unwrap();
    assert_eq!(node.verified_block_signer_did(&cid).await.unwrap(), did);
}
