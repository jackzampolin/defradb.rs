use super::tests::SIGNING_STORE_GUARD;
use super::EmbeddedNode;
use crypto::Key;
use defra_core::signing::{SigningConfig, SigningKeyType};
use identity::{Identity as _, RawIdentity};

const POLICY_YAML: &str = r#"
name: Sensitive Rows
resources:
  - name: users
    relations:
      - name: reader
    permissions:
      - name: read
        expr: reader
      - name: update
      - name: delete
"#;
const DID_A: &str = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
const DID_B: &str = "did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR";

async fn node_with_protected_users() -> (EmbeddedNode, String) {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let policy_id = node.add_dac_policy(DID_A, POLICY_YAML).await.unwrap();
    node.add_schema(&format!(
        "type Users @policy(id: \"{policy_id}\", resource: \"users\") {{ name: String }}"
    ))
    .await
    .unwrap();

    let did_a = identity::Did::new(DID_A).unwrap();
    let create = node
        .execute_request_with_retry(
            query::QueryRequest::new(
                "mutation { add_Users(input: {name: \"secret\"}) { _docID } }",
            )
            .with_identity(Some(did_a)),
            Default::default(),
        )
        .await;
    assert!(!create.has_errors(), "{:?}", create.errors);
    let doc_id = create.data.as_ref().unwrap()["add_Users"][0]["_docID"]
        .as_str()
        .unwrap()
        .to_string();
    (node, doc_id)
}

/// Generate a fresh Ed25519 node identity and register it in the process-local
/// signing registry with exportable key bytes, so `with_node_acp_enabled` can
/// match it against the database node DID.
fn register_local_node_identity() -> String {
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_ed25519(private_key.clone()).unwrap();
    let did = identity.did().unwrap().to_string();

    let public_key_bytes = identity.public_key_bytes();
    defra_core::signing::store_identity(
        &did,
        SigningConfig {
            key_type: SigningKeyType::Ed25519,
            private_key_bytes: private_key.raw().to_vec(),
            public_key_bytes: public_key_bytes.clone(),
            public_key_hex: hex::encode(&public_key_bytes),
            remote_signer: None,
            signing_authorization: None,
        },
    );

    did
}

async fn visible_users(node: &EmbeddedNode, identity: Option<identity::Did>) -> usize {
    let response = node
        .execute_request_with_retry(
            query::QueryRequest::new("query { Users { name } }").with_identity(identity),
            Default::default(),
        )
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    response.data.unwrap()["Users"].as_array().unwrap().len()
}

#[tokio::test]
async fn add_dac_policy_rejects_empty_identity_and_policy() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let err = node.add_dac_policy("", POLICY_YAML).await.unwrap_err();
    assert_eq!(err.to_string(), "policy creator can not be empty");
    let err = node.add_dac_policy(DID_A, "").await.unwrap_err();
    assert_eq!(err.to_string(), "policy data can not be empty");
    node.shutdown().await;
}

#[tokio::test]
async fn add_dac_policy_returns_id_and_validates_policy() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let policy_id = node.add_dac_policy(DID_A, POLICY_YAML).await.unwrap();
    assert!(!policy_id.is_empty());
    let bad = "name: Bad\nresources:\n  - name: users\n    relations:\n      - name: reader\n    permissions:\n      - name: read\n        expr: undeclared\n";
    assert!(node.add_dac_policy(DID_A, bad).await.is_err());
    node.shutdown().await;
}

#[tokio::test]
async fn add_schema_rejects_unregistered_policy_id() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let err = node
        .add_schema("type Users @policy(id: \"deadbeef\", resource: \"users\") { name: String }")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("schema policy validation error"),
        "{err:#}"
    );

    node.add_schema("type Plain { name: String }")
        .await
        .expect("a schema without @policy must not need the policy store");
    node.shutdown().await;
}

#[tokio::test]
async fn add_view_rejects_unregistered_policy_id() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema("type Plain { name: String }")
        .await
        .unwrap();

    let err = node
        .add_view(
            "Plain { name }",
            "type PlainView @policy(id: \"deadbeef\", resource: \"users\") { name: String }",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("schema policy validation error"),
        "{err:#}"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn dac_share_journey_grant_and_revoke() {
    let (node, doc_id) = node_with_protected_users().await;
    let did_b = identity::Did::new(DID_B).unwrap();

    assert_eq!(visible_users(&node, Some(did_b.clone())).await, 0);
    assert_eq!(visible_users(&node, None).await, 0);

    let mut updates = node.subscribe(&[events::EventName::Update]);
    let existed = node
        .add_dac_actor_relationship(DID_A, "Users", &doc_id, "reader", DID_B)
        .await
        .unwrap();
    assert!(!existed, "first grant must be newly added");
    let message = tokio::time::timeout(std::time::Duration::from_secs(2), updates.recv())
        .await
        .expect("new grant must publish a doc-update event")
        .expect("event bus closed");
    let update = message.as_update().expect("document update");
    assert_eq!(update.doc_id, doc_id);
    assert!(!update.block.is_empty());
    assert_eq!(visible_users(&node, Some(did_b.clone())).await, 1);

    assert!(
        node.add_dac_actor_relationship(DID_A, "Users", &doc_id, "reader", DID_B)
            .await
            .unwrap(),
        "second grant reports existed_already"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), updates.recv())
            .await
            .is_err(),
        "an existed-already grant must not publish a doc-update event"
    );

    assert!(node
        .delete_dac_actor_relationship(DID_A, "Users", &doc_id, "reader", DID_B)
        .await
        .unwrap());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), updates.recv())
            .await
            .is_err(),
        "a revoke must not publish a doc-update event"
    );
    assert_eq!(visible_users(&node, Some(did_b)).await, 0);
    assert!(!node
        .delete_dac_actor_relationship(DID_A, "Users", &doc_id, "reader", DID_B)
        .await
        .unwrap());
    node.shutdown().await;
}

#[tokio::test]
async fn dac_relationship_rejects_owner_relation() {
    let (node, doc_id) = node_with_protected_users().await;

    let add = node
        .add_dac_actor_relationship(DID_A, "Users", &doc_id, "owner", DID_B)
        .await
        .unwrap_err();
    assert_eq!(
        add.to_string(),
        "OPERATION_FORBIDDEN: cannot add owner relation"
    );
    let delete = node
        .delete_dac_actor_relationship(DID_A, "Users", &doc_id, "owner", DID_B)
        .await
        .unwrap_err();
    assert_eq!(
        delete.to_string(),
        "OPERATION_FORBIDDEN: cannot delete owner relation"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn dac_wildcard_grant_opens_document_to_every_actor() {
    let (node, doc_id) = node_with_protected_users().await;

    assert!(!node
        .add_dac_actor_relationship(DID_A, "Users", &doc_id, "reader", "*")
        .await
        .unwrap());
    assert_eq!(visible_users(&node, None).await, 1);
    node.shutdown().await;
}

#[tokio::test]
async fn dac_relationship_requires_a_collection_policy() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema("type Plain { name: String }")
        .await
        .unwrap();

    let add = node
        .add_dac_actor_relationship(DID_A, "Plain", "doc-1", "reader", DID_B)
        .await
        .unwrap_err();
    assert_eq!(
        add.to_string(),
        "operation requires ACP, but collection has no policy"
    );
    let delete = node
        .delete_dac_actor_relationship(DID_A, "Plain", "doc-1", "reader", DID_B)
        .await
        .unwrap_err();
    assert_eq!(
        delete.to_string(),
        "operation requires ACP, but collection has no policy"
    );
    node.shutdown().await;
}

#[tokio::test]
async fn document_acp_handle_registers_and_checks_a_document() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    let policy_id = node.add_dac_policy(DID_A, POLICY_YAML).await.unwrap();
    let did_a = identity::Did::new(DID_A).unwrap();

    let document_acp = node.document_acp();
    document_acp
        .register_doc_object(&did_a, &policy_id, "users", "doc-x")
        .await
        .unwrap();

    assert!(document_acp
        .check_doc_access(
            &acp::Identity::Authenticated(did_a),
            acp::DocumentPermission::Read,
            &policy_id,
            "users",
            "doc-x",
        )
        .await
        .unwrap());
    assert!(!document_acp
        .check_doc_access(
            &acp::Identity::Anonymous,
            acp::DocumentPermission::Read,
            &policy_id,
            "users",
            "doc-x",
        )
        .await
        .unwrap());
    node.shutdown().await;
}

#[tokio::test]
async fn add_dac_policy_is_gated_by_node_access_control() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();
    let node_did = register_local_node_identity();

    let node = EmbeddedNode::builder()
        .with_node_identity_did(&node_did)
        .with_node_acp_enabled()
        .build()
        .await
        .unwrap();

    let err = node.add_dac_policy(DID_B, POLICY_YAML).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "not authorized to perform operation. Permission: add-dac-policy"
    );
    node.add_dac_policy(&node_did, POLICY_YAML)
        .await
        .expect("node identity must bypass NAC");

    node.shutdown().await;
    defra_core::signing::clear_identity_store();
}
