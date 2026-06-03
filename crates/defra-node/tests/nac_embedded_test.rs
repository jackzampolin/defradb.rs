//! End-to-end proof that an embedded node acts as its own identity for
//! DB-layer NAC enforcement.
//!
//! With NAC enabled, a node whose ambient identity were the wildcard would be
//! denied every operation. This test registers a node identity, enables NAC
//! with that identity as owner, and asserts schema + document operations all
//! succeed because the node operates as itself.

use std::sync::LazyLock;

use crypto::Key;
use defra_core::signing::{SigningConfig, SigningKeyType};
use defra_node::EmbeddedNode;
use identity::{Identity as _, RawIdentity};
use tokio::sync::Mutex;

/// Both tests mutate the process-global signing identity store, so they must
/// not run concurrently (cargo runs tests in a binary in parallel). Without
/// this, one test's `clear_identity_store()` can wipe the other's node
/// identity mid-run, making `execute()` fall back to the unauthenticated path.
static SIGNING_STORE_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Generate a fresh Ed25519 node identity, register it in the process-local
/// signing registry with exportable key bytes, and return its DID.
fn register_local_node_identity() -> String {
    let private_key = crypto::generate_ed25519().expect("generate ed25519 key");
    let identity =
        RawIdentity::from_ed25519(private_key.clone()).expect("build raw identity from key");
    let did = identity.did().expect("derive DID").to_string();

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

#[tokio::test]
async fn embedded_node_acts_as_self_with_nac_enabled() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();
    let did = register_local_node_identity();

    let node = EmbeddedNode::builder()
        .with_node_identity_did(&did)
        .with_node_acp_enabled()
        .build()
        .await
        .expect("node with NAC enabled should build");

    assert_eq!(node.node_identity_did(), Some(did.as_str()));

    // Schema mutation runs as the node identity (CollectionPatch gate).
    node.add_schema("type Widget { name: String }")
        .await
        .expect("add_schema must succeed as node identity");

    // Document create via execute() (DocumentUpdate gate in the mutators).
    let create = node
        .execute(r#"mutation { create_Widget(input: {name: "gadget"}) { _docID name } }"#)
        .await;
    assert!(
        create.errors.is_empty(),
        "create mutation must not be denied: {:?}",
        create.errors
    );

    // Query via execute() (DocumentRead gate).
    let read = node.execute("query { Widget { name } }").await;
    assert!(
        read.errors.is_empty(),
        "query must not be denied: {:?}",
        read.errors
    );
    let data = read.data.expect("query returns data");
    assert!(
        data.to_string().contains("gadget"),
        "query should return the created document: {data}"
    );

    node.shutdown().await;
    defra_core::signing::clear_identity_store();
}

#[tokio::test]
async fn node_acp_requires_node_identity() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();

    let error = match EmbeddedNode::builder()
        .with_node_acp_enabled()
        .build()
        .await
    {
        Ok(_) => panic!("enabling NAC without a node identity must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("node ACP requires a node identity"),
        "{error:#}"
    );
}
