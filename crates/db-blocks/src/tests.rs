use super::*;
use blockstore::{Blockstore, DefraBlockstore};
use crypto::keys::Key;
use defra_core::encryption::EncryptionConfig;
use std::sync::{Arc, Mutex};
use storage::backends::MemoryStore;

fn make_test_blockstore() -> Arc<DefraBlockstore<MemoryStore>> {
    let store = Arc::new(MemoryStore::new());
    Arc::new(DefraBlockstore::new(store, false))
}

#[tokio::test]
async fn test_build_blocks_creates_proper_structure() {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));

    let blockstore = make_test_blockstore();
    let schema_version_id = "bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq";

    let result = build_blocks_from_document(&doc, schema_version_id, &blockstore)
        .await
        .unwrap();

    // Should have created 2 field blocks (name, age)
    assert_eq!(result.field_cids.len(), 2);
    assert!(!result.doc_id.is_empty());

    // Composite block should be in blockstore
    let stored = blockstore.get(&result.cid).await.unwrap();
    assert!(stored.is_some());

    // Each field block should be in blockstore
    for field_cid in &result.field_cids {
        let stored = blockstore.get(field_cid).await.unwrap();
        assert!(stored.is_some());
    }
}

#[tokio::test]
async fn test_build_blocks_derives_doc_id_from_genesis_cid() {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Dana".to_string()));
    let blockstore = make_test_blockstore();

    let result = build_blocks_from_document(&doc, "schema-v1", &blockstore)
        .await
        .unwrap();
    assert_eq!(result.doc_id, derive_doc_id(&result.cid));
    assert!(result.doc_id.starts_with("bae-"));
}

#[tokio::test]
async fn test_field_block_contains_lww_delta() {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Bob".to_string()));

    let blockstore = make_test_blockstore();
    let schema_version_id = "schema-v1";

    let result = build_blocks_from_document(&doc, schema_version_id, &blockstore)
        .await
        .unwrap();

    // Get the field block
    let field_cid = &result.field_cids[0];
    let field_bytes = blockstore.get(field_cid).await.unwrap().unwrap();

    // Decode and verify it's an LWW block
    let field_block = Block::from_dag_cbor(&field_bytes).unwrap();
    match &field_block.delta {
        CrdtDelta::Lww(payload) => {
            assert_eq!(payload.field_name, "name");
            assert_eq!(payload.schema_version_id, schema_version_id);
            assert_eq!(payload.priority, 1);
        }
        _ => panic!("Expected LWW delta"),
    }
}

#[tokio::test]
async fn test_composite_block_has_field_links() {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Charlie".to_string()));
    doc.set("age", NormalValue::Int(25));

    let blockstore = make_test_blockstore();

    let result = build_blocks_from_document(&doc, "schema-v1", &blockstore)
        .await
        .unwrap();

    // Decode the composite block
    let composite_block = Block::from_dag_cbor(&result.block).unwrap();

    // Verify it's a Composite delta
    match &composite_block.delta {
        CrdtDelta::Composite(payload) => {
            assert_eq!(payload.status, 1); // Active
            assert_eq!(payload.priority, 1);
        }
        _ => panic!("Expected Composite delta"),
    }

    // Verify links to field blocks
    let links = composite_block.links.as_ref().expect("Should have links");
    assert_eq!(links.len(), 2);

    // Links should reference field CIDs
    let link_cids: Vec<Cid> = links.iter().map(|l| l.link).collect();
    for field_cid in &result.field_cids {
        assert!(link_cids.contains(field_cid));
    }
}

#[test]
fn test_compute_document_blocks_places_encryption_metadata_in_blockstore_entries() {
    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let computed = compute_document_blocks(
        &doc,
        "schema-v1",
        DocStorageIdentity::new(1, 1),
        Some(&enc),
        None,
    )
    .expect("blocks should compute");

    assert!(
        computed.blockstore_entries.len() >= 3,
        "encryption metadata should be included in blockstore entries alongside field and composite blocks"
    );
}

struct LocalSecp256r1Signer {
    private_key: crypto::Secp256r1PrivateKey,
}

impl defra_core::signing::RemoteSigner for LocalSecp256r1Signer {
    fn sign_sync(
        &self,
        data: &[u8],
        _authorization: Option<&defra_core::signing::SigningAuthorization>,
    ) -> Result<Vec<u8>, String> {
        self.private_key
            .sign(data)
            .map_err(|error| format!("remote sign failed: {}", error))
    }
}

// Go's internal/core/block/signing.go:74-79 rejects any key type other than
// secp256k1 or Ed25519 with ErrUnsupportedKeyForSigning. Rust must match so
// that a Rust node cannot produce blocks a Go node will refuse to verify.
#[test]
fn test_compute_signature_rejects_local_secp256r1_block_signing() {
    let private_key = crypto::generate_secp256r1().expect("should generate secp256r1 key");
    let public_key = private_key.public_key();

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema-v1".to_string(),
            status: 1,
            priority: 1,
        }),
        Vec::new(),
        Vec::new(),
    );

    let signer = defra_core::signing::SigningConfig {
        key_type: defra_core::signing::SigningKeyType::Secp256r1,
        private_key_bytes: defra_core::signing::SigningConfig::private_key_bytes_from_slice(
            private_key.raw(),
        ),
        public_key_bytes: public_key.raw_owned(),
        public_key_hex: hex::encode(public_key.raw()),
        remote_signer: None,
        signing_authorization: None,
    };

    let err = compute_signature(&block, &signer)
        .expect_err("secp256r1 block signing must be rejected for Go parity");
    assert!(
        err.contains("secp256r1") || err.contains("ES256") || err.contains("secp256k1"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_compute_signature_rejects_remote_secp256r1_block_signing() {
    let private_key = crypto::generate_secp256r1().expect("should generate secp256r1 key");
    let public_key = private_key.public_key();

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema-v1".to_string(),
            status: 1,
            priority: 1,
        }),
        Vec::new(),
        Vec::new(),
    );

    let signer = defra_core::signing::SigningConfig {
        key_type: defra_core::signing::SigningKeyType::Secp256r1,
        private_key_bytes: Vec::new(),
        public_key_bytes: public_key.raw_owned(),
        public_key_hex: hex::encode(public_key.raw()),
        remote_signer: Some(Arc::new(LocalSecp256r1Signer { private_key })),
        signing_authorization: None,
    };

    let err = compute_signature(&block, &signer)
        .expect_err("secp256r1 block signing must be rejected even when delegated to remote");
    assert!(
        err.contains("secp256r1") || err.contains("ES256") || err.contains("secp256k1"),
        "unexpected error: {err}"
    );
}

struct CapturingRemoteSigner {
    private_key: crypto::Ed25519PrivateKey,
    seen_authorization: Arc<Mutex<Option<defra_core::signing::SigningAuthorization>>>,
}

impl defra_core::signing::RemoteSigner for CapturingRemoteSigner {
    fn sign_sync(
        &self,
        data: &[u8],
        authorization: Option<&defra_core::signing::SigningAuthorization>,
    ) -> Result<Vec<u8>, String> {
        *self.seen_authorization.lock().expect("lock") = authorization.cloned();
        self.private_key
            .sign(data)
            .map_err(|error| format!("remote sign failed: {}", error))
    }
}

#[test]
fn test_compute_signature_passes_signing_authorization_to_remote_signer() {
    let private_key = crypto::generate_ed25519().expect("should generate ed25519 key");
    let public_key = private_key.public_key();
    let seen_authorization = Arc::new(Mutex::new(None));

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema-v1".to_string(),
            status: 1,
            priority: 1,
        }),
        Vec::new(),
        Vec::new(),
    );

    let signer = defra_core::signing::SigningConfig {
        key_type: defra_core::signing::SigningKeyType::Ed25519,
        private_key_bytes: Vec::new(),
        public_key_bytes: public_key.raw_owned(),
        public_key_hex: hex::encode(public_key.raw()),
        remote_signer: Some(Arc::new(CapturingRemoteSigner {
            private_key,
            seen_authorization: seen_authorization.clone(),
        })),
        signing_authorization: Some(defra_core::signing::SigningAuthorization::Policy {
            policy_id: "policy-1".to_string(),
            resource: "transcript".to_string(),
            object_id: "transcript".to_string(),
            permission: "writer".to_string(),
        }),
    };

    compute_signature(&block, &signer)
        .expect("signature should succeed")
        .expect("composite block should be signed");

    assert_eq!(
        *seen_authorization.lock().expect("lock"),
        signer.signing_authorization
    );
}
