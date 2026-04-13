use super::*;
use blockstore::{Blockstore, DefraBlockstore};
use crypto::keys::PublicKey;
use defra_core::encryption::EncryptionConfig;
use defra_core::SignatureType;
use std::sync::{Arc, Mutex};
use storage::backends::MemoryStore;

fn make_test_blockstore() -> Arc<DefraBlockstore<MemoryStore>> {
    let store = Arc::new(MemoryStore::new());
    Arc::new(DefraBlockstore::new(store, false))
}

#[tokio::test]
async fn test_build_blocks_creates_proper_structure() {
    let mut doc = Document::new();
    doc.generate_and_set_doc_id().unwrap();
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
async fn test_build_blocks_requires_doc_id() {
    let doc = Document::new();
    let blockstore = make_test_blockstore();

    let result = build_blocks_from_document(&doc, "schema-v1", &blockstore).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must have an ID"));
}

#[tokio::test]
async fn test_field_block_contains_lww_delta() {
    let mut doc = Document::new();
    doc.generate_and_set_doc_id().unwrap();
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
    doc.generate_and_set_doc_id().unwrap();
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
fn test_compute_document_blocks_places_encryption_metadata_in_encstore_entries() {
    let mut doc = Document::new();
    doc.generate_and_set_doc_id().unwrap();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let computed = compute_document_blocks(&doc, "schema-v1", Some(&enc), None)
        .expect("blocks should compute");

    assert!(
        !computed.encstore_entries.is_empty(),
        "encryption metadata should be written to encstore entries"
    );
    assert!(
        computed.blockstore_entries.len() >= 2,
        "field and composite blocks should still be stored in blockstore entries"
    );
}

struct TestRemoteSecp256r1Signer {
    private_key: crypto::Secp256r1PrivateKey,
}

impl defra_core::signing::RemoteSigner for TestRemoteSecp256r1Signer {
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

#[test]
fn test_compute_signature_supports_remote_secp256r1() {
    let private_key = crypto::generate_secp256r1().expect("should generate secp256r1 key");
    let public_key = private_key.public_key();

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc-1".to_vec(),
            schema_version_id: "schema-v1".to_string(),
            status: 1,
            priority: 1,
        }),
        Vec::new(),
        Vec::new(),
    );

    let signer = defra_core::signing::SigningConfig {
        key_type: "secp256r1".to_string(),
        private_key_bytes: Vec::new(),
        public_key_bytes: public_key.raw(),
        public_key_hex: hex::encode(public_key.raw()),
        remote_signer: Some(Arc::new(TestRemoteSecp256r1Signer { private_key })),
        signing_authorization: None,
    };

    let (sig_cid, sig_cbor) = compute_signature(&block, &signer)
        .expect("signature should succeed")
        .expect("composite block should be signed");
    assert!(!sig_cid.to_bytes().is_empty());

    let signature = Signature::from_dag_cbor(&sig_cbor).expect("signature block should decode");
    assert_eq!(signature.header.sig_type, SignatureType::ES256);
    assert_eq!(
        signature.header.identity,
        signer.public_key_hex.as_bytes().to_vec()
    );

    let public_key = crypto::Secp256r1PublicKey::from_bytes(&signer.public_key_bytes)
        .expect("public key should decode");
    let signed_bytes = block.to_dag_cbor().expect("block should encode");
    let valid = public_key
        .verify(&signed_bytes, &signature.value)
        .expect("verification should succeed");
    assert!(valid, "remote secp256r1 signature should verify");
}

struct CapturingRemoteSigner {
    private_key: crypto::Secp256r1PrivateKey,
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
    let private_key = crypto::generate_secp256r1().expect("should generate secp256r1 key");
    let public_key = private_key.public_key();
    let seen_authorization = Arc::new(Mutex::new(None));

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc-2".to_vec(),
            schema_version_id: "schema-v1".to_string(),
            status: 1,
            priority: 1,
        }),
        Vec::new(),
        Vec::new(),
    );

    let signer = defra_core::signing::SigningConfig {
        key_type: "secp256r1".to_string(),
        private_key_bytes: Vec::new(),
        public_key_bytes: public_key.raw(),
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
