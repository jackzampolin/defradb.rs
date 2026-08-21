//! KMS interaction on the write path: key generation and inherited-key
//! resolution when a `KmsService` is configured.
//!
//! Split out of `write.rs` to keep that file within the repo's size guidance.

use async_trait::async_trait;
use cid::Cid;
use datastore::NamespaceView;
use datastore::SharedTxn;
use db::block::builder::*;
use defra_core::block::generate_cid_from_bytes;
use defra_core::block::Block;
use defra_core::encryption::EncryptionConfig;
use document::Document;
use document::NormalValue;
use std::sync::Arc;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::namespace::Namespace;

/// Stub KMS that mirrors the real `MemoryKeyStore::generate` block shape so
/// the returned CID matches what the legacy path would produce for the same
/// scope. The key is fixed to make the assertion deterministic.
///
/// Like `MemoryKeyStore`, it keeps the `Encryption` block to itself rather
/// than writing it into the encstore or blockstore — the write path must
/// come back through `get_keys` to resolve it.
struct StubKms;

#[async_trait]
impl kms::KmsService for StubKms {
    async fn get_keys(
        &self,
        _: &kms::RequestContext,
        cids: &[kms::EncryptionCid],
    ) -> kms::Result<kms::KeyResults> {
        let (r, tx) = kms::KeyResults::new(cids.len());
        for cid in cids {
            if *cid == expected_field_cid() {
                let _ = tx.send(Ok((*cid, [5u8; 32]))).await;
            }
        }
        drop(tx);
        Ok(r)
    }

    async fn generate_key(
        &self,
        _: &kms::RequestContext,
        scope: kms::KeyScope,
    ) -> kms::Result<(kms::EncryptionCid, [u8; 32])> {
        let (doc_id_bytes, field_name) = match scope {
            kms::KeyScope::Document { doc_id, field } => (doc_id.into_bytes(), field),
            kms::KeyScope::Collection { collection_id } => (Vec::new(), Some(collection_id)),
        };
        let _ = (doc_id_bytes, field_name);
        let block = defra_core::Encryption { key: vec![5u8; 32] };
        let bytes = block.to_dag_cbor().unwrap();
        let cid = defra_core::block::generate_cid_from_bytes(&bytes).unwrap();
        Ok((cid, [5u8; 32]))
    }

    async fn serve_request(
        &self,
        _: kms::PeerIdentity,
        _: kms::FetchEncryptionKeyRequest,
    ) -> kms::Result<kms::FetchEncryptionKeyReply> {
        Err(kms::Error::Unsupported("stub"))
    }
}

/// Recompute the CID the stub KMS would return, so the test asserts
/// against the KMS-derived CID rather than a hardcoded value.
fn expected_field_cid() -> Cid {
    let block = defra_core::Encryption { key: vec![5u8; 32] };
    let bytes = block.to_dag_cbor().unwrap();
    generate_cid_from_bytes(&bytes).unwrap()
}

#[tokio::test]
async fn kms_write_path_links_field_block_to_kms_encryption_cid() {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared.clone(), Namespace::Headstore);

    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let kms: Arc<dyn kms::KmsService> = Arc::new(StubKms);

    let result = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        DocStorageIdentity::new(1, 1),
        None,
        Some(&enc),
        None,
        Some(&kms),
    )
    .await
    .expect("KMS write path should succeed");

    assert_eq!(result.field_cids.len(), 1);
    let field_cid = result.field_cids[0];
    let field_bytes = blockstore
        .get(&field_cid.to_bytes())
        .await
        .unwrap()
        .expect("field block stored");
    let field_block = Block::from_dag_cbor(&field_bytes).unwrap();

    let expected = expected_field_cid();
    assert_eq!(
        field_block.encryption,
        Some(expected),
        "field block must link to the KMS-generated encryption CID"
    );
}

/// Inheritance must resolve the previous block's key through the KMS when
/// one is configured. A `KeyStore` is only promised local persistence, not
/// persistence into the encstore or blockstore, so reading those alone
/// fails an update that carries no config of its own.
#[tokio::test]
async fn inheritance_resolves_the_key_through_the_kms() {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared.clone(), Namespace::Headstore);
    let identity = DocStorageIdentity::new(1, 1);

    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let kms: Arc<dyn kms::KmsService> = Arc::new(StubKms);

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        Some(&kms),
    )
    .await
    .expect("KMS create should succeed");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set(
        "secret",
        NormalValue::String("still classified".to_string()),
    );
    let modified: std::collections::HashSet<String> = ["secret".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        Some(&kms),
    )
    .await
    .expect("update must resolve the inherited key through the KMS");

    let field_bytes = blockstore
        .get(&updated.field_cids[0].to_bytes())
        .await
        .unwrap()
        .expect("field block stored");
    assert_eq!(
        Block::from_dag_cbor(&field_bytes).unwrap().encryption,
        Some(expected_field_cid()),
        "update must inherit the KMS-generated encryption link"
    );
}

/// The document-level policy fallback must mint through the KMS as well. A
/// field introduced by an update on an `encrypt_doc` document has no head to
/// inherit from, so it takes the generate path — and on a KMS-configured node
/// that key belongs in the KMS, not in an inline block.
#[tokio::test]
async fn document_policy_fallback_mints_through_the_kms() {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared.clone(), Namespace::Headstore);
    let identity = DocStorageIdentity::new(2, 2);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: true,
        encrypt_fields: vec![],
    };

    let kms: Arc<dyn kms::KmsService> = Arc::new(StubKms);

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        Some(&kms),
    )
    .await
    .expect("KMS create should succeed");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set("bio", NormalValue::String("ssn 123-45-6789".to_string()));
    let modified: std::collections::HashSet<String> = ["bio".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        Some(&kms),
    )
    .await
    .expect("update should succeed");

    let bio_bytes = blockstore
        .get(&updated.field_cids[0].to_bytes())
        .await
        .unwrap()
        .expect("field block stored");
    assert_eq!(
        Block::from_dag_cbor(&bio_bytes).unwrap().encryption,
        Some(expected_field_cid()),
        "the new field's key must come from the KMS, not from an inline block"
    );
}
