use std::sync::Arc;

use datastore::NamespaceView;
use storage::corekv::Store;

use crate::database::DB;
use crate::txn::DbTxn;

/// Verify a block's embedded signature and return the signer's DID.
///
/// The public key and algorithm are read from the signed block's signature
/// header. The DID is returned only after the signature has been verified over
/// the canonical DAG-CBOR block bytes.
pub async fn verified_block_signer_did<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    cid_str: &str,
    caller_identity: &acp::Identity,
) -> Result<String, String> {
    let txn = database
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create transaction: {}", e))?;
    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {}", e))?;

    verified_block_signer_did_with_blockstore(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        caller_identity,
    )
    .await
}

/// Load one authorized signed block and its detached signature block as
/// canonical DAG-CBOR bytes. Remote clients use this to verify CID integrity
/// and authorship locally instead of trusting a server-side yes/no verdict.
pub async fn authorized_signed_block_bytes<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    cid_str: &str,
    caller_identity: &acp::Identity,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let txn = database
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create transaction: {}", e))?;
    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {}", e))?;
    let (_, _, block_bytes, signature_bytes) = load_authorized_block_signature(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        caller_identity,
    )
    .await?;
    Ok((block_bytes, signature_bytes))
}

/// Verify a block visible inside an active transaction and return its signer DID.
pub async fn verified_block_signer_did_in_txn<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    txn: &DbTxn<S>,
    cid_str: &str,
    caller_identity: &acp::Identity,
) -> Result<String, String> {
    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {}", e))?;

    verified_block_signer_did_with_blockstore(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        caller_identity,
    )
    .await
}

pub(crate) async fn verified_block_signer_did_with_blockstore<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    blockstore: NamespaceView,
    systemstore: NamespaceView,
    cid_str: &str,
    caller_identity: &acp::Identity,
) -> Result<String, String> {
    let (block, signature, _, _) = load_authorized_block_signature(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        caller_identity,
    )
    .await?;
    verified_signature_signer_did(&block, &signature)
}

fn verified_signature_signer_did(
    block: &defra_core::block::Block,
    signature: &defra_core::block::Signature,
) -> Result<String, String> {
    let signature_identity = std::str::from_utf8(&signature.header.identity)
        .map_err(|e| format!("signature identity is not valid UTF-8: {}", e))?;
    let key_type = match signature.header.sig_type {
        defra_core::block::SignatureType::ES256K => crypto::KeyType::Secp256k1,
        defra_core::block::SignatureType::ES256 => crypto::KeyType::Secp256r1,
        defra_core::block::SignatureType::EdDSA => crypto::KeyType::Ed25519,
        defra_core::block::SignatureType::BLS => crypto::KeyType::Bls12381,
    };
    let public_key = crypto::public_key_from_string(key_type, signature_identity)
        .map_err(|e| format!("invalid signature identity: {}", e))?;

    verify_signature_bytes(block, signature, public_key.as_ref())?;
    public_key
        .did()
        .map_err(|e| format!("failed to derive signer DID: {}", e))
}

/// Verify the signature of a block.
///
/// Loads a block from the blockstore by CID, checks that it has a signature,
/// loads the signature block, and verifies the signature using the provided
/// public key.
///
/// Also checks document-level ACP permission (Read) if the block has a delta
/// with schema_version_id.
pub async fn verify_block_signature<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    cid_str: &str,
    public_key_hex: &str,
    key_type: crypto::KeyType,
    caller_identity: &acp::Identity,
) -> Result<(), String> {
    let pub_key = crypto::public_key_from_string(key_type, public_key_hex)
        .map_err(|e| format!("invalid public key: {}", e))?;

    let txn = database
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create transaction: {}", e))?;
    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {}", e))?;

    verify_block_signature_with_blockstore(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        pub_key.as_ref(),
        public_key_hex,
        caller_identity,
    )
    .await
}

pub async fn verify_block_signature_in_txn<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    txn: &DbTxn<S>,
    cid_str: &str,
    public_key_hex: &str,
    key_type: crypto::KeyType,
    caller_identity: &acp::Identity,
) -> Result<(), String> {
    let pub_key = crypto::public_key_from_string(key_type, public_key_hex)
        .map_err(|e| format!("invalid public key: {}", e))?;

    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("failed to get blockstore: {}", e))?;
    let systemstore = txn
        .systemstore()
        .map_err(|e| format!("failed to get systemstore: {}", e))?;

    verify_block_signature_with_blockstore(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        pub_key.as_ref(),
        public_key_hex,
        caller_identity,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn verify_block_signature_with_blockstore<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    blockstore: NamespaceView,
    systemstore: NamespaceView,
    cid_str: &str,
    pub_key: &dyn crypto::PublicKey,
    public_key_hex: &str,
    caller_identity: &acp::Identity,
) -> Result<(), String> {
    let (block, signature, _, _) = load_authorized_block_signature(
        database,
        document_acp,
        blockstore,
        systemstore,
        cid_str,
        caller_identity,
    )
    .await?;

    let sig_identity = std::str::from_utf8(&signature.header.identity)
        .map_err(|e| format!("signature identity is not valid UTF-8: {}", e))?;
    if sig_identity != public_key_hex {
        return Err("signature was created by a different key".to_string());
    }

    verify_signature_bytes(&block, &signature, pub_key)
}

async fn load_authorized_block_signature<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    blockstore: NamespaceView,
    systemstore: NamespaceView,
    cid_str: &str,
    caller_identity: &acp::Identity,
) -> Result<
    (
        defra_core::block::Block,
        defra_core::block::Signature,
        Vec<u8>,
        Vec<u8>,
    ),
    String,
> {
    let parsed_cid: cid::Cid = cid_str.parse().map_err(|e| format!("invalid CID: {}", e))?;
    let block_bytes = blockstore
        .get(&parsed_cid.to_bytes())
        .await
        .map_err(|e| format!("failed to load block: {}", e))?
        .ok_or_else(|| format!("could not find block: {}", cid_str))?;
    blockstore::verify_block_cid(&parsed_cid, &block_bytes)
        .map_err(|e| format!("block CID verification failed: {}", e))?;

    let block = defra_core::block::Block::from_dag_cbor(&block_bytes)
        .map_err(|e| format!("failed to decode block: {}", e))?;
    let sig_cid = block.signature.ok_or("block has no signature")?;
    let sig_bytes = blockstore
        .get(&sig_cid.to_bytes())
        .await
        .map_err(|e| format!("failed to load signature block: {}", e))?
        .ok_or_else(|| format!("signature block not found: {}", sig_cid))?;
    blockstore::verify_block_cid(&sig_cid, &sig_bytes)
        .map_err(|e| format!("signature block CID verification failed: {}", e))?;
    let signature = defra_core::block::Signature::from_dag_cbor(&sig_bytes)
        .map_err(|e| format!("failed to decode signature block: {}", e))?;

    // Check document/collection-level ACP permission (Read) if the block has
    // collection metadata.
    if let Some(schema_version_id) = block.delta.schema_version_id() {
        if let Some(collection) = database
            .get_collection_by_version_id(schema_version_id)
            .map_err(|e| format!("failed to get collection: {}", e))?
        {
            let collection = collection.schema();
            if let Some(policy) = &collection.policy {
                // Field blocks can be shared across documents, so allow if the
                // caller can read any owner. An ownerless document block is
                // denied; only non-document blocks use collection-level access.
                let owning_doc_ids =
                    crate::docid::map::resolve_block_doc_ids(&systemstore, &parsed_cid, &block)
                        .await
                        .map_err(|e| format!("failed to resolve block owners: {}", e))?
                        .ok_or_else(|| "missing permission".to_string())?;

                let node_did = database.node_did();
                let checker = acp::read_access::DirectChecker {
                    acp: document_acp,
                    identity: caller_identity,
                    node_did: node_did.as_ref(),
                };

                let candidates = if owning_doc_ids.is_empty() {
                    vec![String::new()]
                } else {
                    owning_doc_ids
                };

                let mut has_permission = false;
                for doc_id in &candidates {
                    if acp::read_access::check_doc_read_access(
                        &checker,
                        &policy.id,
                        &policy.resource_name,
                        &collection.collection_id,
                        collection.is_branchable,
                        doc_id,
                    )
                    .await
                    .map_err(|e| format!("ACP check failed: {}", e))?
                    {
                        has_permission = true;
                        break;
                    }
                }

                if !has_permission {
                    return Err("missing permission".to_string());
                }
            }
        }
    }

    Ok((block, signature, block_bytes.to_vec(), sig_bytes.to_vec()))
}

fn verify_signature_bytes(
    block: &defra_core::block::Block,
    signature: &defra_core::block::Signature,
    public_key: &dyn crypto::PublicKey,
) -> Result<(), String> {
    let mut block_to_verify = block.clone();
    block_to_verify.signature = None;
    let signed_bytes = block_to_verify
        .to_dag_cbor()
        .map_err(|e| format!("failed to serialize block for verification: {}", e))?;

    let valid = public_key
        .verify(&signed_bytes, &signature.value)
        .map_err(|e| format!("signature verification error: {}", e))?;

    if !valid {
        return Err("signature verification failed".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crypto::PrivateKey;
    use defra_core::block::{
        Block, CrdtDelta, LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
    };

    use super::verified_signature_signer_did;

    fn test_block() -> Block {
        Block {
            delta: CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority: 1,
                data: b"original".to_vec(),
            }),
            heads: None,
            links: None,
            encryption: None,
            signature: None,
        }
    }

    fn sign_block(block: &Block) -> (Signature, String) {
        let private_key = crypto::generate_ed25519().expect("generate Ed25519 key");
        let public_key = private_key.public_key();
        let signer_did = public_key.did().expect("derive signer DID");
        let signature = Signature::new(
            SignatureHeader::new(
                SignatureType::EdDSA,
                hex::encode(public_key.raw()).into_bytes(),
            ),
            private_key
                .sign(&block.to_dag_cbor().expect("encode block"))
                .expect("sign block"),
        );
        (signature, signer_did)
    }

    #[test]
    fn verified_signer_returns_signer_did() {
        let block = test_block();
        let (signature, signer_did) = sign_block(&block);

        let verified_did =
            verified_signature_signer_did(&block, &signature).expect("valid signature must verify");
        assert_eq!(verified_did, signer_did);
    }

    #[test]
    fn verified_signer_rejects_tampered_block() {
        let block = test_block();
        let (signature, _) = sign_block(&block);
        let mut tampered = block;
        let CrdtDelta::Lww(payload) = &mut tampered.delta else {
            panic!("expected LWW delta");
        };
        payload.data = b"tampered".to_vec();

        let error = verified_signature_signer_did(&tampered, &signature)
            .expect_err("tampered block must fail verification");
        assert!(error.contains("signature verification"), "{error}");
    }

    #[test]
    fn verified_signer_rejects_invalid_identity() {
        let block = test_block();
        let (mut signature, _) = sign_block(&block);
        signature.header.identity = b"not hex".to_vec();

        let error = verified_signature_signer_did(&block, &signature)
            .expect_err("invalid identity must fail verification");
        assert!(error.contains("invalid signature identity"), "{error}");
    }
}
