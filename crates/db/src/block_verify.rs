use std::sync::Arc;

use datastore::NamespaceView;
use storage::corekv::Store;

use crate::database::DB;
use crate::txn::DbTxn;

/// Verify the signature of a block.
///
/// Loads a block from the blockstore by CID, checks that it has a signature,
/// loads the signature block, and verifies the signature using the provided
/// public key.
///
/// Also checks document-level ACP permission (Read) if the block has a delta
/// with doc_id and schema_version_id.
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

    verify_block_signature_with_blockstore(
        database,
        document_acp,
        blockstore,
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

    verify_block_signature_with_blockstore(
        database,
        document_acp,
        blockstore,
        cid_str,
        pub_key.as_ref(),
        public_key_hex,
        caller_identity,
    )
    .await
}

async fn verify_block_signature_with_blockstore<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    blockstore: NamespaceView,
    cid_str: &str,
    pub_key: &dyn crypto::PublicKey,
    public_key_hex: &str,
    caller_identity: &acp::Identity,
) -> Result<(), String> {
    let parsed_cid: cid::Cid = cid_str.parse().map_err(|e| format!("invalid CID: {}", e))?;
    let (block, signature) = {
        let block_bytes = blockstore
            .get(&parsed_cid.to_bytes())
            .await
            .map_err(|e| format!("failed to load block: {}", e))?
            .ok_or_else(|| format!("could not find block: {}", cid_str))?;

        let block = defra_core::block::Block::from_dag_cbor(&block_bytes)
            .map_err(|e| format!("failed to decode block: {}", e))?;

        let sig_cid = block.signature.ok_or("block has no signature")?;

        let sig_bytes = blockstore
            .get(&sig_cid.to_bytes())
            .await
            .map_err(|e| format!("failed to load signature block: {}", e))?
            .ok_or_else(|| format!("signature block not found: {}", sig_cid))?;

        let signature = defra_core::block::Signature::from_dag_cbor(&sig_bytes)
            .map_err(|e| format!("failed to decode signature block: {}", e))?;

        (block, signature)
    };

    // Check document-level ACP permission (Read) if block has delta with doc_id
    if let (Some(doc_id_bytes), Some(schema_version_id)) =
        (block.delta.doc_id(), block.delta.schema_version_id())
    {
        let doc_id = String::from_utf8_lossy(doc_id_bytes).to_string();

        if let Some(collection) = database
            .get_collection_by_version_id(schema_version_id)
            .map_err(|e| format!("failed to get collection: {}", e))?
        {
            let node_did = database.node_did();
            let has_permission = crate::check_doc_permission(
                document_acp,
                caller_identity,
                acp::DocumentPermission::Read,
                collection.schema(),
                &doc_id,
                node_did.as_ref(),
            )
            .await
            .map_err(|e| format!("ACP check failed: {}", e))?;

            if !has_permission {
                return Err("missing permission".to_string());
            }
        }
    }

    // Verify that the identity matches the signature's identity
    let sig_identity = String::from_utf8_lossy(&signature.header.identity);
    if sig_identity.as_ref() != public_key_hex {
        return Err("signature was created by a different key".to_string());
    }

    // Get the bytes to verify (block serialized without signature)
    let mut block_to_verify = block.clone();
    block_to_verify.signature = None;
    let signed_bytes = block_to_verify
        .to_dag_cbor()
        .map_err(|e| format!("failed to serialize block for verification: {}", e))?;

    let valid = pub_key
        .verify(&signed_bytes, &signature.value)
        .map_err(|e| format!("signature verification error: {}", e))?;

    if !valid {
        return Err("signature verification failed".to_string());
    }

    Ok(())
}
