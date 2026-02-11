use std::sync::Arc;

use storage::corekv::Store;

use crate::database::DB;

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

    let parsed_cid: cid::Cid = cid_str.parse().map_err(|e| format!("invalid CID: {}", e))?;

    let txn = database
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create transaction: {}", e))?;

    let (block, signature) = {
        let blockstore = txn
            .blockstore()
            .map_err(|e| format!("failed to get blockstore: {}", e))?;

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
            let has_permission = crate::check_doc_permission(
                document_acp,
                caller_identity,
                acp::DocumentPermission::Read,
                collection.schema(),
                &doc_id,
            )
            .await
            .map_err(|e| format!("ACP check failed: {}", e))?;

            if !has_permission {
                let _ = txn.discard();
                return Err("missing permission".to_string());
            }
        }
    }

    // Verify that the identity matches the signature's identity
    let sig_identity = String::from_utf8_lossy(&signature.header.identity);
    if sig_identity.as_ref() != public_key_hex {
        let _ = txn.discard();
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
        let _ = txn.discard();
        return Err("signature verification failed".to_string());
    }

    Ok(())
}
