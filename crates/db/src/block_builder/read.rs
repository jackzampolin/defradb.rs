use super::*;

/// Read the latest composite block for a document from the committed store.
///
/// After a mutation (create/update/delete), the composite block and its CID
/// are written to the blockstore and headstore as part of the transaction.
/// This function reads the committed composite head CID from the headstore,
/// then fetches the corresponding block bytes from the blockstore.
///
/// Use this instead of `build_blocks_from_document` for P2P broadcast after
/// updates/deletes, since `build_blocks_from_document` recreates blocks from
/// scratch with wrong priority/heads.
pub async fn read_latest_composite_block<S: storage::corekv::Store>(
    db: &crate::database::DB<S>,
    doc_id: &str,
) -> Result<BlockResult, String> {
    let txn = db
        .new_txn(true)
        .await
        .map_err(|e| format!("Failed to create read txn: {}", e))?;

    let headstore = txn
        .headstore()
        .map_err(|e| format!("Failed to get headstore: {}", e))?;

    let composite_heads = get_all_field_heads(&headstore, doc_id, "C").await?;

    let composite_cid = composite_heads
        .first()
        .map(|entry| entry.cid)
        .ok_or_else(|| format!("No composite head found for doc {}", doc_id))?;

    let blockstore = txn
        .blockstore()
        .map_err(|e| format!("Failed to get blockstore: {}", e))?;

    let block_bytes = blockstore
        .get(&composite_cid.to_bytes())
        .await
        .map_err(|e| format!("Failed to read composite block: {}", e))?
        .ok_or_else(|| format!("Composite block not found for CID {}", composite_cid))?;

    let _ = txn.force_discard();

    Ok(BlockResult {
        cid: composite_cid,
        block: block_bytes,
        doc_id: doc_id.to_string(),
        field_cids: vec![],
    })
}
