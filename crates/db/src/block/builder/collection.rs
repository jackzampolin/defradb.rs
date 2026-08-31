use super::*;
use bytes::Bytes;

/// Write a collection-level block for branchable collections.
///
/// Branchable collections maintain a separate DAG at the collection level,
/// tracking all document operations. Each mutation creates a collection block
/// that links to the document's composite block.
///
/// This matches Go's `crdt.NewCollection()` → `coreblock.AddDelta()` flow.
pub async fn write_collection_block(
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    collection_short_id: u32,
    schema_version_id: &str,
    doc_composite_cid: Cid,
    signing_config: Option<&SigningConfig>,
) -> Result<(Cid, Bytes), String> {
    let found = crate::block::heads::live_collection_heads(headstore, collection_short_id)
        .await
        .map_err(|e| format!("Failed to read collection heads: {}", e))?;
    let mut col_heads = found.live;

    let priority: u64 = found.max_priority + 1;

    // Sort heads by CID string representation to match Go's Block.New() sorting.
    col_heads.sort_by_cached_key(|a| a.to_string());

    // Create the Collection delta payload
    let collection_payload = CollectionDeltaPayload {
        schema_version_id: schema_version_id.to_string(),
        priority,
    };

    // The collection block links to the document composite block
    // Go uses empty string for link name here, fieldName comes from linked block's delta
    let links = vec![DAGLink::new(String::new(), doc_composite_cid)];

    // Kept because `Block::new` takes the heads by value and the markers below
    // need to name each one. A collection's head set is one entry in the common
    // case and a handful after a merge, so this is not the copy to worry about.
    let superseded_parents = col_heads.clone();

    // Create the collection block
    let mut collection_block =
        Block::new(CrdtDelta::Collection(collection_payload), col_heads, links);

    // Sign the collection block if a signer is available
    if let Some(signer) = signing_config {
        if let Some(sig_cid) = sign_block(&collection_block, signer, blockstore).await? {
            collection_block.signature = Some(sig_cid);
        }
    }

    // Serialize and generate CID
    let collection_bytes = collection_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode collection block: {}", e))?;
    let collection_cid = generate_cid_from_bytes(&collection_bytes)
        .map_err(|e| format!("Failed to generate collection CID: {}", e))?;

    // Store the collection block in blockstore
    blockstore
        .set(&collection_cid.to_bytes(), &collection_bytes)
        .await
        .map_err(|e| format!("Failed to store collection block: {}", e))?;

    // Record that this block superseded each head it was built on, rather than
    // deleting those heads.
    //
    // Every key written here ends in `collection_cid`, this writer's own block,
    // so two writers appending concurrently never write the same key and the
    // engine has nothing to reject. Deleting the parents instead makes both
    // writers write the parent's key, which is a write-write conflict that
    // regolith refuses at every isolation level. See `proofs/tla/HeadSet.tla`
    // (`MC_HeadSet_Red_EagerDelete.cfg` is that defect) and
    // `HeadSet.applyDerived_parents_not_head` for why this reaches the same
    // head set without the shared write.
    crate::block::heads::record_supersedes(
        headstore,
        collection_short_id,
        &superseded_parents,
        collection_cid,
    )
    .await
    .map_err(|e| format!("Failed to record superseded collection head: {}", e))?;

    // Write new collection head: /c/{collection_id}/{cid} → priority
    let col_head_key = HeadstoreColKey::new(collection_short_id, collection_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore
        .set(&col_head_key.bytes(), &priority_bytes)
        .await
        .map_err(|e| format!("Failed to write collection head: {}", e))?;

    tracing::debug!(
        collection_id = collection_short_id,
        cid = %collection_cid,
        priority = priority,
        doc_composite_cid = %doc_composite_cid,
        "Built collection block for branchable collection"
    );

    // Collect collection CID for batch signing if a session is active.
    if let Some(session_key) = defra_core::batch_signing::get_batch_session_key() {
        defra_core::batch_signing::batch_collect_cid(&session_key, collection_cid);
    }

    Ok((collection_cid, collection_bytes.into()))
}
