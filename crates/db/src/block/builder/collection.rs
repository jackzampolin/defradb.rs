use super::*;

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
) -> Result<(Cid, Vec<u8>), String> {
    use storage::corekv::IterOptions;

    // Get existing collection head (if any)
    let col_prefix = HeadstoreColKey::collection_prefix(collection_short_id);
    let opts = IterOptions::new()
        .with_prefix(col_prefix)
        .with_commutative_set();

    let mut iter = headstore
        .iterator(opts)
        .await
        .map_err(|e| format!("Failed to create collection headstore iterator: {}", e))?;

    let mut col_heads: Vec<Cid> = Vec::new();
    let mut max_priority: u64 = 0;
    let mut old_head_keys: Vec<Vec<u8>> = Vec::new();

    while let Some(kv_pair) = iter
        .next()
        .await
        .map_err(|e| format!("Failed to iterate collection headstore: {}", e))?
    {
        let priority = decode_priority_varint(&kv_pair.value);
        if priority > max_priority {
            max_priority = priority;
        }
        // Parse CID from key: /c/{collection_id}/{cid}
        let key_str = String::from_utf8_lossy(&kv_pair.key);
        let parts: Vec<&str> = key_str.split('/').collect();
        if let Some(cid_str) = parts.last() {
            if let Ok(cid) = cid_str.parse::<Cid>() {
                col_heads.push(cid);
            }
        }
        old_head_keys.push(kv_pair.key.clone());
    }

    let priority: u64 = max_priority + 1;

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

    // Delete old collection head entries
    for old_key in old_head_keys {
        headstore
            .delete(&old_key)
            .await
            .map_err(|e| format!("Failed to delete old collection head: {}", e))?;
    }

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

    Ok((collection_cid, collection_bytes))
}
