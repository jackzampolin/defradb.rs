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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datastore::SharedTxn;
    use storage::backends::MemoryStore;
    use storage::corekv::{IterOptions, Store};
    use storage::namespace::Namespace;

    use super::*;

    fn test_cid(value: &[u8]) -> Cid {
        generate_cid_from_bytes(value).unwrap()
    }

    fn views(shared: &Arc<SharedTxn>) -> (NamespaceView, NamespaceView) {
        (
            NamespaceView::new(Arc::clone(shared), Namespace::Blockstore),
            NamespaceView::new(Arc::clone(shared), Namespace::Headstore),
        )
    }

    async fn commit(shared: Arc<SharedTxn>) {
        Arc::try_unwrap(shared)
            .ok()
            .expect("transaction views dropped")
            .into_txn()
            .commit()
            .await
            .unwrap();
    }

    async fn collection_heads(store: &MemoryStore, collection_id: u32) -> Vec<Cid> {
        let shared = SharedTxn::new(store.new_txn(true).await.unwrap());
        let (_, headstore) = views(&shared);
        let mut iterator = headstore
            .iterator(
                IterOptions::new().with_prefix(HeadstoreColKey::collection_prefix(collection_id)),
            )
            .await
            .unwrap();
        let mut heads = Vec::new();
        while let Some(pair) = iterator.next().await.unwrap() {
            let cid = String::from_utf8(pair.key)
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            heads.push(cid);
        }
        iterator.close().await.unwrap();
        heads.sort_by_cached_key(Cid::to_string);
        heads
    }

    #[tokio::test]
    async fn concurrent_collection_transitions_preserve_and_merge_sibling_heads() {
        const COLLECTION_ID: u32 = 7;

        let store = MemoryStore::new();
        let seed_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
        let (seed_blocks, seed_heads) = views(&seed_txn);
        write_collection_block(
            &seed_blocks,
            &seed_heads,
            COLLECTION_ID,
            "schema-v1",
            test_cid(b"seed document"),
            None,
        )
        .await
        .unwrap();
        drop((seed_blocks, seed_heads));
        commit(seed_txn).await;

        let first_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
        let second_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
        let (first_blocks, first_heads) = views(&first_txn);
        let (second_blocks, second_heads) = views(&second_txn);

        let (first_cid, _) = write_collection_block(
            &first_blocks,
            &first_heads,
            COLLECTION_ID,
            "schema-v1",
            test_cid(b"first document"),
            None,
        )
        .await
        .unwrap();
        let (second_cid, _) = write_collection_block(
            &second_blocks,
            &second_heads,
            COLLECTION_ID,
            "schema-v1",
            test_cid(b"second document"),
            None,
        )
        .await
        .unwrap();
        drop((first_blocks, first_heads, second_blocks, second_heads));

        commit(first_txn).await;
        commit(second_txn).await;

        let mut expected_siblings = vec![first_cid, second_cid];
        expected_siblings.sort_by_cached_key(Cid::to_string);
        assert_eq!(
            collection_heads(&store, COLLECTION_ID).await,
            expected_siblings
        );

        let merge_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
        let (merge_blocks, merge_heads) = views(&merge_txn);
        let (merged_cid, merged_bytes) = write_collection_block(
            &merge_blocks,
            &merge_heads,
            COLLECTION_ID,
            "schema-v1",
            test_cid(b"merge document"),
            None,
        )
        .await
        .unwrap();
        let merged_block = Block::from_dag_cbor(&merged_bytes).unwrap();
        assert_eq!(merged_block.heads, Some(expected_siblings));
        drop((merge_blocks, merge_heads));
        commit(merge_txn).await;

        assert_eq!(
            collection_heads(&store, COLLECTION_ID).await,
            vec![merged_cid]
        );
    }
}
