//! Collection version sync via Bitswap.
//!
//! Ports the FFI `version_sync.rs` logic: fetches collection definition blocks
//! by CID, recursively fetches linked field/previous-version blocks, processes
//! through the merge handler, and syncs lens transforms when present.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::p2p_adapter::VersionSyncer;
use p2p::P2PHostHandle;

/// Database-backed version syncer that fetches schema blocks via Bitswap.
pub struct DbVersionSyncer<S: storage::corekv::Store, B: Blockstore> {
    blockstore: Arc<B>,
    merge_handler: Arc<db::DbMergeHandler<S, B>>,
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + 'static, B: Blockstore + 'static> DbVersionSyncer<S, B> {
    pub fn new(
        blockstore: Arc<B>,
        merge_handler: Arc<db::DbMergeHandler<S, B>>,
        db: Arc<db::DB<S>>,
    ) -> Self {
        Self {
            blockstore,
            merge_handler,
            db,
        }
    }

    pub fn new_arc(
        blockstore: Arc<B>,
        merge_handler: Arc<db::DbMergeHandler<S, B>>,
        db: Arc<db::DB<S>>,
    ) -> Arc<dyn VersionSyncer> {
        Arc::new(Self::new(blockstore, merge_handler, db))
    }
}

/// Fetch a single block via Bitswap, polling until available.
async fn fetch_block<B: Blockstore>(
    target_cid: cid::Cid,
    blockstore: &Arc<B>,
    handle: &P2PHostHandle,
    peers: &[libp2p::PeerId],
    timeout_secs: u64,
) -> Result<Vec<u8>, String> {
    // Check local blockstore first
    if let Ok(Some(data)) = blockstore.get(&target_cid).await {
        return Ok(data);
    }

    handle
        .bitswap_sync(target_cid, peers.to_vec(), vec![target_cid])
        .await
        .map_err(|e| format!("bitswap sync for {}: {}", target_cid, e))?;

    let timeout = std::time::Duration::from_secs(timeout_secs);
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(Some(data)) = blockstore.get(&target_cid).await {
            return Ok(data);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(format!("timeout fetching block {}", target_cid))
}

/// Fetch lens IPLD blocks via Bitswap and register the WASM module.
async fn sync_lens<S: storage::corekv::Store + 'static, B: Blockstore>(
    transform_cid: &cid::Cid,
    blockstore: &Arc<B>,
    handle: &P2PHostHandle,
    db: &Arc<db::DB<S>>,
    connected_peers: &[libp2p::PeerId],
) -> Result<(), String> {
    use defra_core::{LensConfigBlock, LensModuleBlock, LensWasmBlock};

    let config_data = fetch_block(*transform_cid, blockstore, handle, connected_peers, 10).await?;
    let config_block: LensConfigBlock = serde_ipld_dagcbor::from_slice(&config_data)
        .map_err(|e| format!("decode config block: {}", e))?;

    let mut lens_modules = Vec::new();

    for module_cid in &config_block.modules {
        let module_data = fetch_block(*module_cid, blockstore, handle, connected_peers, 10).await?;
        let module_block: LensModuleBlock = serde_ipld_dagcbor::from_slice(&module_data)
            .map_err(|e| format!("decode module block {}: {}", module_cid, e))?;

        let lens_data =
            fetch_block(module_block.lens, blockstore, handle, connected_peers, 10).await?;
        let wasm_block: LensWasmBlock = serde_ipld_dagcbor::from_slice(&lens_data)
            .map_err(|e| format!("decode lens block {}: {}", module_block.lens, e))?;

        let wasm_bytes = match &wasm_block {
            LensWasmBlock::Direct { wasm_bytes } => wasm_bytes.clone(),
            LensWasmBlock::Chunked { chunks } => {
                let mut all_bytes = Vec::new();
                for chunk_cid in chunks {
                    let chunk_data =
                        fetch_block(*chunk_cid, blockstore, handle, connected_peers, 10).await?;
                    let decoded: serde_bytes::ByteBuf = serde_ipld_dagcbor::from_slice(&chunk_data)
                        .map_err(|e| format!("decode WASM chunk {}: {}", chunk_cid, e))?;
                    all_bytes.extend_from_slice(&decoded);
                }
                all_bytes
            }
        };

        let mut lens_mod = lens::LensModule::from_bytes(wasm_bytes);
        lens_mod.inverse = module_block.inverse;

        if !module_block.arguments.is_empty() {
            let args_map: serde_json::Map<String, serde_json::Value> = module_block
                .arguments
                .iter()
                .map(|kv| (kv.key.clone(), serde_json::Value::String(kv.value.clone())))
                .collect();
            lens_mod.arguments = Some(serde_json::Value::Object(args_map));
        }

        lens_modules.push(lens_mod);
    }

    let config = lens::LensConfig {
        source_schema_version_id: String::new(),
        destination_schema_version_id: String::new(),
        lenses: lens_modules,
    };

    let transform_id = lens::TransformId::new(transform_cid.to_string());
    db.lens_store()
        .add_with_id(transform_id, config)
        .await
        .map_err(|e| format!("register lens: {}", e))?;

    tracing::debug!(cid = %transform_cid, "lens registered");
    Ok(())
}

#[async_trait]
impl<S: storage::corekv::Store + 'static, B: Blockstore + 'static> VersionSyncer
    for DbVersionSyncer<S, B>
{
    async fn sync_versions(
        &self,
        handle: &P2PHostHandle,
        version_ids: Vec<String>,
        connected_peers: Vec<libp2p::PeerId>,
    ) -> Result<(), String> {
        use p2p::sync::MergeHandler;

        for version_id_str in &version_ids {
            let version_cid = cid::Cid::try_from(version_id_str.as_str())
                .map_err(|e| format!("invalid cid: {}", e))?;

            // Start Bitswap sync for the version CID
            if let Err(e) = handle
                .bitswap_sync(version_cid, connected_peers.clone(), vec![version_cid])
                .await
            {
                tracing::warn!(cid = %version_cid, error = %e, "bitswap sync failed");
                continue;
            }

            // Poll blockstore via transaction until block arrives
            let timeout = std::time::Duration::from_secs(30);
            let start = std::time::Instant::now();
            let mut block_found = false;

            while start.elapsed() < timeout {
                let txn = match self.db.new_txn(true).await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to create txn");
                        break;
                    }
                };

                let bs = match txn.blockstore() {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to get blockstore");
                        break;
                    }
                };

                match bs.has(&version_cid.to_bytes()).await {
                    Ok(true) => {
                        block_found = true;
                        break;
                    }
                    Ok(false) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "blockstore check failed");
                        break;
                    }
                }
            }

            if !block_found {
                tracing::warn!(cid = %version_cid, "timeout waiting for block");
                continue;
            }

            // Read block data from blockstore
            let block_data = match self.blockstore.get(&version_cid).await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    tracing::warn!(cid = %version_cid, "block not found after fetch");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(cid = %version_cid, error = %e, "failed to read block");
                    continue;
                }
            };

            // Decode block to extract linked CIDs
            let linked_cids = match defra_core::Block::from_dag_cbor(&block_data) {
                Ok(block) => block.all_links(),
                Err(e) => {
                    tracing::warn!(cid = %version_cid, error = %e, "failed to decode block");
                    vec![]
                }
            };

            // BFS fetch of all linked blocks (field definitions, previous versions)
            let mut fetch_queue: VecDeque<cid::Cid> = linked_cids.into_iter().collect();
            let mut fetched: HashSet<String> = HashSet::new();
            fetched.insert(version_cid.to_string());

            while let Some(link_cid) = fetch_queue.pop_front() {
                if fetched.contains(&link_cid.to_string()) {
                    continue;
                }
                fetched.insert(link_cid.to_string());

                let already_present = match self.blockstore.get(&link_cid).await {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(e) => {
                        tracing::warn!(cid = %link_cid, error = %e, "error checking link");
                        continue;
                    }
                };

                if !already_present {
                    if let Err(e) = handle
                        .bitswap_sync(link_cid, connected_peers.clone(), vec![link_cid])
                        .await
                    {
                        tracing::warn!(cid = %link_cid, error = %e, "bitswap sync failed for link");
                        continue;
                    }

                    let link_timeout = std::time::Duration::from_secs(10);
                    let link_start = std::time::Instant::now();
                    let mut link_found = false;

                    while link_start.elapsed() < link_timeout {
                        match self.blockstore.get(&link_cid).await {
                            Ok(Some(_)) => {
                                link_found = true;
                                break;
                            }
                            Ok(None) => {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                tracing::warn!(cid = %link_cid, error = %e, "error waiting for link");
                                break;
                            }
                        }
                    }

                    if !link_found {
                        tracing::warn!(cid = %link_cid, "timeout waiting for linked block");
                        continue;
                    }
                }

                // If linked block is a CollectionDefinition, also fetch its sub-links
                if let Ok(Some(link_data)) = self.blockstore.get(&link_cid).await {
                    if let Ok(link_block) = defra_core::Block::from_dag_cbor(&link_data) {
                        if matches!(
                            &link_block.delta,
                            defra_core::CrdtDelta::CollectionDefinition(_)
                        ) {
                            for sub_cid in link_block.all_links() {
                                if !fetched.contains(&sub_cid.to_string()) {
                                    fetch_queue.push_back(sub_cid);
                                }
                            }
                        }
                    }
                }
            }

            // Process through merge handler as a schema block.
            // CollectionDefinition blocks are not document-level operations and are
            // governed by NAC (already checked at the call site), not document ACP.
            let metadata = p2p::sync::BlockMetadata::schema_sync();
            match self
                .merge_handler
                .handle_block(&version_cid, &block_data, metadata)
                .await
            {
                Ok(outcome) => {
                    tracing::debug!(cid = %version_cid, ?outcome, "merge handler result");
                }
                Err(e) => {
                    tracing::warn!(cid = %version_cid, error = %e, "merge handler error");
                }
            }

            // Check for lens transform to sync
            if let Ok(block) = defra_core::Block::from_dag_cbor(&block_data) {
                if let defra_core::CrdtDelta::CollectionDefinition(ref payload) = block.delta {
                    if let Some(ref transform_cid) = payload.query_transform {
                        if let Err(e) = sync_lens(
                            transform_cid,
                            &self.blockstore,
                            handle,
                            &self.db,
                            &connected_peers,
                        )
                        .await
                        {
                            tracing::warn!(error = %e, "lens sync failed");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
