use std::ffi::c_char;
use std::sync::Arc;

use crate::ffi_entry;
use acp::nac::NodePermission;
use blockstore::{Blockstore, DefraBlockstore};
use defra_core::Block;
use p2p::sync::MergeHandler;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::{P2PState, NODES};
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

/// Sync collection versions (schema definitions) from connected peers via Bitswap.
///
/// This fetches collection definition blocks by their CIDs (version IDs), recursively
/// fetches previous versions and field definition blocks, then saves them to the
/// database as inactive collection versions.
///
/// Unlike DocSync and BranchableSync (which use PubSub request/reply), this uses
/// Bitswap directly to fetch blocks by CID.
///
/// # Safety
///
/// `identity_did` and `version_ids_json` must be valid null-terminated UTF-8 strings.
/// `version_ids_json` should be a JSON array of CID strings: `["bafyrei...", "bafyrei..."]`
#[no_mangle]
pub unsafe extern "C" fn p2p_sync_collection_versions(
    node_ptr: usize,
    identity_did: *const c_char,
    version_ids_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pSyncCollectionVersions
        ));

        let version_ids_str = try_ffi!(require_c_str(version_ids_json, "version_ids_json"));

        tracing::debug!(version_ids = %version_ids_str, "p2p_sync_collection_versions called");

        // Parse the JSON array of version IDs
        let version_ids: Vec<String> = match serde_json::from_str(&version_ids_str) {
            Ok(ids) => ids,
            Err(e) => return FfiResult::error(format!("failed to parse version_ids_json: {}", e)),
        };

        if version_ids.is_empty() {
            tracing::debug!("no version IDs provided, returning early");
            return FfiResult::ok();
        }

        tracing::debug!(count = version_ids.len(), "parsed version IDs to sync");

        let result = NODES
            .get(node_ptr, |state| {
            let p2p = match &state.p2p {
                Some(p2p) => p2p,
                None => return Err("no p2p system configured".to_string()),
            };
            let db = &state.database;

            rt.block_on(async {
                // Get connected peers to use as providers
                let connected_peers = p2p
                    .handle
                    .connected_peers()
                    .await
                    .map_err(|e| format!("failed to get connected peers: {}", e))?;

                tracing::debug!(count = connected_peers.len(), "connected peers");

                if connected_peers.is_empty() {
                    tracing::debug!("no connected peers, returning early");
                    return Ok(());
                }

                // Process each version ID
                for version_id_str in &version_ids {
                    tracing::debug!(version_id = %version_id_str, "processing version");

                    // Parse CID from version ID string
                    let version_cid = match cid::Cid::try_from(version_id_str.as_str()) {
                        Ok(cid) => cid,
                        Err(e) => {
                            tracing::warn!(version_id = %version_id_str, error = %e, "invalid CID, skipping");
                            continue;
                        }
                    };

                    // Start Bitswap sync for the version CID
                    tracing::debug!(cid = %version_cid, "starting bitswap sync");

                    if let Err(e) = p2p.handle
                        .bitswap_sync(version_cid, connected_peers.clone(), vec![version_cid])
                        .await
                    {
                        tracing::warn!(cid = %version_cid, error = %e, "bitswap sync failed");
                        continue;
                    }

                    // Wait for block to be fetched by polling the blockstore via transaction
                    let timeout = std::time::Duration::from_secs(30);
                    let start = std::time::Instant::now();
                    let mut block_found = false;

                    while start.elapsed() < timeout {
                        // Create a read-only transaction to check blockstore
                        let txn = match db.new_txn(true).await {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to create txn");
                                break;
                            }
                        };

                        let blockstore = match txn.blockstore() {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!(error = %e, "failed to get blockstore");
                                break;
                            }
                        };

                        // Check if block exists
                        let cid_bytes = version_cid.to_bytes();
                        match blockstore.has(&cid_bytes).await {
                            Ok(true) => {
                                block_found = true;
                                tracing::debug!(cid = %version_cid, "block fetched successfully");
                                break;
                            }
                            Ok(false) => {
                                // Not yet, wait and retry
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

                    tracing::debug!("block fetched, extracting linked field blocks");

                    // Read block data from blockstore
                    let block_data = match p2p.blockstore.get(&version_cid).await {
                        Ok(Some(data)) => data,
                        Ok(None) => {
                            tracing::warn!(cid = %version_cid, "block not found in blockstore after fetch");
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(cid = %version_cid, error = %e, "failed to read block");
                            continue;
                        }
                    };

                    // Decode block to extract linked field CIDs
                    let linked_cids = match Block::from_dag_cbor(&block_data) {
                        Ok(block) => {
                            let links = block.all_links();
                            tracing::debug!(count = links.len(), "collection block linked CIDs");
                            links
                        }
                        Err(e) => {
                            tracing::warn!(cid = %version_cid, error = %e, "failed to decode block");
                            vec![]
                        }
                    };

                    // Fetch all linked blocks recursively.
                    // For patched versions, heads point to previous version CIDs which
                    // themselves have linked field blocks that also need fetching.
                    let mut fetch_queue: std::collections::VecDeque<cid::Cid> = linked_cids.into_iter().collect();
                    let mut fetched: std::collections::HashSet<String> = std::collections::HashSet::new();
                    fetched.insert(version_cid.to_string());

                    while let Some(link_cid) = fetch_queue.pop_front() {
                        if fetched.contains(&link_cid.to_string()) {
                            continue;
                        }
                        fetched.insert(link_cid.to_string());

                        // Check if we already have this block
                        let already_present = match p2p.blockstore.get(&link_cid).await {
                            Ok(Some(_)) => true,
                            Ok(None) => false,
                            Err(e) => {
                                tracing::warn!(cid = %link_cid, error = %e, "error checking link");
                                continue;
                            }
                        };

                        if !already_present {
                            tracing::debug!(cid = %link_cid, "fetching linked block");

                            if let Err(e) = p2p.handle
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
                                match p2p.blockstore.get(&link_cid).await {
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

                        tracing::debug!(cid = %link_cid, "linked block available");

                        // If this linked block is a CollectionDefinition (previous version),
                        // also fetch its linked blocks (field definitions)
                        if let Ok(Some(link_data)) = p2p.blockstore.get(&link_cid).await {
                            if let Ok(link_block) = Block::from_dag_cbor(&link_data) {
                                if matches!(&link_block.delta, defra_core::block::CrdtDelta::CollectionDefinition(_)) {
                                    let sub_links = link_block.all_links();
                                    tracing::debug!(cid = %link_cid, count = sub_links.len(), "previous version has sub-links");
                                    for sub_cid in sub_links {
                                        if !fetched.contains(&sub_cid.to_string()) {
                                            fetch_queue.push_back(sub_cid);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    tracing::debug!("all linked blocks fetched, processing through merge handler");

                    // Process through merge handler with recovery metadata
                    // (collection definitions don't have doc_id/collection_id in the traditional sense)
                    let metadata = p2p::sync::BlockMetadata::recovery();

                    match p2p.merge_handler.handle_block(&version_cid, &block_data, metadata).await {
                        Ok(outcome) => {
                            tracing::debug!(cid = %version_cid, ?outcome, "merge handler result");
                        }
                        Err(e) => {
                            tracing::warn!(cid = %version_cid, error = %e, "merge handler error");
                        }
                    }

                    tracing::debug!(version_id = %version_id_str, "successfully synced version");

                    // After merge, check if this is a view with a lens transform to sync.
                    if let Ok(block) = Block::from_dag_cbor(&block_data) {
                        if let defra_core::CrdtDelta::CollectionDefinition(ref payload) = block.delta {
                            if let Some(ref transform_cid) = payload.query_transform {
                                tracing::debug!(transform_cid = %transform_cid, "view has transform, syncing lens");
                                if let Err(e) = sync_lens(transform_cid, p2p, db, &connected_peers).await {
                                    tracing::warn!(error = %e, "lens sync failed");
                                }
                            }
                        }
                    }
                }

                Ok(())
            })
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
        .and_then(|r| r);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(e) => FfiResult::error(e),
        }
    }
}

/// Fetch a single block via Bitswap, polling until available.
async fn fetch_lens_block(
    target_cid: cid::Cid,
    blockstore: &Arc<DefraBlockstore<crate::state::FfiStore>>,
    handle: &p2p::P2PHostHandle,
    peers: &[libp2p::PeerId],
) -> Result<Vec<u8>, String> {
    // Check local blockstore first
    if let Ok(Some(data)) = blockstore.get(&target_cid).await {
        return Ok(data);
    }
    // Fetch via Bitswap
    handle
        .bitswap_sync(target_cid, peers.to_vec(), vec![target_cid])
        .await
        .map_err(|e| format!("bitswap sync for {}: {}", target_cid, e))?;

    // Poll until available
    let timeout = std::time::Duration::from_secs(10);
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
///
/// Follows Go's 3-level block hierarchy: ConfigBlock -> ModuleBlock -> LensBlock.
async fn sync_lens(
    transform_cid: &cid::Cid,
    p2p: &P2PState,
    db: &Arc<crate::state::FfiDatabase>,
    connected_peers: &[libp2p::PeerId],
) -> Result<(), String> {
    use defra_core::{LensConfigBlock, LensModuleBlock, LensWasmBlock};

    // 1. Fetch ConfigBlock
    let config_data = fetch_lens_block(
        *transform_cid,
        &p2p.blockstore,
        &p2p.handle,
        connected_peers,
    )
    .await?;
    let config_block: LensConfigBlock = serde_ipld_dagcbor::from_slice(&config_data)
        .map_err(|e| format!("decode config block: {}", e))?;

    tracing::debug!(
        module_count = config_block.modules.len(),
        "config block decoded"
    );

    let mut lens_modules = Vec::new();

    for module_cid in &config_block.modules {
        // 2. Fetch ModuleBlock
        tracing::debug!(cid = %module_cid, "fetching module block");
        let module_data =
            fetch_lens_block(*module_cid, &p2p.blockstore, &p2p.handle, connected_peers).await?;
        let module_block: LensModuleBlock = serde_ipld_dagcbor::from_slice(&module_data)
            .map_err(|e| format!("decode module block {}: {}", module_cid, e))?;

        // 3. Fetch LensBlock (WASM bytes)
        let lens_data = fetch_lens_block(
            module_block.lens,
            &p2p.blockstore,
            &p2p.handle,
            connected_peers,
        )
        .await?;
        let wasm_block: LensWasmBlock = serde_ipld_dagcbor::from_slice(&lens_data)
            .map_err(|e| format!("decode lens block {}: {}", module_block.lens, e))?;

        let wasm_bytes = match &wasm_block {
            LensWasmBlock::Direct { wasm_bytes } => wasm_bytes.clone(),
            LensWasmBlock::Chunked { chunks } => {
                tracing::debug!(count = chunks.len(), "fetching WASM chunks");
                let mut all_bytes = Vec::new();
                for chunk_cid in chunks {
                    let chunk_data =
                        fetch_lens_block(*chunk_cid, &p2p.blockstore, &p2p.handle, connected_peers)
                            .await?;
                    // Chunks are CBOR-encoded byte strings, decode to get raw bytes
                    let decoded: serde_bytes::ByteBuf = serde_ipld_dagcbor::from_slice(&chunk_data)
                        .map_err(|e| format!("decode WASM chunk {}: {}", chunk_cid, e))?;
                    all_bytes.extend_from_slice(&decoded);
                }
                all_bytes
            }
        };

        tracing::debug!(
            size = wasm_bytes.len(),
            inverse = module_block.inverse,
            "got WASM module"
        );

        let mut lens_mod = lens::LensModule::from_bytes(wasm_bytes);
        lens_mod.inverse = module_block.inverse;

        // Convert arguments from key-value pairs to JSON object
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

    // Build LensConfig and register under the Go CID
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
