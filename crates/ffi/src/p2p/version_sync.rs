use std::ffi::c_char;
use std::sync::Arc;

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
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::P2pCollectionCreate
    ));

    let version_ids_str = try_ffi!(require_c_str(version_ids_json, "version_ids_json"));

    eprintln!(
        "[FFI-COLLECTION-VERSION] p2p_sync_collection_versions called with version_ids={}",
        version_ids_str
    );

    // Parse the JSON array of version IDs
    let version_ids: Vec<String> = match serde_json::from_str(&version_ids_str) {
        Ok(ids) => ids,
        Err(e) => return FfiResult::error(format!("failed to parse version_ids_json: {}", e)),
    };

    if version_ids.is_empty() {
        eprintln!("[FFI-COLLECTION-VERSION] No version IDs provided, returning early");
        return FfiResult::ok();
    }

    eprintln!(
        "[FFI-COLLECTION-VERSION] Parsed {} version IDs to sync",
        version_ids.len()
    );

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

                eprintln!(
                    "[FFI-COLLECTION-VERSION] Connected peers: {}",
                    connected_peers.len()
                );

                if connected_peers.is_empty() {
                    eprintln!("[FFI-COLLECTION-VERSION] No connected peers, returning early");
                    return Ok(());
                }

                // Process each version ID
                for version_id_str in &version_ids {
                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Processing version_id={}",
                        version_id_str
                    );

                    // Parse CID from version ID string
                    let version_cid = match cid::Cid::try_from(version_id_str.as_str()) {
                        Ok(cid) => cid,
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Invalid CID '{}': {}",
                                version_id_str, e
                            );
                            continue;
                        }
                    };

                    // Start Bitswap sync for the version CID
                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Starting Bitswap sync for cid={}",
                        version_cid
                    );

                    if let Err(e) = p2p.handle
                        .bitswap_sync(version_cid, connected_peers.clone(), vec![version_cid])
                        .await
                    {
                        eprintln!(
                            "[FFI-COLLECTION-VERSION] Bitswap sync failed for {}: {}",
                            version_cid, e
                        );
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
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Failed to create txn: {}",
                                    e
                                );
                                break;
                            }
                        };

                        let blockstore = match txn.blockstore() {
                            Ok(b) => b,
                            Err(e) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Failed to get blockstore: {}",
                                    e
                                );
                                break;
                            }
                        };

                        // Check if block exists
                        let cid_bytes = version_cid.to_bytes();
                        match blockstore.has(&cid_bytes).await {
                            Ok(true) => {
                                block_found = true;
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Block {} fetched successfully",
                                    version_cid
                                );
                                break;
                            }
                            Ok(false) => {
                                // Not yet, wait and retry
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Blockstore check failed: {}",
                                    e
                                );
                                break;
                            }
                        }
                    }

                    if !block_found {
                        eprintln!(
                            "[FFI-COLLECTION-VERSION] Timeout waiting for block {}",
                            version_cid
                        );
                        continue;
                    }

                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Block fetched, extracting linked field blocks"
                    );

                    // Read block data from blockstore
                    let block_data = match p2p.blockstore.get(&version_cid).await {
                        Ok(Some(data)) => data,
                        Ok(None) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Block {} not found in blockstore after fetch",
                                version_cid
                            );
                            continue;
                        }
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Failed to read block {}: {}",
                                version_cid, e
                            );
                            continue;
                        }
                    };

                    // Decode block to extract linked field CIDs
                    let linked_cids = match Block::from_dag_cbor(&block_data) {
                        Ok(block) => {
                            let links = block.all_links();
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Collection block has {} linked CIDs",
                                links.len()
                            );
                            links
                        }
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Failed to decode block {}: {}",
                                version_cid, e
                            );
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
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Error checking link {}: {}",
                                    link_cid, e
                                );
                                continue;
                            }
                        };

                        if !already_present {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Fetching linked block {}",
                                link_cid
                            );

                            if let Err(e) = p2p.handle
                                .bitswap_sync(link_cid, connected_peers.clone(), vec![link_cid])
                                .await
                            {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Bitswap sync failed for link {}: {}",
                                    link_cid, e
                                );
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
                                        eprintln!(
                                            "[FFI-COLLECTION-VERSION] Error waiting for link {}: {}",
                                            link_cid, e
                                        );
                                        break;
                                    }
                                }
                            }

                            if !link_found {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] Timeout waiting for linked block {}",
                                    link_cid
                                );
                                continue;
                            }
                        }

                        eprintln!(
                            "[FFI-COLLECTION-VERSION] Linked block {} available",
                            link_cid
                        );

                        // If this linked block is a CollectionDefinition (previous version),
                        // also fetch its linked blocks (field definitions)
                        if let Ok(Some(link_data)) = p2p.blockstore.get(&link_cid).await {
                            if let Ok(link_block) = Block::from_dag_cbor(&link_data) {
                                if matches!(&link_block.delta, defra_core::block::CrdtDelta::CollectionDefinition(_)) {
                                    let sub_links = link_block.all_links();
                                    eprintln!(
                                        "[FFI-COLLECTION-VERSION] Previous version {} has {} sub-links",
                                        link_cid, sub_links.len()
                                    );
                                    for sub_cid in sub_links {
                                        if !fetched.contains(&sub_cid.to_string()) {
                                            fetch_queue.push_back(sub_cid);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    eprintln!(
                        "[FFI-COLLECTION-VERSION] All linked blocks fetched, processing through merge handler"
                    );

                    // Process through merge handler with recovery metadata
                    // (collection definitions don't have doc_id/collection_id in the traditional sense)
                    let metadata = p2p::sync::BlockMetadata::recovery();

                    match p2p.merge_handler.handle_block(&version_cid, &block_data, metadata).await {
                        Ok(outcome) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Merge handler result for {}: {:?}",
                                version_cid, outcome
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "[FFI-COLLECTION-VERSION] Merge handler error for {}: {}",
                                version_cid, e
                            );
                        }
                    }

                    eprintln!(
                        "[FFI-COLLECTION-VERSION] Successfully synced version {}",
                        version_id_str
                    );

                    // After merge, check if this is a view with a lens transform to sync.
                    if let Ok(block) = Block::from_dag_cbor(&block_data) {
                        if let defra_core::CrdtDelta::CollectionDefinition(ref payload) = block.delta {
                            if let Some(ref transform_cid) = payload.query_transform {
                                eprintln!(
                                    "[FFI-COLLECTION-VERSION] View has transform CID={}, syncing lens...",
                                    transform_cid
                                );
                                if let Err(e) = sync_lens(transform_cid, p2p, db, &connected_peers).await {
                                    eprintln!(
                                        "[FFI-COLLECTION-VERSION] Lens sync failed: {}",
                                        e
                                    );
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

    eprintln!(
        "[FFI-LENS-SYNC] Config block has {} module(s)",
        config_block.modules.len()
    );

    let mut lens_modules = Vec::new();

    for module_cid in &config_block.modules {
        // 2. Fetch ModuleBlock
        eprintln!("[FFI-LENS-SYNC] Fetching module block cid={}", module_cid);
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
                eprintln!("[FFI-LENS-SYNC] Fetching {} WASM chunks", chunks.len());
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

        eprintln!(
            "[FFI-LENS-SYNC] Got WASM module ({} bytes), inverse={}",
            wasm_bytes.len(),
            module_block.inverse
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

    eprintln!(
        "[FFI-LENS-SYNC] Lens registered under CID {}",
        transform_cid
    );
    Ok(())
}
