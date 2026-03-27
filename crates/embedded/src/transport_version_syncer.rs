use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;
use p2p::transport::PeerId;
use p2p::P2PTransport;

/// Trait for syncing collection versions via a generic transport.
#[async_trait]
pub trait TransportVersionSyncer: Send + Sync {
    async fn sync_versions(
        &self,
        version_ids: Vec<String>,
        connected_peers: Vec<PeerId>,
    ) -> Result<(), String>;
}

/// Database-backed version syncer that fetches schema blocks via transport.
pub struct DbTransportVersionSyncer<S: storage::corekv::Store, B: Blockstore, T: P2PTransport> {
    blockstore: Arc<B>,
    merge_handler: Arc<db::DbMergeHandler<S, B>>,
    db: Arc<db::DB<S>>,
    transport: T,
}

impl<S: storage::corekv::Store + 'static, B: Blockstore + 'static, T: P2PTransport>
    DbTransportVersionSyncer<S, B, T>
{
    pub fn new(
        blockstore: Arc<B>,
        merge_handler: Arc<db::DbMergeHandler<S, B>>,
        db: Arc<db::DB<S>>,
        transport: T,
    ) -> Self {
        Self {
            blockstore,
            merge_handler,
            db,
            transport,
        }
    }

    pub fn new_arc(
        blockstore: Arc<B>,
        merge_handler: Arc<db::DbMergeHandler<S, B>>,
        db: Arc<db::DB<S>>,
        transport: T,
    ) -> Arc<dyn TransportVersionSyncer> {
        Arc::new(Self::new(blockstore, merge_handler, db, transport))
    }
}

async fn fetch_block<B: Blockstore, T: P2PTransport>(
    target_cid: cid::Cid,
    blockstore: &Arc<B>,
    transport: &T,
    peers: &[PeerId],
    timeout_secs: u64,
) -> Result<bytes::Bytes, String> {
    if let Ok(Some(data)) = blockstore.get(&target_cid).await {
        return Ok(data);
    }

    transport
        .sync_blocks(target_cid, peers.to_vec(), vec![target_cid])
        .await
        .map_err(|error| format!("block sync for {target_cid}: {error}"))?;

    let timeout = std::time::Duration::from_secs(timeout_secs);
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(Some(data)) = blockstore.get(&target_cid).await {
            return Ok(data);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Err(format!("timeout fetching block {target_cid}"))
}

async fn sync_lens<S: storage::corekv::Store + 'static, B: Blockstore, T: P2PTransport>(
    transform_cid: &cid::Cid,
    blockstore: &Arc<B>,
    transport: &T,
    db: &Arc<db::DB<S>>,
    connected_peers: &[PeerId],
) -> Result<(), String> {
    use defra_core::{LensConfigBlock, LensModuleBlock, LensWasmBlock};

    let config_data =
        fetch_block(*transform_cid, blockstore, transport, connected_peers, 10).await?;
    let config_block: LensConfigBlock = serde_ipld_dagcbor::from_slice(&config_data)
        .map_err(|error| format!("decode config block: {error}"))?;

    let mut lens_modules = Vec::new();
    for module_cid in &config_block.modules {
        let module_data =
            fetch_block(*module_cid, blockstore, transport, connected_peers, 10).await?;
        let module_block: LensModuleBlock = serde_ipld_dagcbor::from_slice(&module_data)
            .map_err(|error| format!("decode module block {module_cid}: {error}"))?;

        let lens_data = fetch_block(
            module_block.lens,
            blockstore,
            transport,
            connected_peers,
            10,
        )
        .await?;
        let wasm_block: LensWasmBlock = serde_ipld_dagcbor::from_slice(&lens_data)
            .map_err(|error| format!("decode lens block {}: {error}", module_block.lens))?;

        let wasm_bytes = match &wasm_block {
            LensWasmBlock::Direct { wasm_bytes } => wasm_bytes.clone(),
            LensWasmBlock::Chunked { chunks } => {
                let mut all_bytes = Vec::new();
                for chunk_cid in chunks {
                    let chunk_data =
                        fetch_block(*chunk_cid, blockstore, transport, connected_peers, 10).await?;
                    let decoded: serde_bytes::ByteBuf = serde_ipld_dagcbor::from_slice(&chunk_data)
                        .map_err(|error| format!("decode WASM chunk {chunk_cid}: {error}"))?;
                    all_bytes.extend_from_slice(&decoded);
                }
                all_bytes
            }
            _ => Vec::new(),
        };

        let mut lens_module = lens::LensModule::from_bytes(wasm_bytes);
        lens_module.inverse = module_block.inverse;
        if !module_block.arguments.is_empty() {
            let args_map: serde_json::Map<String, serde_json::Value> = module_block
                .arguments
                .iter()
                .map(|entry| {
                    (
                        entry.key.clone(),
                        serde_json::Value::String(entry.value.clone()),
                    )
                })
                .collect();
            lens_module.arguments = Some(serde_json::Value::Object(args_map));
        }
        lens_modules.push(lens_module);
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
        .map_err(|error| format!("register lens: {error}"))?;

    Ok(())
}

#[async_trait]
impl<S: storage::corekv::Store + 'static, B: Blockstore + 'static, T: P2PTransport>
    TransportVersionSyncer for DbTransportVersionSyncer<S, B, T>
{
    async fn sync_versions(
        &self,
        version_ids: Vec<String>,
        connected_peers: Vec<PeerId>,
    ) -> Result<(), String> {
        use p2p::sync::MergeHandler;

        for version_id_str in &version_ids {
            let version_cid = cid::Cid::try_from(version_id_str.as_str())
                .map_err(|error| format!("invalid cid: {error}"))?;

            if let Err(error) = self
                .transport
                .sync_blocks(version_cid, connected_peers.clone(), vec![version_cid])
                .await
            {
                tracing::warn!(cid = %version_cid, error = %error, "block sync failed");
                continue;
            }

            let timeout = std::time::Duration::from_secs(30);
            let start = std::time::Instant::now();
            let mut block_found = false;
            while start.elapsed() < timeout {
                let txn = match self.db.new_txn(true).await {
                    Ok(txn) => txn,
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to create txn");
                        break;
                    }
                };
                let blockstore = match txn.blockstore() {
                    Ok(blockstore) => blockstore,
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to get blockstore");
                        break;
                    }
                };

                match blockstore.has(&version_cid.to_bytes()).await {
                    Ok(true) => {
                        block_found = true;
                        break;
                    }
                    Ok(false) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                    Err(error) => {
                        tracing::warn!(error = %error, "blockstore check failed");
                        break;
                    }
                }
            }

            if !block_found {
                tracing::warn!(cid = %version_cid, "timeout waiting for block");
                continue;
            }

            let block_data = match self.blockstore.get(&version_cid).await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    tracing::warn!(cid = %version_cid, "block not found after fetch");
                    continue;
                }
                Err(error) => {
                    tracing::warn!(cid = %version_cid, error = %error, "failed to read block");
                    continue;
                }
            };

            let linked_cids = match defra_core::Block::from_dag_cbor(&block_data) {
                Ok(block) => block.all_links(),
                Err(error) => {
                    tracing::warn!(cid = %version_cid, error = %error, "failed to decode block");
                    Vec::new()
                }
            };

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
                    Err(error) => {
                        tracing::warn!(cid = %link_cid, error = %error, "error checking link");
                        continue;
                    }
                };

                if !already_present {
                    if let Err(error) = self
                        .transport
                        .sync_blocks(link_cid, connected_peers.clone(), vec![link_cid])
                        .await
                    {
                        tracing::warn!(cid = %link_cid, error = %error, "block sync failed for link");
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
                            Err(error) => {
                                tracing::warn!(cid = %link_cid, error = %error, "error waiting for link");
                                break;
                            }
                        }
                    }
                    if !link_found {
                        tracing::warn!(cid = %link_cid, "timeout waiting for linked block");
                        continue;
                    }
                }

                if let Ok(Some(link_data)) = self.blockstore.get(&link_cid).await {
                    if let Ok(link_block) = defra_core::Block::from_dag_cbor(&link_data) {
                        if matches!(
                            &link_block.delta,
                            defra_core::block::CrdtDelta::CollectionDefinition(_)
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

            let metadata = p2p::sync::BlockMetadata::schema_sync();
            if let Err(error) = self
                .merge_handler
                .handle_block(&version_cid, &block_data, metadata)
                .await
            {
                tracing::warn!(cid = %version_cid, error = %error, "merge handler error");
            }

            if let Ok(block) = defra_core::Block::from_dag_cbor(&block_data) {
                if let defra_core::CrdtDelta::CollectionDefinition(ref payload) = block.delta {
                    if let Some(ref transform_cid) = payload.query_transform {
                        if let Err(error) = sync_lens(
                            transform_cid,
                            &self.blockstore,
                            &self.transport,
                            &self.db,
                            &connected_peers,
                        )
                        .await
                        {
                            tracing::warn!(error = %error, "lens sync failed");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
