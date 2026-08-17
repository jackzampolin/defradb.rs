//! Event handlers for the replication loop.

use blockstore::Blockstore;
use cid::Cid;

use super::config::ReplicationConfig;
use super::result::ReplicationResult;
use crate::sync::coordinator::dag_context::DagFetchContext;
use crate::sync::coordinator::SyncCoordinator;
use crate::sync::manager::SyncEvent;
use crate::sync::merge::{
    BlockMetadata, MergeBlock, MergeHandler, MergeOutcome, RecoveredBlockMetadata,
};
use crate::transport::P2PTransport;

pub(super) struct DagFetchRequest {
    root_cid: Cid,
    missing: Vec<Cid>,
    providers: Vec<String>,
    doc_id: String,
    collection_id: String,
    creator: String,
    sender_peer: Option<String>,
    is_explicit_replicator: bool,
    explicit_replay_authorization: Option<crate::ExplicitReplayAuthorization>,
}

struct MergeEventMetadata {
    cid: Cid,
    doc_id: String,
    collection_id: String,
    creator: String,
    sender_peer: Option<String>,
    is_explicit_replicator: bool,
    explicit_replay_authorization: Option<crate::ExplicitReplayAuthorization>,
}

fn merge_block_from_metadata(
    cid: Cid,
    block_data: bytes::Bytes,
    metadata: &BlockMetadata<'_>,
) -> Option<MergeBlock> {
    Some(MergeBlock {
        cid,
        block_data,
        doc_id: metadata.doc_id?.to_string(),
        collection_id: metadata.collection_id?.to_string(),
        creator: metadata.creator?.to_string(),
        sender_peer: metadata.sender_peer.map(str::to_string),
        is_explicit_replicator: metadata.is_explicit_replicator,
        explicit_replay_authorization: metadata.explicit_replay_authorization.clone(),
        verified_creator: metadata.verified_creator.clone(),
    })
}

fn recovered_metadata_error(cid: Cid, details: impl Into<String>) -> ReplicationResult {
    ReplicationResult::Failed {
        cid,
        error: format!("Recovery metadata incomplete: {}", details.into()),
    }
}

async fn recover_metadata_for_block<H>(
    handler: &H,
    cid: Cid,
    block_data: &[u8],
) -> Result<RecoveredBlockMetadata, ReplicationResult>
where
    H: MergeHandler + ?Sized + 'static,
{
    match handler.recover_block_metadata(&cid, block_data).await {
        Ok(Some(metadata)) if metadata.is_complete() => Ok(metadata),
        Ok(Some(metadata)) => Err(recovered_metadata_error(
            cid,
            format!(
                "handler returned doc_id='{}', collection_id='{}', creator='{}'",
                metadata.doc_id, metadata.collection_id, metadata.creator
            ),
        )),
        Ok(None) => Err(recovered_metadata_error(
            cid,
            "handler did not return recovered metadata",
        )),
        Err(error) => Err(recovered_metadata_error(cid, error.to_string())),
    }
}

/// Handle a DagNeedsFetch event by initiating a Bitswap sync.
pub(super) async fn handle_dag_needs_fetch<B, T>(
    coordinator: &SyncCoordinator<B, T>,
    request: DagFetchRequest,
) -> ReplicationResult
where
    B: Blockstore + 'static,
    T: P2PTransport,
{
    let DagFetchRequest {
        root_cid,
        missing,
        providers,
        doc_id,
        collection_id,
        creator,
        sender_peer,
        is_explicit_replicator,
        explicit_replay_authorization,
    } = request;

    tracing::debug!(
        cid = %root_cid,
        missing_count = missing.len(),
        provider_count = providers.len(),
        "Initiating Bitswap fetch for missing blocks"
    );

    if let Some(source_peer) = sender_peer {
        tracing::debug!(
            cid = %root_cid,
            source_peer = %source_peer,
            "Using poll-based DAG fetcher for push-driven DAG recovery"
        );

        let transport = coordinator.transport().clone();
        let blockstore = coordinator.blockstore().clone();
        let event_tx = coordinator.manager().event_sender();
        let limiter = coordinator.dag_fetch_limiter();
        let diagnostics = coordinator.manager().diagnostics();
        let source_peer = crate::transport::PeerId::new(source_peer);
        let alternate_providers: Vec<crate::transport::PeerId> = providers
            .into_iter()
            .map(crate::transport::PeerId::new)
            .collect();
        let context = DagFetchContext::new(doc_id, collection_id, creator, source_peer)
            .with_alternate_providers(alternate_providers)
            .with_explicit_replicator(is_explicit_replicator)
            .with_explicit_replay_authorization(explicit_replay_authorization)
            .with_pending_lease(coordinator.manager().pending_dag_lease(root_cid))
            .with_block_sync_completions(coordinator.manager().block_sync_completion_tracker())
            .with_rooted_car_completions(coordinator.manager().rooted_car_completion_tracker())
            .with_rooted_provider_discovery();

        coordinator.spawn_pending_dag_fetch_task(root_cid, "pushlog_fetch_dag", async move {
            crate::sync::coordinator::dag_fetcher::poll_fetch_dag(
                transport,
                blockstore,
                event_tx,
                root_cid,
                context,
                limiter,
                diagnostics,
            )
            .await;
        });

        return ReplicationResult::DagFetchStarted { root_cid };
    }

    // Convert string peer IDs to transport PeerIds.
    // If the provider list is empty, fall back to all connected transport peers.
    let transport_providers: Vec<crate::transport::PeerId> = if providers.is_empty() {
        match coordinator.transport().connected_peers().await {
            Ok(peers) => peers,
            Err(e) => {
                tracing::warn!(
                    cid = %root_cid,
                    error = %e,
                    "Failed to list connected peers as Bitswap fetch providers"
                );
                Vec::new()
            }
        }
    } else {
        providers
            .into_iter()
            .map(crate::transport::PeerId::new)
            .collect()
    };

    // Start Bitswap sync via transport
    match coordinator
        .transport()
        .sync_blocks(root_cid, transport_providers, missing)
        .await
    {
        Ok(query_id) => {
            // Register the query so we can track completion
            coordinator.manager().register_query(query_id, root_cid);
            ReplicationResult::BitswapFetchStarted { root_cid, query_id }
        }
        Err(e) => {
            tracing::warn!(
                cid = %root_cid,
                error = %e,
                "Failed to start Bitswap fetch"
            );
            ReplicationResult::Failed {
                cid: root_cid,
                error: format!("Failed to start Bitswap fetch: {}", e),
            }
        }
    }
}

/// Handle a BlockReceived event.
pub(super) async fn handle_block_received<B, T, H>(
    coordinator: &SyncCoordinator<B, T>,
    handler: &H,
    config: &ReplicationConfig,
    cid: Cid,
    metadata: BlockMetadata<'_>,
) -> ReplicationResult
where
    B: Blockstore + 'static,
    T: P2PTransport,
    H: MergeHandler + ?Sized + 'static,
{
    // Load block from blockstore
    let block_data = match coordinator.blockstore().get(&cid).await {
        Ok(Some(data)) => data,
        Ok(None) => {
            return ReplicationResult::Failed {
                cid,
                error: "Block not found in blockstore".to_string(),
            }
        }
        Err(e) => {
            return ReplicationResult::Failed {
                cid,
                error: format!("Failed to load block: {}", e),
            }
        }
    };

    let recovered_metadata: Option<RecoveredBlockMetadata>;
    let metadata = if metadata.is_recovery && metadata.is_incomplete() {
        let recovered = match recover_metadata_for_block(handler, cid, &block_data).await {
            Ok(recovered) => recovered,
            Err(result) => return result,
        };
        recovered_metadata = Some(recovered);
        let recovered = recovered_metadata
            .as_ref()
            .expect("recovered metadata was just stored");
        BlockMetadata::recovered(
            &recovered.doc_id,
            &recovered.collection_id,
            &recovered.creator,
            recovered.verified_creator.clone(),
        )
    } else {
        metadata
    };

    if metadata.explicit_replay_authorization.is_some() {
        let Some(block) = merge_block_from_metadata(cid, block_data.clone(), &metadata) else {
            return ReplicationResult::Failed {
                cid,
                error: "Explicit replay authorization requires complete block metadata".to_string(),
            };
        };
        if let Err(error) = handler
            .validate_authorization(block.explicit_replay_authorization.as_ref(), &block)
            .await
        {
            return ReplicationResult::Failed {
                cid,
                error: error.to_string(),
            };
        }
    }

    // Extract doc_id and collection_id for use in result.
    let doc_id_for_result = metadata.doc_id.unwrap_or("").to_string();
    let collection_id_for_result = metadata.collection_id.unwrap_or("").to_string();
    let collection_id_for_broadcast = metadata.collection_id.unwrap_or("");

    // Delegate merge to handler
    match handler.handle_block(&cid, &block_data, metadata).await {
        Ok(MergeOutcome::Merged) => {
            // Merge successful - mark as merged
            if let Err(e) = coordinator.mark_as_merged(&cid).await {
                // Return a distinct result so callers know the merge succeeded
                // but bookkeeping failed (block will be reprocessed on restart)
                return ReplicationResult::MergedButNotMarked {
                    cid,
                    error: e.to_string(),
                };
            }

            // Optionally re-broadcast (skip if metadata incomplete - can't broadcast without doc/collection IDs)
            if config.rebroadcast_on_merge && !collection_id_for_broadcast.is_empty() {
                match coordinator
                    .broadcast_local_update(
                        &cid,
                        &block_data,
                        &doc_id_for_result,
                        collection_id_for_broadcast,
                    )
                    .await
                {
                    Ok(crate::sync::BroadcastResult::Success) => {
                        // Both topics succeeded - nothing to report
                    }
                    Ok(crate::sync::BroadcastResult::PartialDocumentOnly { collection_error }) => {
                        // Partial success - return distinct result so callers know
                        return ReplicationResult::MergedButBroadcastFailed {
                            cid,
                            doc_id: doc_id_for_result,
                            collection_id: collection_id_for_result,
                            broadcast_error: format!(
                                "Partial: document topic succeeded but collection topic failed: {}",
                                collection_error
                            ),
                        };
                    }
                    Ok(crate::sync::BroadcastResult::PartialCollectionOnly { document_error }) => {
                        // Partial success - return distinct result so callers know
                        return ReplicationResult::MergedButBroadcastFailed {
                            cid,
                            doc_id: doc_id_for_result,
                            collection_id: collection_id_for_result,
                            broadcast_error: format!(
                                "Partial: collection topic succeeded but document topic failed: {}",
                                document_error
                            ),
                        };
                    }
                    Err(e) => {
                        // Total failure - return a distinct result
                        return ReplicationResult::MergedButBroadcastFailed {
                            cid,
                            doc_id: doc_id_for_result,
                            collection_id: collection_id_for_result,
                            broadcast_error: e.to_string(),
                        };
                    }
                }
            }

            ReplicationResult::Merged {
                cid,
                doc_id: doc_id_for_result,
                collection_id: collection_id_for_result,
            }
        }
        Ok(MergeOutcome::Skipped { reason, terminal }) => {
            if terminal {
                if let Err(e) = coordinator.mark_as_merged(&cid).await {
                    return ReplicationResult::MergedButNotMarked {
                        cid,
                        error: format!("skipped but failed to mark: {}", e),
                    };
                }
            }

            ReplicationResult::Skipped {
                cid,
                doc_id: doc_id_for_result,
                collection_id: collection_id_for_result,
                reason,
                terminal,
            }
        }
        Ok(MergeOutcome::Rejected { reason }) => {
            coordinator.quarantine_pending_dag(&cid, &reason).await;

            ReplicationResult::Quarantined {
                cid,
                doc_id: doc_id_for_result,
                collection_id: collection_id_for_result,
                reason,
            }
        }
        Ok(_) => ReplicationResult::Failed {
            cid,
            error: "unsupported merge outcome".to_string(),
        },
        Err(e) => ReplicationResult::Failed {
            cid,
            error: e.to_string(),
        },
    }
}

/// Returns true if this event type can be batch-merged.
pub(super) fn is_mergeable_event(event: &SyncEvent) -> bool {
    matches!(
        event,
        SyncEvent::BlockReceived { .. } | SyncEvent::DagReady { .. }
    )
}

fn event_merge_cid(event: &SyncEvent) -> Option<Cid> {
    match event {
        SyncEvent::BlockReceived { cid, .. } => Some(*cid),
        SyncEvent::DagReady { root_cid, .. } => Some(*root_cid),
        _ => None,
    }
}

async fn skipped_if_already_merged<B, T>(
    coordinator: &SyncCoordinator<B, T>,
    event: &SyncEvent,
    cid: Cid,
) -> Option<ReplicationResult>
where
    B: Blockstore + 'static,
    T: P2PTransport,
{
    match coordinator.manager().is_merged(&cid).await {
        Ok(false) => None,
        Ok(true) => match event {
            SyncEvent::BlockReceived {
                doc_id,
                collection_id,
                ..
            } => Some(already_merged_result(cid, doc_id, collection_id)),
            SyncEvent::DagReady {
                root_cid,
                doc_id,
                collection_id,
                ..
            } => {
                coordinator.clear_pending_dag(root_cid);
                Some(already_merged_result(cid, doc_id, collection_id))
            }
            _ => None,
        },
        Err(error) => Some(ReplicationResult::Failed {
            cid,
            error: error.to_string(),
        }),
    }
}

fn already_merged_result(cid: Cid, doc_id: &str, collection_id: &str) -> ReplicationResult {
    ReplicationResult::Skipped {
        cid,
        doc_id: doc_id.to_string(),
        collection_id: collection_id.to_string(),
        reason: "already merged".to_string(),
        terminal: true,
    }
}

/// Extract merge block metadata from a SyncEvent.
fn event_to_merge_metadata(event: &SyncEvent) -> MergeEventMetadata {
    match event {
        SyncEvent::BlockReceived {
            cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
            ..
        } => MergeEventMetadata {
            cid: *cid,
            doc_id: doc_id.clone(),
            collection_id: collection_id.clone(),
            creator: creator.clone(),
            sender_peer: sender_peer.clone(),
            is_explicit_replicator: *is_explicit_replicator,
            explicit_replay_authorization: explicit_replay_authorization.clone(),
        },
        SyncEvent::DagReady {
            root_cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
            ..
        } => MergeEventMetadata {
            cid: *root_cid,
            doc_id: doc_id.clone(),
            collection_id: collection_id.clone(),
            creator: creator.clone(),
            sender_peer: sender_peer.clone(),
            is_explicit_replicator: *is_explicit_replicator,
            explicit_replay_authorization: explicit_replay_authorization.clone(),
        },
        _ => unreachable!("is_mergeable_event should have filtered this"),
    }
}

/// Process a batch of merge-eligible events using handle_block_batch().
///
/// Loads block data from the blockstore, delegates batch merge to the handler,
/// then batch-marks successful merges in a single transaction.
pub(super) async fn process_merge_batch<B, T, H>(
    coordinator: &SyncCoordinator<B, T>,
    events: Vec<SyncEvent>,
    handler: &H,
    config: &ReplicationConfig,
) -> Vec<ReplicationResult>
where
    B: Blockstore + 'static,
    T: P2PTransport,
    H: MergeHandler + ?Sized + 'static,
{
    let mut merge_blocks = Vec::with_capacity(events.len());
    let mut results = Vec::new();

    // Load block data for each event from blockstore
    for event in &events {
        let MergeEventMetadata {
            cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
        } = event_to_merge_metadata(event);

        if matches!(event, SyncEvent::DagReady { .. }) {
            coordinator.clear_pending_dag(&cid);
        }

        match coordinator.blockstore().get(&cid).await {
            Ok(Some(data)) => {
                let block = MergeBlock {
                    cid,
                    block_data: data,
                    doc_id,
                    collection_id,
                    creator,
                    sender_peer,
                    is_explicit_replicator,
                    explicit_replay_authorization,
                    verified_creator: None,
                };

                if let Err(error) = handler
                    .validate_authorization(block.explicit_replay_authorization.as_ref(), &block)
                    .await
                {
                    results.push(ReplicationResult::Failed {
                        cid,
                        error: error.to_string(),
                    });
                    continue;
                }

                merge_blocks.push(block);
            }
            Ok(None) => {
                results.push(ReplicationResult::Failed {
                    cid,
                    error: "Block not found in blockstore".to_string(),
                });
            }
            Err(e) => {
                results.push(ReplicationResult::Failed {
                    cid,
                    error: format!("Failed to load block: {}", e),
                });
            }
        }
    }

    if merge_blocks.is_empty() {
        return results;
    }

    // Call handler.handle_block_batch()
    let batch_result_start = results.len();
    let batch_results = handler.handle_block_batch(&merge_blocks).await;

    // Collect CIDs to mark as merged
    let mut merged_cids = Vec::new();
    for (block, result) in merge_blocks.iter().zip(batch_results) {
        match result {
            Ok(MergeOutcome::Merged) => {
                merged_cids.push(block.cid);
                results.push(ReplicationResult::Merged {
                    cid: block.cid,
                    doc_id: block.doc_id.clone(),
                    collection_id: block.collection_id.clone(),
                });
            }
            Ok(MergeOutcome::Skipped { reason, terminal }) => {
                if terminal {
                    merged_cids.push(block.cid);
                }
                results.push(ReplicationResult::Skipped {
                    cid: block.cid,
                    doc_id: block.doc_id.clone(),
                    collection_id: block.collection_id.clone(),
                    reason,
                    terminal,
                });
            }
            Ok(MergeOutcome::Rejected { reason }) => {
                // Deliberately NOT added to merged_cids: quarantine leaves
                // the block unmerged (see `quarantine_pending_dag`).
                coordinator
                    .quarantine_pending_dag(&block.cid, &reason)
                    .await;

                results.push(ReplicationResult::Quarantined {
                    cid: block.cid,
                    doc_id: block.doc_id.clone(),
                    collection_id: block.collection_id.clone(),
                    reason,
                });
            }
            Ok(_) => {
                results.push(ReplicationResult::Failed {
                    cid: block.cid,
                    error: "unsupported merge outcome".to_string(),
                });
            }
            Err(e) => {
                results.push(ReplicationResult::Failed {
                    cid: block.cid,
                    error: e.to_string(),
                });
            }
        }
    }

    // Batch mark_as_merged in one txn
    if !merged_cids.is_empty() {
        if let Err(e) = coordinator.mark_batch_as_merged(&merged_cids).await {
            tracing::error!(
                error = %e,
                count = merged_cids.len(),
                "Failed to batch mark_as_merged"
            );
            // Downgrade affected terminal results to MergedButNotMarked.
            for result in &mut results {
                let cid = match result {
                    ReplicationResult::Merged { cid, .. } => *cid,
                    ReplicationResult::Skipped {
                        cid,
                        terminal: true,
                        ..
                    } => *cid,
                    _ => continue,
                };

                if merged_cids.contains(&cid) {
                    *result = ReplicationResult::MergedButNotMarked {
                        cid,
                        error: e.to_string(),
                    };
                }
            }
        }
    }

    if config.rebroadcast_on_merge {
        for (index, block) in merge_blocks.iter().enumerate() {
            let result_index = batch_result_start + index;
            if block.collection_id.is_empty()
                || !matches!(results[result_index], ReplicationResult::Merged { .. })
            {
                continue;
            }

            match coordinator
                .broadcast_local_update(
                    &block.cid,
                    block.block_data.as_ref(),
                    &block.doc_id,
                    &block.collection_id,
                )
                .await
            {
                Ok(crate::sync::BroadcastResult::Success) => {}
                Ok(crate::sync::BroadcastResult::PartialDocumentOnly { collection_error }) => {
                    results[result_index] = ReplicationResult::MergedButBroadcastFailed {
                        cid: block.cid,
                        doc_id: block.doc_id.clone(),
                        collection_id: block.collection_id.clone(),
                        broadcast_error: format!(
                            "Partial: document topic succeeded but collection topic failed: {}",
                            collection_error
                        ),
                    };
                }
                Ok(crate::sync::BroadcastResult::PartialCollectionOnly { document_error }) => {
                    results[result_index] = ReplicationResult::MergedButBroadcastFailed {
                        cid: block.cid,
                        doc_id: block.doc_id.clone(),
                        collection_id: block.collection_id.clone(),
                        broadcast_error: format!(
                            "Partial: collection topic succeeded but document topic failed: {}",
                            document_error
                        ),
                    };
                }
                Err(error) => {
                    results[result_index] = ReplicationResult::MergedButBroadcastFailed {
                        cid: block.cid,
                        doc_id: block.doc_id.clone(),
                        collection_id: block.collection_id.clone(),
                        broadcast_error: error.to_string(),
                    };
                }
            }
        }
    }

    results
}

/// Process a sync event and return the result.
pub(super) async fn process_event<B, T, H>(
    coordinator: &SyncCoordinator<B, T>,
    event: SyncEvent,
    handler: &H,
    config: &ReplicationConfig,
) -> ReplicationResult
where
    B: Blockstore + 'static,
    T: P2PTransport,
    H: MergeHandler + ?Sized + 'static,
{
    match event {
        SyncEvent::BlockReceived {
            cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
        } => {
            handle_block_received(
                coordinator,
                handler,
                config,
                cid,
                BlockMetadata::normal(
                    &doc_id,
                    &collection_id,
                    &creator,
                    sender_peer.as_deref(),
                    is_explicit_replicator,
                )
                .with_explicit_replay_authorization(explicit_replay_authorization),
            )
            .await
        }
        SyncEvent::SyncError { cid, error } => ReplicationResult::Failed { cid, error },
        SyncEvent::DagNeedsFetch {
            root_cid,
            missing,
            providers,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
        } => {
            handle_dag_needs_fetch(
                coordinator,
                DagFetchRequest {
                    root_cid,
                    missing,
                    providers,
                    doc_id,
                    collection_id,
                    creator,
                    sender_peer,
                    is_explicit_replicator,
                    explicit_replay_authorization,
                },
            )
            .await
        }
        SyncEvent::DagReady {
            root_cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
        } => {
            coordinator.clear_pending_dag(&root_cid);
            // DAG is complete after Bitswap fetch - process as block received
            tracing::info!(
                cid = %root_cid,
                doc_id = %doc_id,
                "DAG ready for merge after Bitswap fetch"
            );
            handle_block_received(
                coordinator,
                handler,
                config,
                root_cid,
                BlockMetadata::normal(
                    &doc_id,
                    &collection_id,
                    &creator,
                    sender_peer.as_deref(),
                    is_explicit_replicator,
                )
                .with_explicit_replay_authorization(explicit_replay_authorization),
            )
            .await
        }
    }
}

/// Process a sync event after acquiring the CID-level merge guard.
pub(super) async fn process_event_serialized<B, T, H>(
    coordinator: &SyncCoordinator<B, T>,
    event: SyncEvent,
    handler: &H,
    config: &ReplicationConfig,
) -> ReplicationResult
where
    B: Blockstore + 'static,
    T: P2PTransport,
    H: MergeHandler + ?Sized + 'static,
{
    let Some(cid) = event_merge_cid(&event) else {
        return process_event(coordinator, event, handler, config).await;
    };

    loop {
        match coordinator
            .manager()
            .process_queue()
            .try_acquire(&cid)
            .await
        {
            Ok(_guard) => {
                if let Some(result) = skipped_if_already_merged(coordinator, &event, cid).await {
                    return result;
                }
                return process_event(coordinator, event, handler, config).await;
            }
            Err(wait_for_current_merge) => {
                if wait_for_current_merge.await.is_err() {
                    tracing::debug!(
                        cid = %cid,
                        "CID merge guard owner was cancelled; checking merge status"
                    );
                }
                if let Some(result) = skipped_if_already_merged(coordinator, &event, cid).await {
                    return result;
                }
            }
        }
    }
}

pub(super) async fn process_events_individually<B, T, H>(
    coordinator: &SyncCoordinator<B, T>,
    events: Vec<SyncEvent>,
    handler: &H,
    config: &ReplicationConfig,
) -> Vec<ReplicationResult>
where
    B: Blockstore + 'static,
    T: P2PTransport,
    H: MergeHandler + ?Sized + 'static,
{
    let mut results = Vec::with_capacity(events.len());
    for event in events {
        results.push(process_event_serialized(coordinator, event, handler, config).await);
    }
    results
}

pub(super) fn has_duplicate_merge_cids(events: &[SyncEvent]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(events.len());
    events
        .iter()
        .filter_map(event_merge_cid)
        .any(|cid| !seen.insert(cid))
}
