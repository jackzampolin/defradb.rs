//! Event handlers for the replication loop.

use blockstore::Blockstore;
use cid::Cid;

use acp::ReplicatedDocActorRelationships;

use super::config::ReplicationConfig;
use super::result::ReplicationResult;
use crate::sync::coordinator::SyncCoordinator;
use crate::sync::manager::SyncEvent;
use crate::sync::merge::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
use crate::transport::P2PTransport;

/// Handle a DagNeedsFetch event by initiating a Bitswap sync.
pub(super) async fn handle_dag_needs_fetch<B, T>(
    coordinator: &SyncCoordinator<B, T>,
    root_cid: Cid,
    missing: Vec<Cid>,
    providers: Vec<String>,
) -> ReplicationResult
where
    B: Blockstore + 'static,
    T: P2PTransport,
{
    tracing::debug!(
        cid = %root_cid,
        missing_count = missing.len(),
        provider_count = providers.len(),
        "Initiating Bitswap fetch for missing blocks"
    );

    // Convert string peer IDs to transport PeerIds.
    // If the provider list is empty, fall back to all connected transport peers.
    let transport_providers: Vec<crate::transport::PeerId> = if providers.is_empty() {
        coordinator
            .transport()
            .connected_peers()
            .await
            .unwrap_or_default()
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
    acp_actor_relationships: Option<&ReplicatedDocActorRelationships>,
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

    // Extract doc_id and collection_id for use in result (use empty string if recovery mode)
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

            if let Err(error) = coordinator
                .apply_replicated_actor_relationships(&doc_id_for_result, acp_actor_relationships)
                .await
            {
                return ReplicationResult::Failed {
                    cid,
                    error: error.to_string(),
                };
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
                if let Err(error) = coordinator
                    .apply_replicated_actor_relationships(
                        &doc_id_for_result,
                        acp_actor_relationships,
                    )
                    .await
                {
                    return ReplicationResult::Failed {
                        cid,
                        error: error.to_string(),
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
        Err(e) => ReplicationResult::Failed {
            cid,
            error: e.to_string(),
        },
    }
}

/// Returns true if this event type can be batch-merged.
pub(super) fn is_mergeable_event(event: &SyncEvent) -> bool {
    match event {
        SyncEvent::BlockReceived {
            acp_actor_relationships,
            ..
        }
        | SyncEvent::DagReady {
            acp_actor_relationships,
            ..
        } => acp_actor_relationships.is_none(),
        _ => false,
    }
}

/// Extract merge block metadata from a SyncEvent.
fn event_to_merge_metadata(
    event: &SyncEvent,
) -> (
    Cid,
    String,
    String,
    String,
    Option<String>,
    bool,
    Option<crate::ExplicitReplayAuthorization>,
) {
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
        } => (
            *cid,
            doc_id.clone(),
            collection_id.clone(),
            creator.clone(),
            sender_peer.clone(),
            *is_explicit_replicator,
            explicit_replay_authorization.clone(),
        ),
        SyncEvent::DagReady {
            root_cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
            ..
        } => (
            *root_cid,
            doc_id.clone(),
            collection_id.clone(),
            creator.clone(),
            sender_peer.clone(),
            *is_explicit_replicator,
            explicit_replay_authorization.clone(),
        ),
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
    _config: &ReplicationConfig,
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
        let (
            cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
        ) = event_to_merge_metadata(event);

        match coordinator.blockstore().get(&cid).await {
            Ok(Some(data)) => {
                merge_blocks.push(MergeBlock {
                    cid,
                    block_data: data,
                    doc_id,
                    collection_id,
                    creator,
                    sender_peer,
                    is_explicit_replicator,
                    explicit_replay_authorization,
                    verified_creator: None,
                });
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
    let batch_results = handler.handle_block_batch(&merge_blocks).await;

    // Collect CIDs to mark as merged
    let mut merged_cids = Vec::new();
    for (block, result) in merge_blocks.iter().zip(batch_results.into_iter()) {
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
            // Downgrade affected Merged results to MergedButNotMarked
            for result in &mut results {
                if let ReplicationResult::Merged { cid, .. } = result {
                    if merged_cids.contains(cid) {
                        *result = ReplicationResult::MergedButNotMarked {
                            cid: *cid,
                            error: e.to_string(),
                        };
                    }
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
            acp_actor_relationships,
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
                acp_actor_relationships.as_ref(),
            )
            .await
        }
        SyncEvent::BlockAlreadyMerged {
            cid,
            doc_id,
            collection_id,
            acp_actor_relationships,
        } => {
            if let Err(error) = coordinator
                .apply_replicated_actor_relationships(&doc_id, acp_actor_relationships.as_ref())
                .await
            {
                ReplicationResult::Failed {
                    cid,
                    error: error.to_string(),
                }
            } else {
                ReplicationResult::Skipped {
                    cid,
                    doc_id,
                    collection_id,
                    reason: "already merged".to_string(),
                    terminal: true,
                }
            }
        }
        SyncEvent::SyncError { cid, error } => ReplicationResult::Failed { cid, error },
        SyncEvent::DagNeedsFetch {
            root_cid,
            missing,
            providers,
            ..
        } => handle_dag_needs_fetch(coordinator, root_cid, missing, providers).await,
        SyncEvent::DagReady {
            root_cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
            acp_actor_relationships,
        } => {
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
                acp_actor_relationships.as_ref(),
            )
            .await
        }
    }
}
