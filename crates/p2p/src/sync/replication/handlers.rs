//! Event handlers for the replication loop.

use blockstore::Blockstore;
use cid::Cid;
use libp2p::PeerId;

use super::config::ReplicationConfig;
use super::result::ReplicationResult;
use crate::sync::coordinator::SyncCoordinator;
use crate::sync::manager::SyncEvent;
use crate::sync::merge::{BlockMetadata, MergeHandler, MergeOutcome};

/// Handle a DagNeedsFetch event by initiating a Bitswap sync.
pub(super) async fn handle_dag_needs_fetch<B>(
    coordinator: &SyncCoordinator<B>,
    root_cid: Cid,
    missing: Vec<Cid>,
    providers: Vec<PeerId>,
) -> ReplicationResult
where
    B: Blockstore + 'static,
{
    tracing::debug!(
        cid = %root_cid,
        missing_count = missing.len(),
        provider_count = providers.len(),
        "Initiating Bitswap fetch for missing blocks"
    );

    // Start Bitswap sync via host
    match coordinator
        .host()
        .bitswap_sync(root_cid, providers, missing)
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
pub(super) async fn handle_block_received<B, H>(
    coordinator: &SyncCoordinator<B>,
    handler: &H,
    config: &ReplicationConfig,
    cid: Cid,
    metadata: BlockMetadata<'_>,
) -> ReplicationResult
where
    B: Blockstore + 'static,
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

            ReplicationResult::Merged {
                cid,
                doc_id: doc_id_for_result,
                collection_id: collection_id_for_result,
            }
        }
        Ok(MergeOutcome::Skipped { reason }) => {
            // Merge skipped - still mark as merged to prevent reprocessing
            if let Err(e) = coordinator.mark_as_merged(&cid).await {
                // For skipped blocks, marking failure is less critical since
                // re-processing will just skip again, but still report it
                return ReplicationResult::MergedButNotMarked {
                    cid,
                    error: format!("skipped but failed to mark: {}", e),
                };
            }

            ReplicationResult::Skipped { cid, reason }
        }
        Err(e) => ReplicationResult::Failed {
            cid,
            error: e.to_string(),
        },
    }
}

/// Process a sync event and return the result.
pub(super) async fn process_event<B, H>(
    coordinator: &SyncCoordinator<B>,
    event: SyncEvent,
    handler: &H,
    config: &ReplicationConfig,
) -> ReplicationResult
where
    B: Blockstore + 'static,
    H: MergeHandler + ?Sized + 'static,
{
    match event {
        SyncEvent::BlockReceived {
            cid,
            doc_id,
            collection_id,
            creator,
        } => {
            handle_block_received(
                coordinator,
                handler,
                config,
                cid,
                BlockMetadata::normal(&doc_id, &collection_id, &creator),
            )
            .await
        }
        SyncEvent::BlockAlreadyMerged { cid } => ReplicationResult::Skipped {
            cid,
            reason: "already merged".to_string(),
        },
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
            schema_version_id,
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
                BlockMetadata::normal(&doc_id, &collection_id, &schema_version_id),
            )
            .await
        }
    }
}
