//! PushLog processing and block storage.

use std::collections::HashSet;
use std::time::Instant;

use cid::Cid;

use blockstore::{verify_block_cid, Blockstore};

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;
use crate::sync::manager::events::SyncEvent;
use crate::sync::manager::links::find_missing_links;
use crate::sync::manager::pending::{PendingDag, MAX_PENDING_DAGS, PENDING_DAG_TTL};

use super::SyncManager;

impl<B: Blockstore + 'static> SyncManager<B> {
    /// Process an incoming PushLog broadcast.
    ///
    /// This is the main entry point for handling sync messages from the network.
    ///
    /// # Flow
    ///
    /// 1. Parse CID from the message
    /// 2. Acquire process queue lock (serialize concurrent syncs for same CID)
    /// 3. Check if already merged
    /// 4. Store block in blockstore (marked as unmerged)
    /// 5. Emit BlockReceived event for database layer to merge
    ///
    /// # Go Compatibility
    ///
    /// This matches Go's `processPushlogRequest()` in `p2p.go:446-530`,
    /// except the actual CRDT merge is delegated to the database layer.
    pub async fn process_pushlog(&self, msg: &PushLogBroadcast) -> Result<()> {
        // Parse CID from message
        let cid = Cid::try_from(msg.cid.as_slice())
            .map_err(|e| Error::InvalidCid(format!("Failed to parse CID: {}", e)))?;
        tracing::debug!(
            cid = %cid,
            doc_id = %msg.doc_id,
            collection_id = %msg.collection_id,
            block_len = msg.block.len(),
            "Processing pushlog"
        );

        // Try to acquire exclusive processing rights for this CID
        match self.process_queue.try_acquire(&cid).await {
            Ok(_guard) => {
                // We're the first - process the block
                self.process_block_inner(&cid, msg).await
            }
            Err(rx) => {
                // Another task is processing - wait for it
                if rx.await.is_err() {
                    tracing::debug!(
                        ?cid,
                        "First processor task was cancelled, will check merge status"
                    );
                }

                // Now check if block is already merged
                match self.blockstore.is_merged(&cid).await {
                    Ok(true) => {
                        // Already merged by the other task
                        if self
                            .event_tx
                            .send(SyncEvent::BlockAlreadyMerged { cid })
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                ?cid,
                                "Failed to send BlockAlreadyMerged event - receiver dropped"
                            );
                            return Err(Error::ChannelSend);
                        }
                        Ok(())
                    }
                    Ok(false) => {
                        // Not yet merged - we need to process it
                        // (This can happen if the first task failed)
                        self.process_block_inner(&cid, msg).await
                    }
                    Err(e) => {
                        if self
                            .event_tx
                            .send(SyncEvent::SyncError {
                                cid,
                                error: e.to_string(),
                            })
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                ?cid,
                                "Failed to send SyncError event - receiver dropped"
                            );
                            // Return channel error since we can't notify caller of the blockstore error
                            return Err(Error::ChannelSend);
                        }
                        Err(Error::BlockstoreError(e.to_string()))
                    }
                }
            }
        }
    }

    /// Inner block processing logic.
    pub(super) async fn process_block_inner(
        &self,
        cid: &Cid,
        msg: &PushLogBroadcast,
    ) -> Result<()> {
        // Check if already merged
        match self.blockstore.is_merged(cid).await {
            Ok(true) => {
                tracing::debug!(cid = %cid, doc_id = %msg.doc_id, "Block already merged, skipping");
                if self
                    .event_tx
                    .send(SyncEvent::BlockAlreadyMerged { cid: *cid })
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        ?cid,
                        "Failed to send BlockAlreadyMerged event - receiver dropped"
                    );
                    return Err(Error::ChannelSend);
                }
                return Ok(());
            }
            Ok(false) => {
                // Not merged, continue processing
            }
            Err(e) => {
                if self
                    .event_tx
                    .send(SyncEvent::SyncError {
                        cid: *cid,
                        error: e.to_string(),
                    })
                    .await
                    .is_err()
                {
                    tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
                    return Err(Error::ChannelSend);
                }
                return Err(Error::BlockstoreError(e.to_string()));
            }
        }

        // Verify CID matches block content before storing (finding 06-29).
        if let Err(e) = verify_block_cid(cid, &msg.block) {
            let p2p_err = crate::error::blockstore_verify_to_p2p(e, cid);
            tracing::warn!(
                cid = %cid,
                error = %p2p_err,
                "PushLog block failed CID verification, discarding"
            );
            return Err(p2p_err);
        }

        // Store the block (marked as unmerged in P2P mode)
        if let Err(e) = self.blockstore.put(cid, &msg.block).await {
            if self
                .event_tx
                .send(SyncEvent::SyncError {
                    cid: *cid,
                    error: e.to_string(),
                })
                .await
                .is_err()
            {
                tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
                return Err(Error::ChannelSend);
            }
            return Err(Error::BlockstoreError(e.to_string()));
        }

        tracing::debug!(
            ?cid,
            doc_id = %msg.doc_id,
            collection_id = %msg.collection_id,
            "Block stored, checking for missing links"
        );

        // Check for missing linked blocks
        let missing = match find_missing_links(self.blockstore.as_ref(), &msg.block).await {
            Ok(m) => m,
            Err(e) => {
                // Block parsing failed - emit error event and propagate error
                if self
                    .event_tx
                    .send(SyncEvent::SyncError {
                        cid: *cid,
                        error: e.to_string(),
                    })
                    .await
                    .is_err()
                {
                    tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
                    return Err(Error::ChannelSend);
                }
                return Err(e);
            }
        };

        if missing.is_empty() {
            // DAG is complete - emit BlockReceived for merge
            tracing::info!(
                ?cid,
                doc_id = %msg.doc_id,
                "DAG complete, emitting BlockReceived event"
            );

            if self
                .event_tx
                .send(SyncEvent::BlockReceived {
                    cid: *cid,
                    doc_id: msg.doc_id.clone(),
                    collection_id: msg.collection_id.clone(),
                    creator: msg.creator.clone(),
                })
                .await
                .is_err()
            {
                tracing::error!(
                    ?cid,
                    doc_id = %msg.doc_id,
                    "CRITICAL: Failed to send BlockReceived event - block stored but will not be merged. \
                     Event receiver may have been dropped."
                );
                return Err(Error::ChannelSend);
            }
        } else {
            // DAG has missing blocks - track as pending and request Bitswap fetch
            tracing::info!(
                ?cid,
                missing_count = missing.len(),
                doc_id = %msg.doc_id,
                "DAG has missing links, requesting Bitswap fetch"
            );

            // Track this DAG as pending (enforces TTL eviction and capacity limit).
            {
                let inserted = {
                    let mut pending = self.pending_dags.write();
                    let now = Instant::now();
                    pending.retain(|_, v| now.duration_since(v.inserted_at) < PENDING_DAG_TTL);
                    if pending.len() < MAX_PENDING_DAGS {
                        pending.insert(
                            *cid,
                            PendingDag {
                                doc_id: msg.doc_id.clone(),
                                collection_id: msg.collection_id.clone(),
                                creator: msg.creator.clone(),
                                missing: missing.iter().cloned().collect(),
                                source_peer: None,
                                inserted_at: now,
                            },
                        );
                        true
                    } else {
                        false
                    }
                };
                if !inserted {
                    tracing::warn!(
                        cid = %cid,
                        max = MAX_PENDING_DAGS,
                        "Pending DAGs at capacity, dropping PushLog DAG registration"
                    );
                    return Ok(());
                }
            }

            // Get providers for the missing blocks
            let providers = self.get_providers_for_cids(&missing);

            // Emit event to request Bitswap fetch
            if self
                .event_tx
                .send(SyncEvent::DagNeedsFetch {
                    root_cid: *cid,
                    missing: missing.clone(),
                    providers,
                    doc_id: msg.doc_id.clone(),
                    collection_id: msg.collection_id.clone(),
                    creator: msg.creator.clone(),
                })
                .await
                .is_err()
            {
                tracing::error!(
                    ?cid,
                    "Failed to send DagNeedsFetch event - receiver dropped"
                );
                // Clean up pending dag since we can't request fetch
                self.pending_dags.write().remove(cid);
                return Err(Error::ChannelSend);
            }
        }

        Ok(())
    }

    /// Get providers (peers that may have the blocks) for the given CIDs.
    pub(super) fn get_providers_for_cids(&self, cids: &[Cid]) -> Vec<String> {
        let mut providers = HashSet::new();

        // Add peers known to have any of the CIDs
        for cid in cids {
            for peer in self.peer_state.peers_with_cid(cid) {
                providers.insert(peer);
            }
        }

        // If no specific providers found, use all connected peers
        if providers.is_empty() {
            for peer in self.peer_state.connected_peers() {
                providers.insert(peer);
            }
        }

        providers.into_iter().collect()
    }
}
