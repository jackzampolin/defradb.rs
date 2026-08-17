//! Bitswap query tracking and block storage.

use cid::Cid;

use blockstore::{verify_block_cid, Blockstore};

use crate::error::{Error, Result};
use crate::sync::manager::events::SyncEvent;
use crate::QueryId;

use super::SyncManager;

/// Completion signal for poll-owned exact-CID queries. Transport completion
/// already exists; this tracker lets the same fetch owner stop its blockstore
/// poll immediately when a provider failed instead of burning the full window.
#[derive(Debug, Clone, Default)]
pub(crate) struct BlockSyncCompletionTracker {
    waiters: std::sync::Arc<
        parking_lot::Mutex<std::collections::HashMap<QueryId, tokio::sync::oneshot::Sender<bool>>>,
    >,
}

/// Completion signal for libp2p's two-stream rooted CAR protocol.  Request
/// dispatch and response arrival are separate streams, so blockstore polling
/// alone adds avoidable ownership latency at small admission capacities.
#[derive(Debug, Clone, Default)]
pub(crate) struct RootedCarCompletionTracker {
    waiters: std::sync::Arc<
        parking_lot::Mutex<std::collections::HashMap<Cid, tokio::sync::oneshot::Sender<bool>>>,
    >,
}

impl RootedCarCompletionTracker {
    pub(crate) fn register(&self, root_cid: Cid) -> tokio::sync::oneshot::Receiver<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.waiters.lock().insert(root_cid, tx);
        rx
    }

    pub(crate) fn complete(&self, root_cid: Cid, success: bool) -> bool {
        let Some(waiter) = self.waiters.lock().remove(&root_cid) else {
            return false;
        };
        let _ = waiter.send(success);
        true
    }

    pub(crate) fn cancel(&self, root_cid: Cid) {
        self.waiters.lock().remove(&root_cid);
    }
}

impl BlockSyncCompletionTracker {
    pub(crate) fn register(&self, query_id: QueryId) -> tokio::sync::oneshot::Receiver<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.waiters.lock().insert(query_id, tx);
        rx
    }

    pub(crate) fn complete(&self, query_id: QueryId, success: bool) -> bool {
        let Some(waiter) = self.waiters.lock().remove(&query_id) else {
            return false;
        };
        let _ = waiter.send(success);
        true
    }

    pub(crate) fn cancel(&self, query_id: QueryId) {
        self.waiters.lock().remove(&query_id);
    }
}

impl<B: Blockstore + 'static> SyncManager<B> {
    pub(crate) fn block_sync_completion_tracker(&self) -> BlockSyncCompletionTracker {
        self.block_sync_completions.clone()
    }

    pub(crate) fn rooted_car_completion_tracker(&self) -> RootedCarCompletionTracker {
        self.rooted_car_completions.clone()
    }
    /// Register a Bitswap query for tracking.
    ///
    /// This maps the QueryId to the root CID so we can identify
    /// which DAG a completion event belongs to.
    pub fn register_query(&self, query_id: QueryId, root_cid: Cid) {
        self.query_to_root.write().insert(query_id, root_cid);
    }

    /// Remove and return the root CID associated with a Bitswap query.
    pub fn take_query_root(&self, query_id: QueryId) -> Option<Cid> {
        self.query_to_root.write().remove(&query_id)
    }

    /// Handle Bitswap query completion.
    ///
    /// Called when a Bitswap sync completes (success or failure).
    pub async fn handle_bitswap_complete(
        &self,
        query_id: QueryId,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        // Find the root CID for this query
        let root_cid = match self.query_to_root.write().remove(&query_id) {
            Some(cid) => cid,
            None => {
                tracing::debug!(
                    query_id = ?query_id,
                    "Bitswap complete for unknown query, ignoring"
                );
                return Ok(());
            }
        };

        if success {
            // All blocks fetched - emit BlockReceived for the root
            let dag = self.pending_dags.write().remove(&root_cid);
            match dag {
                Some(dag) => {
                    tracing::info!(
                        cid = %root_cid,
                        doc_id = %dag.doc_id,
                        "Bitswap sync complete, emitting BlockReceived"
                    );

                    if self
                        .event_tx
                        .send(SyncEvent::BlockReceived {
                            cid: root_cid,
                            doc_id: dag.doc_id,
                            collection_id: dag.collection_id,
                            creator: dag.creator,
                            sender_peer: dag.source_peer,
                            is_explicit_replicator: dag.is_explicit_replicator,
                            explicit_replay_authorization: dag.explicit_replay_authorization,
                        })
                        .await
                        .is_err()
                    {
                        tracing::error!(
                            cid = %root_cid,
                            "Failed to send BlockReceived after Bitswap complete - receiver dropped"
                        );
                        return Err(Error::ChannelSend);
                    }
                }
                None => {
                    // This can happen if the DAG was processed by another path,
                    // cleaned up, or if there's a race condition
                    tracing::warn!(
                        cid = %root_cid,
                        "Bitswap sync completed but no pending DAG found - \
                         DAG may have been processed by another path or cleaned up"
                    );
                }
            }
        } else {
            // Sync failed - emit error, clean up
            self.pending_dags.write().remove(&root_cid);

            let error_msg = error.unwrap_or_else(|| "Bitswap sync failed".to_string());
            tracing::warn!(
                cid = %root_cid,
                error = %error_msg,
                "Bitswap sync failed"
            );

            if self
                .event_tx
                .send(SyncEvent::SyncError {
                    cid: root_cid,
                    error: error_msg,
                })
                .await
                .is_err()
            {
                tracing::warn!(
                    cid = %root_cid,
                    "Failed to send SyncError event - receiver dropped"
                );
                return Err(Error::ChannelSend);
            }
        }

        Ok(())
    }

    /// Store a block received via Bitswap and check if pending DAGs can now proceed.
    ///
    /// This is called when blocks are fetched via Bitswap during DAG synchronization.
    /// The block is stored in the blockstore, and we check if any pending DAGs are
    /// now complete and can be processed.
    ///
    /// Returns `true` if the block was stored (not a duplicate).
    pub async fn store_bitswap_block(&self, cid: &Cid, data: &[u8]) -> Result<bool> {
        // PushLog, CAR, Bitswap and merge all mutate the same per-CID merge
        // marker. Keep one storage owner instead of relying on retries to
        // resolve competing writers.
        let _storage_owner = loop {
            match self.process_queue.try_acquire(cid).await {
                Ok(guard) => break guard,
                Err(waiter) => {
                    let _ = waiter.await;
                }
            }
        };

        // Check if we already have the block
        if self
            .blockstore
            .has(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))?
        {
            tracing::debug!(
                cid = %cid,
                "Bitswap block already in blockstore (duplicate)"
            );
            return Ok(false);
        }

        // Verify CID matches block content before storing (findings 06-29, 06-23, 06-24).
        if let Err(e) = verify_block_cid(cid, data) {
            let p2p_err = crate::error::blockstore_verify_to_p2p(e, cid);
            tracing::warn!(
                cid = %cid,
                error = %p2p_err,
                "Bitswap block failed CID verification, discarding"
            );
            return Err(p2p_err);
        }

        // Store the block
        if let Err(e) = self.blockstore.put(cid, data).await {
            tracing::error!(
                cid = %cid,
                error = %e,
                "Failed to store Bitswap block"
            );
            return Err(Error::BlockstoreError(e.to_string()));
        }

        tracing::info!(
            cid = %cid,
            data_len = data.len(),
            "Stored Bitswap block in blockstore"
        );

        for root_cid in self.pending_dags.read().waiting_roots(cid) {
            tracing::debug!(
                root_cid = %root_cid,
                received_cid = %cid,
                "Pending DAG received a missing block - will check completeness"
            );
        }

        Ok(true)
    }
}

#[cfg(test)]
mod completion_tracker_tests {
    use super::*;

    #[tokio::test]
    async fn poll_owner_observes_transport_completion_exactly_once() {
        let tracker = BlockSyncCompletionTracker::default();
        let query_id = QueryId(42);
        let receiver = tracker.register(query_id);

        assert!(tracker.complete(query_id, false));
        assert!(!receiver.await.expect("completion sender alive"));
        assert!(!tracker.complete(query_id, true));
    }

    #[tokio::test]
    async fn cancelled_poll_owner_does_not_retain_a_completion_waiter() {
        let tracker = BlockSyncCompletionTracker::default();
        let query_id = QueryId(43);
        let receiver = tracker.register(query_id);
        tracker.cancel(query_id);

        assert!(receiver.await.is_err());
        assert!(!tracker.complete(query_id, false));
    }
}
