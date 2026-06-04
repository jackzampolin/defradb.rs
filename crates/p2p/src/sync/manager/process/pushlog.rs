//! PushLog processing and block storage.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use cid::Cid;

use blockstore::{verify_block_cid, Blockstore};

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;
use crate::sync::manager::events::SyncEvent;
use crate::sync::manager::links::find_all_missing_links;
use crate::sync::manager::pending::{PendingDag, MAX_PENDING_DAGS};
use crate::ExplicitReplayAuthorization;

use super::SyncManager;

const MAX_RETRIABLE_PUSHLOG_ATTEMPTS: usize = 4;

fn retriable_pushlog_delay(attempt: usize) -> Duration {
    match attempt {
        1 => Duration::from_millis(10),
        2 => Duration::from_millis(25),
        _ => Duration::from_millis(50),
    }
}

impl<B: Blockstore + 'static> SyncManager<B> {
    async fn retry_retriable_pushlog_op<T, F, Fut>(
        &self,
        cid: &Cid,
        op_name: &'static str,
        mut op: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 1;
        loop {
            match op().await {
                Ok(value) => return Ok(value),
                Err(error) if error.is_retriable() && attempt < MAX_RETRIABLE_PUSHLOG_ATTEMPTS => {
                    tracing::debug!(
                        cid = %cid,
                        op_name,
                        attempt,
                        max_attempts = MAX_RETRIABLE_PUSHLOG_ATTEMPTS,
                        error = %error,
                        "Retryable PushLog storage operation failed; backing off and retrying"
                    );
                    tokio::time::sleep(retriable_pushlog_delay(attempt)).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn emit_sync_error(&self, cid: &Cid, error: &Error) -> Result<()> {
        if self
            .event_tx
            .send(SyncEvent::SyncError {
                cid: *cid,
                error: error.to_string(),
            })
            .await
            .is_err()
        {
            tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
            return Err(Error::ChannelSend);
        }
        Ok(())
    }

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
    /// 5. Emit BlockReceived only once the full reachable DAG is locally present,
    ///    otherwise emit DagNeedsFetch for the missing descendants
    ///
    /// # Go Compatibility
    ///
    /// This matches Go's `processPushlogRequest()` in `p2p.go:446-530`,
    /// except the actual CRDT merge is delegated to the database layer.
    pub async fn process_pushlog(
        &self,
        msg: &PushLogBroadcast,
        sender_peer: Option<&str>,
        is_explicit_replicator: bool,
        explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    ) -> Result<()> {
        // Parse CID from message
        let cid = Cid::try_from(msg.cid.as_ref())
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
                self.process_block_inner(
                    &cid,
                    msg,
                    sender_peer,
                    is_explicit_replicator,
                    explicit_replay_authorization.clone(),
                )
                .await
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
                match self
                    .retry_retriable_pushlog_op(&cid, "post_wait_is_merged", || async {
                        self.blockstore
                            .is_merged(&cid)
                            .await
                            .map_err(Error::from_blockstore)
                    })
                    .await
                {
                    Ok(true) => {
                        // Already merged by the other task
                        if self
                            .event_tx
                            .send(SyncEvent::BlockAlreadyMerged {
                                cid,
                                doc_id: msg.doc_id.clone(),
                                collection_id: msg.collection_id.clone(),
                                creator: msg.creator.clone(),
                            })
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
                        self.process_block_inner(
                            &cid,
                            msg,
                            sender_peer,
                            is_explicit_replicator,
                            explicit_replay_authorization.clone(),
                        )
                        .await
                    }
                    Err(e) => {
                        self.emit_sync_error(&cid, &e).await?;
                        Err(e)
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
        sender_peer: Option<&str>,
        is_explicit_replicator: bool,
        explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    ) -> Result<()> {
        // Check if already merged
        match self
            .retry_retriable_pushlog_op(cid, "is_merged", || async {
                self.blockstore
                    .is_merged(cid)
                    .await
                    .map_err(Error::from_blockstore)
            })
            .await
        {
            Ok(true) => {
                tracing::debug!(cid = %cid, doc_id = %msg.doc_id, "Block already merged, skipping");
                if self
                    .event_tx
                    .send(SyncEvent::BlockAlreadyMerged {
                        cid: *cid,
                        doc_id: msg.doc_id.clone(),
                        collection_id: msg.collection_id.clone(),
                        creator: msg.creator.clone(),
                    })
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
                self.emit_sync_error(cid, &e).await?;
                return Err(e);
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
        if let Err(e) = self
            .retry_retriable_pushlog_op(cid, "put_block", || async {
                self.blockstore
                    .put(cid, &msg.block)
                    .await
                    .map_err(Error::from_blockstore)
            })
            .await
        {
            self.emit_sync_error(cid, &e).await?;
            return Err(e);
        }

        tracing::debug!(
            ?cid,
            doc_id = %msg.doc_id,
            collection_id = %msg.collection_id,
            "Block stored, checking DAG for missing links"
        );

        // Check for missing linked blocks at every depth of the reachable DAG.
        // A single-level check can incorrectly declare Collection -> Composite
        // roots complete while nested field blocks are still missing locally.
        let missing = match find_all_missing_links(self.blockstore.as_ref(), &msg.block).await {
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
                    sender_peer: sender_peer.map(str::to_owned),
                    is_explicit_replicator,
                    explicit_replay_authorization,
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

            // Not wrapped in retry_retriable_pushlog_op: the inner blockstore
            // reads already propagate typed `BlockstoreTxnConflict` via
            // `Error::from_blockstore`, and those are the only retriable errors
            // surfaced here. DAG-traversal failures (missing links, bitswap
            // timeouts, channel-send) are terminal in this context, so an outer
            // retry would not make progress.
            match self.retry_pending_dags_waiting_on(cid).await {
                Ok(completed_roots) => {
                    if !completed_roots.is_empty() {
                        tracing::info!(
                            received_cid = %cid,
                            completed_count = completed_roots.len(),
                            completed_roots = ?completed_roots,
                            "Late PushLog block completed pending DAGs"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        received_cid = %cid,
                        error = %e,
                        "Failed to retry pending DAGs after PushLog block arrival"
                    );
                    return Err(e);
                }
            }
        } else {
            // DAG has missing blocks - track as pending and request Bitswap fetch.
            // Debug level: this fires per PushLog with missing links and is
            // expected during catch-up; the terminal outcome is logged at info
            // (DagReady) or warn (final failure) — see issue #858.
            tracing::debug!(
                ?cid,
                missing_count = missing.len(),
                doc_id = %msg.doc_id,
                collection_id = %msg.collection_id,
                "DAG has missing links, requesting Bitswap fetch"
            );

            // Track this DAG as pending (enforces TTL eviction and capacity limit).
            {
                let inserted = self.insert_pending_dag(
                    *cid,
                    PendingDag {
                        doc_id: msg.doc_id.clone(),
                        collection_id: msg.collection_id.clone(),
                        creator: msg.creator.clone(),
                        missing: missing.iter().cloned().collect(),
                        source_peer: sender_peer.map(str::to_owned),
                        is_explicit_replicator,
                        explicit_replay_authorization: explicit_replay_authorization.clone(),
                        inserted_at: Instant::now(),
                        attempts: 0,
                        fetch_failures: 0,
                        last_fetch_error: None,
                    },
                );
                if !inserted {
                    tracing::warn!(
                        cid = %cid,
                        doc_id = %msg.doc_id,
                        collection_id = %msg.collection_id,
                        source_peer = ?sender_peer,
                        missing_count = missing.len(),
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
                    sender_peer: sender_peer.map(str::to_owned),
                    is_explicit_replicator,
                    explicit_replay_authorization,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use blockstore::DefraBlockstore;
    use bytes::Bytes;
    use defra_core::{
        Block, CollectionDeltaPayload, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload,
    };
    use storage::backends::MemoryStore;

    use crate::sync::{PeerStateTracker, SyncConfig};

    fn create_lww_block(field_name: &str) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                doc_id: b"doc123".to_vec(),
                field_name: field_name.to_string(),
                priority: 1,
                schema_version_id: "schema1".to_string(),
                data: b"value".to_vec(),
            }),
            vec![],
            vec![],
        );
        let bytes = block.to_dag_cbor().expect("encode lww block");
        let cid = block.generate_cid().expect("generate lww cid");
        (cid, bytes)
    }

    fn create_composite_block(doc_id: &str, field_name: &str, field_cid: Cid) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: doc_id.as_bytes().to_vec(),
                schema_version_id: "schema1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new(field_name, field_cid)],
        );
        let bytes = block.to_dag_cbor().expect("encode composite block");
        let cid = block.generate_cid().expect("generate composite cid");
        (cid, bytes)
    }

    fn create_collection_block(
        schema_version_id: &str,
        doc_id: &str,
        composite_cid: Cid,
    ) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Collection(CollectionDeltaPayload {
                schema_version_id: schema_version_id.to_string(),
                priority: 1,
            }),
            vec![],
            vec![DAGLink::new(doc_id, composite_cid)],
        );
        let bytes = block.to_dag_cbor().expect("encode collection block");
        let cid = block.generate_cid().expect("generate collection cid");
        (cid, bytes)
    }

    fn make_broadcast(
        doc_id: &str,
        cid: Cid,
        block: Vec<u8>,
        collection_id: &str,
    ) -> PushLogBroadcast {
        PushLogBroadcast::new(
            doc_id.to_string(),
            Bytes::from(cid.to_bytes()),
            collection_id.to_string(),
            "creator1".to_string(),
            Bytes::from(block),
        )
    }

    #[tokio::test]
    async fn process_pushlog_tracks_nested_missing_links_before_merge() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) =
            SyncManager::new(blockstore.clone(), peer_state, SyncConfig::default());

        let (field_cid, _field_block) = create_lww_block("name");
        let (composite_cid, composite_block) = create_composite_block("doc123", "name", field_cid);
        blockstore
            .put(&composite_cid, &composite_block)
            .await
            .expect("store composite block");

        let (collection_cid, collection_block) =
            create_collection_block("schema1", "doc123", composite_cid);

        manager
            .process_pushlog(
                &make_broadcast("doc123", collection_cid, collection_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("process pushlog");

        match events.try_recv().expect("DagNeedsFetch event") {
            SyncEvent::DagNeedsFetch {
                root_cid,
                missing,
                doc_id,
                collection_id,
                sender_peer,
                ..
            } => {
                assert_eq!(root_cid, collection_cid);
                assert_eq!(missing, vec![field_cid]);
                assert_eq!(doc_id, "doc123");
                assert_eq!(collection_id, "collection1");
                assert_eq!(sender_peer.as_deref(), Some("peer-1"));
            }
            other => panic!("expected DagNeedsFetch, got {:?}", other),
        }

        assert_eq!(manager.pending_dag_count(), 1);
        assert_eq!(
            manager.pending_dag_missing(&collection_cid),
            vec![field_cid]
        );
    }

    #[tokio::test]
    async fn process_pushlog_clears_pending_dag_when_fetch_event_receiver_is_dropped() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, events) =
            SyncManager::new(blockstore.clone(), peer_state, SyncConfig::default());
        drop(events);

        let (field_cid, _field_block) = create_lww_block("name");
        let (composite_cid, composite_block) = create_composite_block("doc123", "name", field_cid);
        blockstore
            .put(&composite_cid, &composite_block)
            .await
            .expect("store composite block");

        let (collection_cid, collection_block) =
            create_collection_block("schema1", "doc123", composite_cid);

        let result = manager
            .process_pushlog(
                &make_broadcast("doc123", collection_cid, collection_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(Error::ChannelSend)));
        assert_eq!(manager.pending_dag_count(), 0);
    }
}
