//! PushLog processing and block storage.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use cid::Cid;

use blockstore::{verify_block_cid, Blockstore};

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;
use crate::sync::manager::events::SyncEvent;
use crate::sync::manager::links::find_all_missing_links;
use crate::sync::manager::pending::PendingDag;
use crate::ExplicitReplayAuthorization;

use super::SyncManager;

const MAX_RETRIABLE_PUSHLOG_ATTEMPTS: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AnnouncedBlockKind {
    Head(u64),
    Descendant,
}

impl AnnouncedBlockKind {
    fn priority(self) -> Option<u64> {
        match self {
            Self::Head(priority) => Some(priority),
            Self::Descendant => None,
        }
    }
}

fn announced_block_kind(bytes: &[u8]) -> AnnouncedBlockKind {
    let Ok(block) = defra_core::Block::from_dag_cbor(bytes) else {
        return AnnouncedBlockKind::Descendant;
    };
    match &block.delta {
        defra_core::CrdtDelta::Composite(_) | defra_core::CrdtDelta::Collection(_) => {
            AnnouncedBlockKind::Head(block.delta.priority())
        }
        _ => AnnouncedBlockKind::Descendant,
    }
}

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
    /// 2. Serialize authorized replay or cheaply suppress an ordinary duplicate
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

        let _guard = if explicit_replay_authorization.is_some() {
            loop {
                match self.process_queue.try_acquire(&cid).await {
                    Ok(guard) => break guard,
                    Err(waiter) => {
                        let _ = waiter.await;
                    }
                }
            }
        } else {
            match self.process_queue.try_acquire_nowait(&cid) {
                Some(guard) => guard,
                None => {
                    self.diagnostics.record_single_flight_suppressed();
                    tracing::debug!(
                        cid = %cid,
                        sender_peer = ?sender_peer,
                        "Suppressing PushLog while the same CID is already being processed"
                    );

                    if self.is_pending_dag_recovery_registered(&cid) {
                        return Ok(());
                    }

                    match self
                        .retry_retriable_pushlog_op(&cid, "suppressed_is_merged", || async {
                            self.blockstore
                                .is_merged(&cid)
                                .await
                                .map_err(Error::from_blockstore)
                        })
                        .await
                    {
                        Ok(true) => {
                            self.diagnostics.record_already_merged_fast_path();
                            return Ok(());
                        }
                        Ok(false) => {
                            return Err(Error::PushLogInFlight {
                                cid: cid.to_string(),
                            });
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        };

        self.process_block_inner(
            &cid,
            msg,
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization,
        )
        .await
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
                self.diagnostics.record_already_merged_fast_path();
                tracing::debug!(cid = %cid, doc_id = %msg.doc_id, "Block already merged, skipping");
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

        let announced_block_kind = announced_block_kind(&msg.block);
        let head_priority = announced_block_kind.priority();
        if !self.can_process_pushlog(cid)
            && !self.scope_head_is_refresh_or_newer(
                *cid,
                sender_peer,
                &msg.collection_id,
                &msg.doc_id,
                head_priority,
            )
        {
            self.diagnostics.record_pending_dag_capacity_shed();
            tracing::warn!(
                cid = %cid,
                doc_id = %msg.doc_id,
                collection_id = %msg.collection_id,
                source_peer = ?sender_peer,
                max = self.max_pending_dags,
                "Pending DAGs at capacity, shedding PushLog before block verification"
            );
            return Err(Error::PendingDagCapacity {
                max: self.max_pending_dags,
            });
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

        // Rolling old Rust senders may still announce dependency blocks before
        // the composite/collection head. Keep those bytes as useful CAR
        // descendants, and advance any root already waiting on them, but never
        // admit or merge them as standalone document heads (#1450). The later
        // head hint remains the sole durable receiver obligation.
        if announced_block_kind == AnnouncedBlockKind::Descendant {
            tracing::debug!(
                cid = %cid,
                doc_id = %msg.doc_id,
                collection_id = %msg.collection_id,
                "Stored legacy dependency PushLog without treating it as a head"
            );
            self.retry_pending_dags_waiting_on(cid).await?;
            return Ok(());
        }

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

            // Different CIDs for one sender/scope must make one serialized
            // durable replacement decision. Otherwise concurrent heartbeats
            // can both observe the old head and recreate a per-root ledger.
            let _registration_writer = self.pending_registration_writer.lock().await;
            let durable_superseded_root = match self.persisted_scope_decision(
                *cid,
                sender_peer,
                &msg.collection_id,
                &msg.doc_id,
                head_priority,
            ) {
                super::PersistedScopeDecision::CoveredByCurrent => {
                    tracing::debug!(
                        cid = %cid,
                        doc_id = %msg.doc_id,
                        collection_id = %msg.collection_id,
                        source_peer = ?sender_peer,
                        "Incoming head is covered by a newer durable sender/scope obligation"
                    );
                    return Ok(());
                }
                super::PersistedScopeDecision::Supersedes(root) => Some(root),
                super::PersistedScopeDecision::Independent
                | super::PersistedScopeDecision::Current => None,
            };

            // Track this DAG as pending (enforces TTL eviction, capacity, and
            // current-head retirement for one sender/document-or-collection scope).
            let inserted_at = Instant::now();
            let superseded = {
                use super::pending_dag::PendingDagAdmission;
                let admission = self.try_insert_pending_dag(
                    *cid,
                    PendingDag {
                        doc_id: msg.doc_id.clone(),
                        collection_id: msg.collection_id.clone(),
                        head_priority,
                        creator: msg.creator.clone(),
                        missing: missing.iter().cloned().collect(),
                        source_peer: sender_peer.map(str::to_owned),
                        is_explicit_replicator,
                        explicit_replay_authorization: explicit_replay_authorization.clone(),
                        is_recovery_registered: false,
                        inserted_at,
                        attempts: 0,
                        fetch_failures: 0,
                        last_fetch_error: None,
                        next_retry_at: tokio::time::Instant::now(),
                        dispatches: 0,
                    },
                );
                // Report the limit that actually tripped so the nack and its
                // WARN log agree (the global cap vs the smaller per-peer quota).
                let rejected_max = match &admission {
                    PendingDagAdmission::Admitted { .. }
                    | PendingDagAdmission::CoveredByCurrent => None,
                    PendingDagAdmission::GlobalCapacity => Some(self.max_pending_dags),
                    PendingDagAdmission::PeerQuota { max_per_peer } => Some(*max_per_peer),
                };
                if let Some(max) = rejected_max {
                    self.diagnostics.record_pending_dag_capacity_shed();
                    // The block is stored but its DAG completion is not
                    // tracked. This must surface as an error: a success reply
                    // deletes the pusher's retry record, silently losing the
                    // document. The reply seams map this typed error to the
                    // at-capacity nack so the pusher retains and retries it.
                    tracing::warn!(
                        cid = %cid,
                        doc_id = %msg.doc_id,
                        collection_id = %msg.collection_id,
                        source_peer = ?sender_peer,
                        missing_count = missing.len(),
                        max,
                        max_per_peer = self.max_pending_dags_per_peer(),
                        "Pending DAGs at capacity, rejecting PushLog DAG registration"
                    );
                    return Err(Error::PendingDagCapacity { max });
                }
                match admission {
                    PendingDagAdmission::Admitted { superseded } => *superseded,
                    PendingDagAdmission::CoveredByCurrent => {
                        tracing::debug!(
                            cid = %cid,
                            doc_id = %msg.doc_id,
                            collection_id = %msg.collection_id,
                            source_peer = ?sender_peer,
                            "Incoming head is covered by the current durable sender/scope obligation"
                        );
                        return Ok(());
                    }
                    PendingDagAdmission::GlobalCapacity | PendingDagAdmission::PeerQuota { .. } => {
                        unreachable!("handled above")
                    }
                }
            };
            let superseded_root =
                durable_superseded_root.or_else(|| superseded.as_ref().map(|(root, _)| *root));

            // Persist the registration before the caller acks success: the
            // ack destroys the pusher's retry record, so an unpersisted
            // registration must fail closed as an error reply instead
            // (#1099; proofs/tla/PendingDagRestart.tla INV_AckBacked).
            // Durable records outlive TTL-evicted map entries, so they carry
            // their own larger cap; at the cap the obligation is refused
            // (backpressure nack) while the pusher still owns retry state.
            let has_durable_registration = if let Some(store) = self.pending_store() {
                let durable_cap = self
                    .max_pending_dags
                    .saturating_mul(super::PERSISTED_PENDING_CAP_FACTOR);
                // Check-and-reserve atomically under the write lock so the
                // cap is hard under concurrent PushLogs; a failed put below
                // releases the reservation. `newly_reserved` is false when
                // the root already holds a record (re-push refresh).
                enum DurableAdmission {
                    Reserved,
                    AlreadyPresent,
                    AtCapacity,
                }
                let admission = {
                    let mut roots = self.persisted_roots.write();
                    if roots.contains(cid) {
                        DurableAdmission::AlreadyPresent
                    } else if roots.len() >= durable_cap
                        && superseded_root.is_none_or(|old| !roots.contains(&old))
                    {
                        DurableAdmission::AtCapacity
                    } else {
                        roots.insert(*cid);
                        if let Some(old) = superseded_root {
                            roots.remove(&old);
                        }
                        DurableAdmission::Reserved
                    }
                };
                if matches!(admission, DurableAdmission::AtCapacity) {
                    self.diagnostics.record_pending_dag_capacity_shed();
                    self.pending_dags.write().remove(cid);
                    if let Some((old_root, old_dag)) = superseded.clone() {
                        self.pending_dags.write().insert(old_root, old_dag);
                    }
                    tracing::warn!(
                        cid = %cid,
                        doc_id = %msg.doc_id,
                        durable_cap,
                        "Durable pending DAG registrations at capacity, rejecting PushLog DAG registration"
                    );
                    return Err(Error::PendingDagCapacity { max: durable_cap });
                }
                let newly_reserved = matches!(admission, DurableAdmission::Reserved);
                let record = crate::sync::pending_store::PersistedPendingDag {
                    doc_id: msg.doc_id.clone(),
                    collection_id: msg.collection_id.clone(),
                    head_priority,
                    creator: msg.creator.clone(),
                    source_peer: sender_peer.map(str::to_owned),
                    is_explicit_replicator,
                    explicit_replay_authorization: explicit_replay_authorization
                        .as_ref()
                        .map(Into::into),
                };
                if let Err(error) = store
                    .replace_scope_head(superseded_root.as_ref(), cid, &record)
                    .await
                {
                    if newly_reserved {
                        self.persisted_roots.write().remove(cid);
                        if let Some(old) = superseded_root {
                            self.persisted_roots.write().insert(old);
                        }
                    }
                    self.pending_dags.write().remove(cid);
                    if let Some((old_root, old_dag)) = superseded.clone() {
                        self.pending_dags.write().insert(old_root, old_dag);
                    }
                    tracing::warn!(
                        cid = %cid,
                        doc_id = %msg.doc_id,
                        error = %error,
                        "Failed to persist pending DAG registration; nacking push"
                    );
                    return Err(Error::Storage(format!(
                        "failed to persist pending DAG registration: {error}"
                    )));
                }
                self.diagnostics
                    .observe_persisted_pending_dag_depth(self.persisted_roots.read().len());
                self.remember_persisted_scope_head(
                    *cid,
                    sender_peer,
                    &msg.collection_id,
                    &msg.doc_id,
                    head_priority,
                );
                self.diagnostics.record_pending_dag_registered();
                self.mark_pending_dag_recovery_registered(cid, inserted_at);
                if let Some(old) = superseded_root {
                    tracing::debug!(
                        old_root_cid = %old,
                        root_cid = %cid,
                        doc_id = %msg.doc_id,
                        collection_id = %msg.collection_id,
                        source_peer = ?sender_peer,
                        "Durably superseded older pending head for sender/scope"
                    );
                }
                tracing::debug!(
                    target: "p2p::sync::restart_recovery",
                    cid = %cid,
                    doc_id = %msg.doc_id,
                    "Persisted pending DAG registration"
                );
                true
            } else {
                false
            };

            // A fresh registration is immediately due (`insert_pending_dag`
            // leaves `next_retry_at = now`); claim it only after durable
            // replacement succeeds so the fetch owner cannot race rollback.
            if !self.try_claim_pending_dag_dispatch(cid, tokio::time::Instant::now()) {
                tracing::debug!(
                    cid = %cid,
                    doc_id = %msg.doc_id,
                    collection_id = %msg.collection_id,
                    "Pending DAG fetch was already claimed; leaving redrive to the receiver clock"
                );
                return Ok(());
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
                // Clean up the in-memory entry since we can't request the
                // fetch; the durable record stays and is re-driven by the
                // resync sweep (the pusher was nacked, so it also retries).
                self.pending_dags.write().remove(cid);
                return Err(Error::ChannelSend);
            }
            if !has_durable_registration {
                self.mark_pending_dag_recovery_registered(cid, inserted_at);
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

    use async_trait::async_trait;
    use blockstore::DefraBlockstore;
    use bytes::Bytes;
    use defra_core::{
        Block, CollectionDeltaPayload, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload,
    };
    use storage::backends::MemoryStore;
    use tokio::sync::Notify;

    use crate::sync::pending_store::{
        PendingDagStorage, PendingDagStore, PersistedPendingDag, PersistedQuarantinedDag,
    };
    use crate::sync::{PeerStateTracker, SyncConfig};

    struct BlockingPendingDagStore {
        inner: PendingDagStore<MemoryStore>,
        replace_entered: Notify,
        replace_release: Notify,
    }

    impl BlockingPendingDagStore {
        fn new(store: Arc<MemoryStore>) -> Self {
            Self {
                inner: PendingDagStore::new(store),
                replace_entered: Notify::new(),
                replace_release: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl PendingDagStorage for BlockingPendingDagStore {
        async fn put(&self, root_cid: &Cid, record: &PersistedPendingDag) -> Result<()> {
            self.inner.put(root_cid, record).await
        }

        async fn replace_scope_head(
            &self,
            superseded_root: Option<&Cid>,
            root_cid: &Cid,
            record: &PersistedPendingDag,
        ) -> Result<()> {
            self.replace_entered.notify_one();
            self.replace_release.notified().await;
            self.inner
                .replace_scope_head(superseded_root, root_cid, record)
                .await
        }

        async fn remove(&self, root_cid: &Cid) -> Result<()> {
            self.inner.remove(root_cid).await
        }

        async fn load_all(&self) -> Result<Vec<(Cid, PersistedPendingDag)>> {
            self.inner.load_all().await
        }

        async fn quarantine(&self, root_cid: &Cid, entry: &PersistedQuarantinedDag) -> Result<()> {
            self.inner.quarantine(root_cid, entry).await
        }

        async fn is_quarantined(&self, root_cid: &Cid) -> Result<bool> {
            self.inner.is_quarantined(root_cid).await
        }

        async fn load_quarantined(&self) -> Result<Vec<(Cid, PersistedQuarantinedDag)>> {
            self.inner.load_quarantined().await
        }

        async fn remove_quarantined(&self, root_cid: &Cid) -> Result<()> {
            self.inner.remove_quarantined(root_cid).await
        }
    }

    fn create_lww_block(field_name: &str) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
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

    fn create_composite_block(_doc_id: &str, field_name: &str, field_cid: Cid) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
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
    async fn legacy_dependency_pushlog_is_stored_without_becoming_a_head() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) =
            SyncManager::new(blockstore.clone(), peer_state, SyncConfig::default());

        let metadata = defra_core::cbor::to_vec(&"signature-metadata").unwrap();
        let metadata_cid = defra_core::block::generate_cid_from_bytes(&metadata).unwrap();
        manager
            .process_pushlog(
                &make_broadcast("doc123", metadata_cid, metadata, "collection1"),
                Some("old-rust-peer"),
                true,
                None,
            )
            .await
            .expect("legacy metadata should remain usable as a descendant");
        assert!(blockstore
            .has(&metadata_cid)
            .await
            .expect("metadata blockstore lookup"));
        assert_eq!(manager.pending_dag_count(), 0);
        assert!(events.try_recv().is_err());

        let (field_cid, field_block) = create_lww_block("name");
        manager
            .process_pushlog(
                &make_broadcast("doc123", field_cid, field_block, "collection1"),
                Some("old-rust-peer"),
                true,
                None,
            )
            .await
            .expect("legacy dependency should remain wire-compatible");

        assert!(blockstore.has(&field_cid).await.expect("blockstore lookup"));
        assert_eq!(manager.pending_dag_count(), 0);
        assert!(
            events.try_recv().is_err(),
            "a field block must not be merged or registered as a document head"
        );

        let (head_cid, head_block) = create_composite_block("doc123", "name", field_cid);
        manager
            .process_pushlog(
                &make_broadcast("doc123", head_cid, head_block, "collection1"),
                Some("old-rust-peer"),
                true,
                None,
            )
            .await
            .expect("the later composite head should use the stored descendant");

        assert!(matches!(
            events.try_recv(),
            Ok(SyncEvent::BlockReceived { cid, .. }) if cid == head_cid
        ));
        assert_eq!(manager.pending_dag_count(), 0);
    }

    #[tokio::test]
    async fn durable_registration_does_not_emit_after_receiver_clock_claims_fetch() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store.clone(), true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) =
            SyncManager::new(blockstore.clone(), peer_state, SyncConfig::default());
        let pending_store = Arc::new(BlockingPendingDagStore::new(store));
        manager
            .install_pending_dag_store(pending_store.clone())
            .await;
        let manager = Arc::new(manager);

        let (field_cid, _field_block) = create_lww_block("name");
        let (root_cid, root_block) = create_composite_block("doc123", "name", field_cid);
        let message = make_broadcast("doc123", root_cid, root_block, "collection1");

        let process_manager = Arc::clone(&manager);
        let process = tokio::spawn(async move {
            process_manager
                .process_pushlog(&message, Some("peer-1"), false, None)
                .await
        });

        pending_store.replace_entered.notified().await;
        let claimed = manager.claim_due_pending_dag_retries(tokio::time::Instant::now());
        assert_eq!(
            claimed.len(),
            1,
            "the receiver clock should win the fetch claim"
        );
        assert_eq!(claimed[0].0, root_cid);
        pending_store.replace_release.notify_one();

        process
            .await
            .expect("PushLog task should not panic")
            .expect("durable registration should still succeed");
        assert!(
            events.try_recv().is_err(),
            "the PushLog path must not emit after another receiver path claims the root"
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

    #[tokio::test]
    async fn conformance_same_cid_concurrent_announcements_are_idempotent() {
        const ANNOUNCEMENT_COUNT: usize = 8;

        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let config = SyncConfig {
            event_buffer_size: 1,
            ..SyncConfig::default()
        };
        let (manager, mut events) = SyncManager::new(blockstore, peer_state, config);
        let manager = Arc::new(manager);

        let (field_cid, _field_block) = create_lww_block("name");
        let (root_cid, root_block) = create_composite_block("doc123", "name", field_cid);
        let message = Arc::new(make_broadcast(
            "doc123",
            root_cid,
            root_block,
            "collection1",
        ));

        manager
            .event_tx
            .send(SyncEvent::SyncError {
                cid: root_cid,
                error: "hold event channel full".to_string(),
            })
            .await
            .expect("prefill event channel");

        let owner_manager = Arc::clone(&manager);
        let owner_message = Arc::clone(&message);
        let owner = tokio::spawn(async move {
            owner_manager
                .process_pushlog(&owner_message, Some("peer-0"), false, None)
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.pending_dag_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner should register the pending DAG");

        let mut suppressed = Vec::new();
        for peer in 1..ANNOUNCEMENT_COUNT {
            let manager = Arc::clone(&manager);
            let message = Arc::clone(&message);
            suppressed.push(tokio::spawn(async move {
                let peer = format!("peer-{peer}");
                manager
                    .process_pushlog(&message, Some(&peer), false, None)
                    .await
            }));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            for task in suppressed {
                let result = task.await.expect("suppressed task should not panic");
                assert!(
                    matches!(
                        result,
                        Err(Error::PushLogInFlight { ref cid }) if cid == &root_cid.to_string()
                    ),
                    "a duplicate must not ack before the owner establishes recovery state"
                );
            }
        })
        .await
        .expect("same-CID announcements should exit while the owner is still in flight");

        assert_eq!(
            manager.diagnostics().snapshot().single_flight_suppressed,
            (ANNOUNCEMENT_COUNT - 1) as u64
        );
        assert_eq!(manager.pending_dag_count(), 1);
        assert_eq!(manager.process_queue.active_count(), 1);

        assert!(matches!(
            events.recv().await,
            Some(SyncEvent::SyncError { .. })
        ));
        owner
            .await
            .expect("owner task should not panic")
            .expect("owner should complete");

        assert!(matches!(
            events.recv().await,
            Some(SyncEvent::DagNeedsFetch { root_cid: cid, .. }) if cid == root_cid
        ));
        assert_eq!(manager.pending_dag_count(), 1);
        assert_eq!(manager.process_queue.active_count(), 0);

        let _guard = manager
            .process_queue
            .try_acquire_nowait(&root_cid)
            .expect("simulate a later receive owner");
        manager
            .process_pushlog(&message, Some("peer-8"), false, None)
            .await
            .expect("an established pending registration can ack a duplicate");
    }

    #[tokio::test]
    async fn explicit_replay_waits_for_in_flight_announcement() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let config = SyncConfig {
            event_buffer_size: 1,
            ..SyncConfig::default()
        };
        let (manager, mut events) = SyncManager::new(blockstore.clone(), peer_state, config);
        let manager = Arc::new(manager);
        let (field_cid, field_block) = create_lww_block("name");
        blockstore
            .put(&field_cid, &field_block)
            .await
            .expect("store composite dependency");
        let (cid, block) = create_composite_block("doc123", "name", field_cid);
        let message = Arc::new(make_broadcast("doc123", cid, block, "collection1"));
        let authorization = ExplicitReplayAuthorization {
            source_peer_id: "peer-1".to_string(),
            target_peer_id: "peer-2".to_string(),
            collection_id: "collection1".to_string(),
            authorizer_did: "creator1".to_string(),
            expires_at: u64::MAX,
            capability: None,
        };

        manager
            .event_tx
            .send(SyncEvent::SyncError {
                cid,
                error: "hold event channel full".to_string(),
            })
            .await
            .unwrap();

        let owner_manager = Arc::clone(&manager);
        let owner_message = Arc::clone(&message);
        let owner = tokio::spawn(async move {
            owner_manager
                .process_pushlog(&owner_message, Some("peer-1"), false, None)
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !blockstore.has(&cid).await.unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary announcement should store the block");

        let replay_manager = Arc::clone(&manager);
        let replay_message = Arc::clone(&message);
        let replay_authorization = authorization.clone();
        let replay = tokio::spawn(async move {
            replay_manager
                .process_pushlog(
                    &replay_message,
                    Some("peer-1"),
                    true,
                    Some(replay_authorization),
                )
                .await
        });

        assert!(matches!(
            events.recv().await,
            Some(SyncEvent::SyncError { .. })
        ));
        owner.await.unwrap().unwrap();

        assert!(matches!(
            events.recv().await,
            Some(SyncEvent::BlockReceived {
                explicit_replay_authorization: None,
                ..
            })
        ));
        replay.await.unwrap().unwrap();

        assert!(matches!(
            events.recv().await,
            Some(SyncEvent::BlockReceived {
                explicit_replay_authorization: Some(actual),
                ..
            }) if actual == authorization
        ));
    }

    #[tokio::test]
    async fn merged_head_exits_before_block_verification_or_registration() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) =
            SyncManager::new(blockstore.clone(), peer_state, SyncConfig::default());

        let (cid, block) = create_lww_block("name");
        blockstore.put(&cid, &block).await.expect("store block");
        blockstore
            .mark_as_merged(&cid)
            .await
            .expect("mark block merged");

        let invalid_reannouncement =
            make_broadcast("doc123", cid, vec![0xff; 1024 * 1024], "collection1");
        manager
            .process_pushlog(&invalid_reannouncement, Some("peer-1"), false, None)
            .await
            .expect("merged fast path should not inspect the pushed block");

        assert_eq!(manager.pending_dag_count(), 0);
        assert_eq!(manager.diagnostics().snapshot().already_merged_fast_path, 1);
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn pending_capacity_sheds_unrelated_blocks_but_accepts_missing_dependency() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let config = SyncConfig {
            max_pending_dags: 1,
            ..SyncConfig::default()
        };
        let (manager, mut events) = SyncManager::new(blockstore.clone(), peer_state, config);

        let (missing_cid, missing_block) = create_lww_block("missing");
        let (first_cid, first_block) = create_composite_block("doc123", "name", missing_cid);
        manager
            .process_pushlog(
                &make_broadcast("doc123", first_cid, first_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("fill pending DAG registry");
        assert!(matches!(
            events.try_recv(),
            Ok(SyncEvent::DagNeedsFetch { root_cid, .. }) if root_cid == first_cid
        ));

        let (rejected_cid, _rejected_block) = create_lww_block("rejected");
        let allocation_heavy_garbage = vec![0xff; 4 * 1024 * 1024];
        let result = manager
            .process_pushlog(
                &make_broadcast(
                    "doc456",
                    rejected_cid,
                    allocation_heavy_garbage,
                    "collection1",
                ),
                Some("peer-2"),
                false,
                None,
            )
            .await;

        assert!(matches!(result, Err(Error::PendingDagCapacity { max: 1 })));
        assert_eq!(
            manager.diagnostics().snapshot().pending_dag_capacity_shed,
            1
        );
        assert!(!blockstore
            .has(&rejected_cid)
            .await
            .expect("check rejected block"));
        assert_eq!(manager.pending_dag_count(), 1);

        manager
            .process_pushlog(
                &make_broadcast("doc123", missing_cid, missing_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("missing dependency must bypass the full registry");

        assert!(blockstore
            .has(&missing_cid)
            .await
            .expect("check missing dependency"));
        assert_eq!(manager.pending_dag_count(), 0);
    }
}
