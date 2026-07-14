//! Pending DAG registration and retry logic.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use cid::Cid;

use blockstore::Blockstore;

use crate::error::{Error, Result};
use crate::sync::manager::events::SyncEvent;
use crate::sync::manager::links::{extract_ipld_links, find_all_missing_links};
use crate::sync::manager::pending::{PendingDag, PENDING_DAG_TTL};
use crate::sync::pending_store::{PersistedPendingDag, PersistedQuarantinedDag};

use super::SyncManager;

#[derive(Debug, Clone)]
pub struct PendingDagFetchFailure {
    pub doc_id: String,
    pub collection_id: String,
    pub source_peer: Option<String>,
    pub missing_count: usize,
    pub fetch_failures: u32,
}

fn evict_expired_pending_dags(
    pending: &mut HashMap<Cid, PendingDag>,
    now: Instant,
) -> Vec<(Cid, PendingDag)> {
    let expired: Vec<_> = pending
        .iter()
        .filter(|(_, dag)| now.duration_since(dag.inserted_at) >= PENDING_DAG_TTL)
        .map(|(cid, dag)| (*cid, dag.clone()))
        .collect();

    for (cid, _) in &expired {
        pending.remove(cid);
    }

    expired
}

impl<B: Blockstore + 'static> SyncManager<B> {
    /// Get the pending DAGs count (for testing/monitoring).
    pub fn pending_dag_count(&self) -> usize {
        self.pending_dags.read().len()
    }

    pub(super) fn is_pending_dag_recovery_registered(&self, root_cid: &Cid) -> bool {
        self.pending_dags
            .read()
            .get(root_cid)
            .is_some_and(|dag| dag.is_recovery_registered)
    }

    pub(super) fn mark_pending_dag_recovery_registered(
        &self,
        root_cid: &Cid,
        inserted_at: Instant,
    ) {
        if let Some(dag) = self.pending_dags.write().get_mut(root_cid) {
            if dag.inserted_at == inserted_at {
                dag.is_recovery_registered = true;
            }
        }
    }

    /// Milliseconds until the earliest due incomplete pending-DAG retry, or
    /// `None` when no incomplete entry is registered (#1116 stage 2
    /// diagnostics: surfaces the retry clock's own state for `SyncStatus`).
    pub fn next_pending_retry_in_ms(&self) -> Option<u64> {
        let now = tokio::time::Instant::now();
        self.pending_dags
            .read()
            .values()
            .filter(|dag| !dag.missing.is_empty())
            .map(|dag| dag.next_retry_at.saturating_duration_since(now).as_millis() as u64)
            .min()
    }

    /// Return whether a root can enter the pending-DAG registry without
    /// decoding its pushed block.
    pub(super) fn can_admit_pending_dag(&self, root_cid: &Cid) -> bool {
        let mut pending = self.pending_dags.write();
        let expired = evict_expired_pending_dags(&mut pending, Instant::now());
        for _ in expired {
            self.diagnostics.record_pending_dag_expired();
        }

        pending.len() < self.max_pending_dags || pending.contains_key(root_cid)
    }

    /// Get CIDs of all pending DAGs.
    pub fn pending_dag_cids(&self) -> Vec<Cid> {
        self.pending_dags.read().keys().copied().collect()
    }

    /// Pending DAGs worth re-driving when `peer` (re)connects: entries the
    /// peer originally provided plus entries whose fetches exhausted their
    /// providers (covers restored registrations whose providers were not yet
    /// reconnected at restore time, #1099).
    pub fn pending_dags_needing_redrive(&self, peer: &str) -> Vec<(Cid, PendingDag)> {
        self.pending_dags
            .read()
            .iter()
            .filter(|(_, dag)| {
                !dag.missing.is_empty()
                    && (dag.source_peer.as_deref() == Some(peer) || dag.fetch_failures > 0)
            })
            .map(|(cid, dag)| (*cid, dag.clone()))
            .collect()
    }

    /// Get missing CIDs for a pending DAG.
    pub fn pending_dag_missing(&self, root_cid: &Cid) -> Vec<Cid> {
        self.pending_dags
            .read()
            .get(root_cid)
            .map(|dag| dag.missing.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Snapshot a pending DAG entry for dispatch (e.g. after a claimed
    /// post-fetch retry, #1116 stage 2).
    pub fn pending_dag_snapshot(&self, root_cid: &Cid) -> Option<PendingDag> {
        self.pending_dags.read().get(root_cid).cloned()
    }

    /// How many times `retry_pending_dag` has been called for this root.
    ///
    /// Returns 0 if no entry exists (either never registered or already resolved).
    pub fn pending_dag_attempts(&self, root_cid: &Cid) -> u32 {
        self.pending_dags
            .read()
            .get(root_cid)
            .map(|dag| dag.attempts)
            .unwrap_or(0)
    }

    /// Get the source peer for a pending DAG (the peer that originally provided it).
    pub fn pending_dag_source_peer(&self, root_cid: &Cid) -> Option<String> {
        self.pending_dags
            .read()
            .get(root_cid)
            .and_then(|dag| dag.source_peer.clone())
    }

    /// Record a provider-exhaustion failure for a pending DAG and return a
    /// snapshot suitable for rate-limited logging.
    pub fn record_pending_dag_fetch_failure(
        &self,
        root_cid: &Cid,
        error: &str,
    ) -> Option<PendingDagFetchFailure> {
        let mut pending = self.pending_dags.write();
        let dag = pending.get_mut(root_cid)?;
        dag.fetch_failures = dag.fetch_failures.saturating_add(1);
        dag.last_fetch_error = Some(error.to_string());
        Some(PendingDagFetchFailure {
            doc_id: dag.doc_id.clone(),
            collection_id: dag.collection_id.clone(),
            source_peer: dag.source_peer.clone(),
            missing_count: dag.missing.len(),
            fetch_failures: dag.fetch_failures,
        })
    }

    /// Clear the remembered fetch-failure state for a pending DAG.
    pub fn clear_pending_dag_fetch_failures(&self, root_cid: &Cid) {
        if let Some(dag) = self.pending_dags.write().get_mut(root_cid) {
            dag.fetch_failures = 0;
            dag.last_fetch_error = None;
        }
    }

    /// A block just arrived (PushLog store or Bitswap fetch). Update every
    /// pending root waiting on it: drop it from `missing`, add the block's
    /// own absent links to the frontier, and run the full verification walk
    /// (`retry_pending_dag`) ONLY for roots whose frontier emptied. The full
    /// walk per arriving block was O(DAG) per root per block — the #1112
    /// inbound storm; the clock's periodic retry self-heals any frontier
    /// drift this incremental pass might accumulate.
    ///
    /// This covers the explicit replay path where a composite can be registered
    /// as pending before its linked field blocks arrive via later PushLog
    /// requests rather than Bitswap.
    pub async fn retry_pending_dags_waiting_on(&self, cid: &Cid) -> Result<Vec<Cid>> {
        let waiting_roots: Vec<Cid> = {
            let pending = self.pending_dags.read();
            pending
                .iter()
                .filter_map(|(root_cid, dag)| dag.missing.contains(cid).then_some(*root_cid))
                .collect()
        };
        if waiting_roots.is_empty() {
            return Ok(Vec::new());
        }

        // The arriving block's own links that are still absent become the
        // new frontier for every waiting root. Computed once per block.
        // Uses `extract_ipld_links` (not `defra_core::collect_block_links`
        // directly) so the `encryption` link stays excluded here too — Go
        // never serves that block over Bitswap (#976), so treating it as
        // "absent" would strand the frontier on a CID that never arrives.
        let mut absent_links: Vec<Cid> = Vec::new();
        if let Ok(Some(data)) = self.blockstore.get(cid).await {
            for link in extract_ipld_links(&data).unwrap_or_default() {
                if !matches!(self.blockstore.has(&link).await, Ok(true)) {
                    absent_links.push(link);
                }
            }
        }

        let emptied: Vec<Cid> = {
            let mut pending = self.pending_dags.write();
            let mut emptied = Vec::new();
            for root_cid in &waiting_roots {
                if let Some(dag) = pending.get_mut(root_cid) {
                    dag.missing.remove(cid);
                    dag.missing.extend(absent_links.iter().copied());
                    if dag.missing.is_empty() {
                        emptied.push(*root_cid);
                    }
                }
            }
            emptied
        };

        let mut completed = Vec::new();
        for root_cid in emptied {
            if self.retry_pending_dag(&root_cid).await? {
                completed.push(root_cid);
            }
        }

        Ok(completed)
    }

    /// Insert a pending DAG entry, enforcing TTL eviction and capacity limits.
    ///
    /// Expired entries (older than `PENDING_DAG_TTL`) are removed before checking
    /// the capacity. If the map is still at `max_pending_dags` after eviction the
    /// new entry is dropped and `false` is returned so callers can reject with a
    /// backpressure nack (#1088 W1) instead of acking a discarded registration.
    /// TTL eviction frees the in-memory slot only: a push-originated entry's
    /// durable record survives (the recovery obligation is discharged solely
    /// by a successful merge) and is re-driven by restart or the durable
    /// resync sweep.
    pub(super) fn insert_pending_dag(&self, root_cid: Cid, dag: PendingDag) -> bool {
        let mut pending = self.pending_dags.write();
        evict_expired_pending_dags(&mut pending, Instant::now());

        if pending.len() >= self.max_pending_dags && !pending.contains_key(&root_cid) {
            return false;
        }

        pending.insert(root_cid, dag);
        true
    }

    fn update_pending_dag_missing_if_current(
        &self,
        root_cid: &Cid,
        inserted_at: Instant,
        missing: HashSet<Cid>,
    ) -> bool {
        let mut pending = self.pending_dags.write();
        let Some(dag) = pending.get_mut(root_cid) else {
            return false;
        };
        if dag.inserted_at != inserted_at {
            return false;
        }
        dag.missing = missing;
        true
    }

    fn take_pending_dag_if_current(
        &self,
        root_cid: &Cid,
        inserted_at: Instant,
    ) -> Option<PendingDag> {
        let mut pending = self.pending_dags.write();
        match pending.get(root_cid) {
            Some(dag) if dag.inserted_at == inserted_at => pending.remove(root_cid),
            _ => None,
        }
    }

    /// Remove a pending DAG entry once another fetch path has completed it.
    pub fn clear_pending_dag(&self, root_cid: &Cid) -> bool {
        self.pending_dags.write().remove(root_cid).is_some()
    }

    /// Claim the right to dispatch a fetch for this root. Returns false when
    /// the entry is missing, already complete, or not yet due — the caller
    /// must then NOT emit a fetch. A successful claim advances the clock, so
    /// concurrent dispatch sites (registration, retry clock, peer connect)
    /// are rate-bounded by construction.
    pub fn try_claim_pending_dag_dispatch(
        &self,
        root_cid: &Cid,
        now: tokio::time::Instant,
    ) -> bool {
        let mut pending = self.pending_dags.write();
        let Some(dag) = pending.get_mut(root_cid) else {
            self.diagnostics.record_pending_dag_retry_suppressed();
            return false;
        };
        if dag.missing.is_empty() || now < dag.next_retry_at {
            self.diagnostics.record_pending_dag_retry_suppressed();
            return false;
        }
        dag.dispatches = dag.dispatches.saturating_add(1);
        dag.next_retry_at = now + super::super::pending::retry_backoff(dag.dispatches);
        self.diagnostics.record_pending_dag_retry_dispatched();
        true
    }

    /// Make a root promptly due (e.g. a provider just connected) without
    /// resetting its backoff rung. The retry clock performs the dispatch.
    pub fn expedite_pending_dag_retry(&self, root_cid: &Cid) {
        if let Some(dag) = self.pending_dags.write().get_mut(root_cid) {
            dag.next_retry_at = dag.next_retry_at.min(tokio::time::Instant::now());
        }
    }

    /// Atomically claim every due incomplete entry and return snapshots for
    /// dispatch. Used by the coordinator retry clock.
    pub fn claim_due_pending_dag_retries(
        &self,
        now: tokio::time::Instant,
    ) -> Vec<(Cid, PendingDag)> {
        let mut pending = self.pending_dags.write();
        let mut claimed = Vec::new();
        for (cid, dag) in pending.iter_mut() {
            if !dag.missing.is_empty() && now >= dag.next_retry_at {
                dag.dispatches = dag.dispatches.saturating_add(1);
                dag.next_retry_at = now + super::super::pending::retry_backoff(dag.dispatches);
                self.diagnostics.record_pending_dag_retry_dispatched();
                claimed.push((*cid, dag.clone()));
            }
        }
        claimed
    }

    /// Register a pending DAG for DocSync.
    ///
    /// This is called when a DocSyncReply contains head CIDs that need to be
    /// fetched via Bitswap. Unlike PushLog-initiated syncs, DocSync doesn't
    /// have collection_id or creator in the message, so we use empty strings.
    /// The merge handler will extract the actual metadata from the block data.
    ///
    /// # Arguments
    ///
    /// * `root_cid` - The head CID to fetch
    /// * `doc_id` - Document ID from the DocSyncItem
    pub fn register_docsync_dag(&self, root_cid: Cid, doc_id: String, source_peer: String) {
        tracing::debug!(
            cid = %root_cid,
            doc_id = %doc_id,
            source_peer = %source_peer,
            "Registering DocSync pending DAG"
        );

        if !self.insert_pending_dag(
            root_cid,
            PendingDag {
                doc_id: doc_id.clone(),
                // DocSync protocol doesn't include collection_id or creator.
                // The merge handler will extract these from the block data.
                collection_id: String::new(),
                creator: String::new(),
                missing: std::iter::once(root_cid).collect(),
                source_peer: Some(source_peer.clone()),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                is_recovery_registered: false,
                inserted_at: Instant::now(),
                attempts: 0,
                fetch_failures: 0,
                last_fetch_error: None,
                next_retry_at: tokio::time::Instant::now(),
                dispatches: 0,
            },
        ) {
            tracing::warn!(
                cid = %root_cid,
                doc_id = %doc_id,
                source_peer = %source_peer,
                max = self.max_pending_dags,
                "Pending DAGs at capacity, dropping DocSync registration"
            );
        }
    }

    /// Register a pending DAG for branchable collection sync.
    ///
    /// Unlike `register_docsync_dag` which stores the document ID,
    /// this stores the collection ID so the merge handler can look up
    /// the local collection for cross-schema-version merges.
    pub fn register_branchable_dag(
        &self,
        root_cid: Cid,
        collection_id: String,
        source_peer: String,
    ) {
        tracing::debug!(
            cid = %root_cid,
            collection_id = %collection_id,
            "Registering branchable sync pending DAG"
        );

        if !self.insert_pending_dag(
            root_cid,
            PendingDag {
                doc_id: String::new(),
                collection_id: collection_id.clone(),
                creator: String::new(),
                missing: std::iter::once(root_cid).collect(),
                source_peer: Some(source_peer.clone()),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                is_recovery_registered: false,
                inserted_at: Instant::now(),
                attempts: 0,
                fetch_failures: 0,
                last_fetch_error: None,
                next_retry_at: tokio::time::Instant::now(),
                dispatches: 0,
            },
        ) {
            tracing::warn!(
                cid = %root_cid,
                collection_id = %collection_id,
                source_peer = %source_peer,
                max = self.max_pending_dags,
                "Pending DAGs at capacity, dropping branchable DAG registration"
            );
        }
    }

    /// Process a pending DAG after Bitswap blocks have been received.
    ///
    /// This is called when BitswapComplete is received, indicating all requested
    /// blocks have arrived. We re-check the DAG for any remaining missing links
    /// (recursively, at all depths) and process it if complete.
    pub async fn retry_pending_dag(&self, root_cid: &Cid) -> Result<bool> {
        enum PendingDagRetryEntry {
            Current(PendingDag),
            Expired(PendingDag),
        }

        // Record the attempt (and capture the incremented value) while holding
        // the lock so concurrent retries observe monotonic attempt counts.
        let pending_info = {
            let mut pending = self.pending_dags.write();
            let expired = evict_expired_pending_dags(&mut pending, Instant::now());
            if let Some((_, dag)) = expired.into_iter().find(|(cid, _)| cid == root_cid) {
                Some(PendingDagRetryEntry::Expired(dag))
            } else {
                pending.get_mut(root_cid).map(|dag| {
                    dag.attempts = dag.attempts.saturating_add(1);
                    PendingDagRetryEntry::Current(dag.clone())
                })
            }
        };

        let Some(info) = pending_info else {
            tracing::debug!(
                root_cid = %root_cid,
                "No pending DAG found for retry (already resolved or never registered)"
            );
            return Ok(false);
        };

        let info = match info {
            PendingDagRetryEntry::Current(info) => info,
            PendingDagRetryEntry::Expired(info) => {
                self.diagnostics.record_pending_dag_expired();
                tracing::warn!(
                    root_cid = %root_cid,
                    doc_id = %info.doc_id,
                    collection_id = %info.collection_id,
                    source_peer = ?info.source_peer,
                    missing_count = info.missing.len(),
                    attempts = info.attempts,
                    fetch_failures = info.fetch_failures,
                    last_fetch_error = ?info.last_fetch_error,
                    age_secs = info.inserted_at.elapsed().as_secs(),
                    "Pending DAG expired (TTL exceeded), dropping"
                );
                return Ok(false);
            }
        };

        self.diagnostics.record_missing_link_retry();

        // Load the root block from blockstore
        let block_data = match self.blockstore.get(root_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::error!(
                    root_cid = %root_cid,
                    "Root block not found in blockstore during retry"
                );
                return Err(Error::BlockstoreError("Root block not found".to_string()));
            }
            Err(e) => {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to load root block from blockstore"
                );
                return Err(Error::from_blockstore(e));
            }
        };

        // Recursively check ALL missing links at every depth of the DAG.
        // This is critical for multi-level DAGs like Collection → Composite → LWW
        // where a single-level check would declare the DAG "ready" prematurely.
        let missing = match find_all_missing_links(self.blockstore.as_ref(), &block_data).await {
            Ok(missing) => missing,
            Err(e) => {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to re-check missing links for pending DAG"
                );
                return Err(e);
            }
        };

        tracing::debug!(
            root_cid = %root_cid,
            doc_id = %info.doc_id,
            missing_count = missing.len(),
            "Retrying pending DAG"
        );

        if !missing.is_empty() {
            tracing::debug!(
                root_cid = %root_cid,
                missing_count = missing.len(),
                "Still missing blocks for DAG"
            );
            // Update the pending info with new missing CIDs (preserve original inserted_at).
            if !self.update_pending_dag_missing_if_current(
                root_cid,
                info.inserted_at,
                missing.into_iter().collect(),
            ) {
                tracing::debug!(
                    root_cid = %root_cid,
                    "Pending DAG changed before retry update; skipping stale retry result"
                );
            }
            return Ok(false);
        }

        // DAG is complete at all depths - remove from pending and process
        let Some(info) = self.take_pending_dag_if_current(root_cid, info.inserted_at) else {
            tracing::debug!(
                root_cid = %root_cid,
                "Pending DAG changed before ready event; skipping stale retry result"
            );
            return Ok(false);
        };
        tracing::info!(
            root_cid = %root_cid,
            doc_id = %info.doc_id,
            collection_id = %info.collection_id,
            attempts = info.attempts,
            pending_duration_ms = info.inserted_at.elapsed().as_millis() as u64,
            "DAG complete, emitting DagReady"
        );

        // Emit event that DAG is ready for merge
        if self
            .event_tx
            .send(SyncEvent::DagReady {
                root_cid: *root_cid,
                doc_id: info.doc_id.clone(),
                collection_id: info.collection_id.clone(),
                creator: info.creator.clone(),
                sender_peer: info.source_peer.clone(),
                is_explicit_replicator: info.is_explicit_replicator,
                explicit_replay_authorization: info.explicit_replay_authorization.clone(),
            })
            .await
            .is_err()
        {
            let reinserted = self.insert_pending_dag(*root_cid, info);
            tracing::error!(
                root_cid = %root_cid,
                reinserted,
                "Failed to emit DagReady for completed DAG"
            );
            return Err(Error::ChannelSend);
        }

        self.diagnostics.record_pending_dag_resolved();
        Ok(true)
    }

    /// Reconcile the in-memory pending map against the durable registrations
    /// (#1099): drop records whose roots merged, and re-register + re-drive
    /// every unmerged record with no live in-memory entry. Runs at startup
    /// (restore) and as the single-flight sweep behind peer connects, so a
    /// registration whose in-memory entry was TTL-evicted is recovered
    /// without a restart. Roots that no longer fit under `max_pending_dags`
    /// keep their record for the next sweep. Returns the count re-driven.
    pub async fn resync_persisted_pending_dags(&self) -> usize {
        let Some(store) = self.pending_store() else {
            return 0;
        };
        // Cheap steady-state exit: nothing to reconcile while every durable
        // root still has a live, unexpired in-memory entry. An expired entry
        // must not mask its record — eviction is lazy, and the record is the
        // only remaining owner of the recovery obligation. Every Nth sweep is
        // forced past this exit so an orphan record (present in the store but
        // absent from the accounting set after a rare reserve/put race) is
        // still rediscovered.
        let forced = self
            .pending_resync_tick
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .is_multiple_of(super::PENDING_RESYNC_FORCED_TICK);
        if !forced {
            let roots = self.persisted_roots.read();
            let pending = self.pending_dags.read();
            let now = Instant::now();
            let all_live = !roots.is_empty()
                && roots.iter().all(|root| {
                    pending
                        .get(root)
                        .is_some_and(|dag| now.duration_since(dag.inserted_at) < PENDING_DAG_TTL)
                });
            if all_live {
                return 0;
            }
        }
        if self
            .pending_resync_in_flight
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return 0;
        }
        let resynced = self.resync_persisted_pending_dags_inner(store).await;
        self.pending_resync_in_flight
            .store(false, std::sync::atomic::Ordering::Release);
        resynced
    }

    async fn resync_persisted_pending_dags_inner(
        &self,
        store: Arc<dyn crate::sync::pending_store::PendingDagStorage>,
    ) -> usize {
        let records = match store.load_all().await {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(error = %error, "Failed to load persisted pending DAG records");
                return 0;
            }
        };
        // Reconcile the accounting set with the authoritative record list:
        // extend with every record (a registration persisted between load_all
        // and this update must stay eligible for merge-time deletion), and
        // prune entries with neither a record nor a live in-memory entry —
        // stale leftovers from a rare reserve/put race that would otherwise
        // hold durable-cap headroom forever. An in-flight reserve→put window
        // always has an in-memory entry, so it is never pruned.
        {
            let record_roots: std::collections::HashSet<Cid> =
                records.iter().map(|(cid, _)| *cid).collect();
            // Lock order: persisted_roots before pending_dags, matching the
            // steady-state exit above.
            let mut roots = self.persisted_roots.write();
            let pending = self.pending_dags.read();
            roots.retain(|root| record_roots.contains(root) || pending.contains_key(root));
            roots.extend(record_roots);
        }
        if records.is_empty() {
            return 0;
        }

        let mut restored = 0usize;
        for (root_cid, record) in records {
            {
                let mut pending = self.pending_dags.write();
                match pending.get(&root_cid) {
                    Some(dag)
                        if Instant::now().duration_since(dag.inserted_at) < PENDING_DAG_TTL =>
                    {
                        continue;
                    }
                    Some(_) => {
                        // Expired in-memory entry: evict it here so the
                        // record below re-registers with a fresh TTL and a
                        // recomputed missing set.
                        pending.remove(&root_cid);
                    }
                    None => {}
                }
            }
            if matches!(self.is_merged(&root_cid).await, Ok(true)) {
                self.remove_persisted_pending(&root_cid).await;
                continue;
            }

            // A quarantined root's live record is a leftover, not a
            // recovery obligation. Steady state never reaches this branch
            // (quarantine_pending_dag's own delete already removed the live
            // record before this sweep ran); it only fires for the crash
            // window between that write and delete (steps (2)->(3)). Either
            // way the quarantine record is authoritative, so clean up the
            // leftover instead of re-registering a root that will only be
            // rejected again.
            if matches!(store.is_quarantined(&root_cid).await, Ok(true)) {
                self.remove_persisted_pending(&root_cid).await;
                continue;
            }

            let missing: Vec<Cid> = match self.blockstore.get(&root_cid).await {
                Ok(Some(data)) => {
                    match find_all_missing_links(self.blockstore.as_ref(), &data).await {
                        Ok(missing) => missing,
                        Err(_) => vec![root_cid],
                    }
                }
                _ => vec![root_cid],
            };

            let dag = PendingDag {
                doc_id: record.doc_id.clone(),
                collection_id: record.collection_id.clone(),
                creator: record.creator.clone(),
                missing: missing.iter().copied().collect(),
                source_peer: record.source_peer.clone(),
                is_explicit_replicator: record.is_explicit_replicator,
                explicit_replay_authorization: record
                    .explicit_replay_authorization
                    .clone()
                    .map(Into::into),
                is_recovery_registered: true,
                inserted_at: Instant::now(),
                attempts: 0,
                fetch_failures: 0,
                last_fetch_error: None,
                next_retry_at: tokio::time::Instant::now(),
                dispatches: 0,
            };

            if !self.insert_pending_dag(root_cid, dag.clone()) {
                tracing::warn!(
                    root_cid = %root_cid,
                    doc_id = %record.doc_id,
                    max = self.max_pending_dags,
                    "Pending DAGs at capacity during resync; record kept for the next sweep"
                );
                continue;
            }

            // A restored entry is immediately due (`insert_pending_dag`
            // leaves `next_retry_at = now`); claim it here so the retry
            // clock's next tick does not also dispatch it. The `bool` is
            // unused — the DagNeedsFetch emission below (when `missing` is
            // non-empty) is the dispatch; when `missing` is empty the claim
            // is a harmless no-op since `try_claim_pending_dag_dispatch`
            // requires a non-empty `missing` set.
            let _claimed =
                self.try_claim_pending_dag_dispatch(&root_cid, tokio::time::Instant::now());

            if missing.is_empty() {
                if let Err(error) = self.retry_pending_dag(&root_cid).await {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Failed to resolve complete restored pending DAG"
                    );
                }
            } else {
                let mut providers = self.get_providers_for_cids(&missing);
                if let Some(source_peer) = record.source_peer.clone() {
                    if !providers.contains(&source_peer) {
                        providers.push(source_peer);
                    }
                }
                if self
                    .event_tx
                    .send(SyncEvent::DagNeedsFetch {
                        root_cid,
                        missing,
                        providers,
                        doc_id: record.doc_id.clone(),
                        collection_id: record.collection_id.clone(),
                        creator: record.creator.clone(),
                        sender_peer: record.source_peer.clone(),
                        is_explicit_replicator: record.is_explicit_replicator,
                        explicit_replay_authorization: dag.explicit_replay_authorization.clone(),
                    })
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        root_cid = %root_cid,
                        "Failed to emit DagNeedsFetch for restored pending DAG"
                    );
                    continue;
                }
            }
            restored += 1;
        }

        if restored > 0 {
            tracing::info!(restored, "restored persisted pending DAG registrations");
        }
        restored
    }

    /// Quarantine a terminally-rejected pending-DAG root (#1128).
    ///
    /// `MergeOutcome::Rejected` means the merge failed on the block's
    /// *content* (e.g. a unique-index violation) rather than transiently:
    /// replaying it will fail identically every time. Unlike a successful
    /// merge, the block is deliberately left unmerged — a remote re-push can
    /// still retry (sender-paced) and re-quarantine on repeat rejection —
    /// but local re-drive (retry clock, resync sweep) must stop.
    ///
    /// Sequencing is load-bearing: the quarantine record is written to the
    /// durable store BEFORE the live `/p2p/pending_dag/` record is deleted.
    /// A crash in the window between the two leaves BOTH records on disk
    /// rather than losing the live one; Task 4's resync sweep consults
    /// `is_quarantined` before re-registering a leftover live record, so the
    /// crash window self-heals instead of re-driving (or silently losing) a
    /// merge that is now known to fail forever.
    pub async fn quarantine_pending_dag(&self, root_cid: &Cid, reason: &str) {
        let entry = self.build_quarantine_entry(root_cid, reason).await;

        if let Some(store) = self.pending_store() {
            if let Err(error) = store.quarantine(root_cid, &entry).await {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %error,
                    "Failed to write quarantine record; leaving the live pending-DAG registration in place for retry"
                );
                return;
            }

            if let Err(error) = store.remove(root_cid).await {
                tracing::warn!(
                    root_cid = %root_cid,
                    error = %error,
                    "Failed to delete live pending-DAG record after quarantine; the next resync sweep will self-heal"
                );
            }
            // Unconditional, unlike `remove_persisted_pending`'s only-on-Ok
            // drop: the quarantine write above (not the live-record delete)
            // is this root's discharge point, so it must stop counting
            // against the durable admission cap even if `store.remove` just
            // failed above. The live record may still be on disk, but the
            // resync sweep's `is_quarantined` check (Task 4) cleans up that
            // leftover independently of this in-memory set.
            self.persisted_roots.write().remove(root_cid);
        }

        self.pending_dags.write().remove(root_cid);
        self.quarantined_pending_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.diagnostics.record_pending_dag_terminal_quarantined();

        tracing::warn!(
            root_cid = %root_cid,
            doc_id = %entry.record.doc_id,
            collection_id = %entry.record.collection_id,
            reason = %reason,
            "Pending DAG quarantined: merge deterministically rejected, will not be re-driven locally"
        );
    }

    /// Build the quarantine record for `root_cid`, preferring the most
    /// complete provenance available so quarantine never fails for lack of
    /// it: the live durable record if one exists, else the in-memory
    /// `PendingDag` entry, else an empty-provenance record as a last resort.
    async fn build_quarantine_entry(
        &self,
        root_cid: &Cid,
        reason: &str,
    ) -> PersistedQuarantinedDag {
        let record = match self.load_live_durable_record(root_cid).await {
            Some(record) => record,
            None => self
                .pending_dags
                .read()
                .get(root_cid)
                .map(|dag| PersistedPendingDag {
                    doc_id: dag.doc_id.clone(),
                    collection_id: dag.collection_id.clone(),
                    creator: dag.creator.clone(),
                    source_peer: dag.source_peer.clone(),
                    is_explicit_replicator: dag.is_explicit_replicator,
                    explicit_replay_authorization: dag
                        .explicit_replay_authorization
                        .as_ref()
                        .map(Into::into),
                })
                .unwrap_or_else(|| PersistedPendingDag {
                    doc_id: String::new(),
                    collection_id: String::new(),
                    creator: String::new(),
                    source_peer: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                }),
        };

        PersistedQuarantinedDag {
            record,
            reason: reason.to_string(),
            quarantined_at_unix_secs: PersistedQuarantinedDag::now_unix_secs(),
        }
    }

    /// Look up the live durable record for `root_cid`, if any. The store has
    /// no by-CID lookup (only `load_all`/prefix scan), so this pays an O(n)
    /// scan of the live keyspace — acceptable because quarantine only fires
    /// on a deterministic content rejection, not the hot merge path.
    async fn load_live_durable_record(&self, root_cid: &Cid) -> Option<PersistedPendingDag> {
        if !self.persisted_roots.read().contains(root_cid) {
            return None;
        }
        let store = self.pending_store()?;
        match store.load_all().await {
            Ok(records) => records
                .into_iter()
                .find(|(cid, _)| cid == root_cid)
                .map(|(_, record)| record),
            Err(error) => {
                tracing::warn!(
                    root_cid = %root_cid,
                    error = %error,
                    "Failed to load durable pending DAG records while building quarantine entry"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use blockstore::DefraBlockstore;
    use defra_core::{
        Block as DefraBlock, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload,
    };
    use multihash_codetable::{Code, MultihashDigest};
    use storage::backends::MemoryStore;

    use crate::sync::manager::DEFAULT_MAX_PENDING_DAGS;
    use crate::sync::{PeerStateTracker, SyncConfig};

    fn test_cid(label: usize) -> Cid {
        Cid::new_v1(
            0x55,
            Code::Sha2_256.digest(format!("cid-{label}").as_bytes()),
        )
    }

    fn test_manager() -> SyncManager<DefraBlockstore<MemoryStore>> {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, _events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());
        manager
    }

    fn pending_dag(doc_id: &str, inserted_at: Instant) -> PendingDag {
        PendingDag {
            doc_id: doc_id.to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            missing: HashSet::new(),
            source_peer: Some("peer".to_string()),
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            is_recovery_registered: false,
            inserted_at,
            attempts: 0,
            fetch_failures: 0,
            last_fetch_error: None,
            next_retry_at: tokio::time::Instant::now(),
            dispatches: 0,
        }
    }

    #[test]
    fn insert_pending_dag_replaces_existing_entry_at_capacity() {
        let manager = test_manager();
        let root = test_cid(0);

        assert!(manager.insert_pending_dag(root, pending_dag("original", Instant::now())));
        for idx in 1..DEFAULT_MAX_PENDING_DAGS {
            assert!(manager.insert_pending_dag(
                test_cid(idx),
                pending_dag(&format!("doc-{idx}"), Instant::now()),
            ));
        }
        assert_eq!(manager.pending_dag_count(), DEFAULT_MAX_PENDING_DAGS);

        assert!(manager.insert_pending_dag(root, pending_dag("replacement", Instant::now())));
        assert_eq!(manager.pending_dag_count(), DEFAULT_MAX_PENDING_DAGS);
        assert_eq!(
            manager
                .pending_dags
                .read()
                .get(&root)
                .map(|dag| dag.doc_id.as_str()),
            Some("replacement")
        );
    }

    #[test]
    fn stale_pending_dag_update_does_not_resurrect_old_generation() {
        let manager = test_manager();
        let root = test_cid(0);
        let current_inserted_at = Instant::now();
        let stale_inserted_at = current_inserted_at + std::time::Duration::from_secs(1);

        assert!(manager.insert_pending_dag(root, pending_dag("current", current_inserted_at)));
        assert!(!manager.update_pending_dag_missing_if_current(
            &root,
            stale_inserted_at,
            [test_cid(1)].into_iter().collect(),
        ));
        assert!(manager.pending_dag_missing(&root).is_empty());
    }

    #[test]
    fn concurrent_pending_dag_insert_burst_stays_bounded() {
        let manager = Arc::new(test_manager());
        let mut handles = Vec::new();

        for worker in 0..8 {
            let manager = Arc::clone(&manager);
            handles.push(std::thread::spawn(move || {
                for idx in 0..200 {
                    let label = worker * 1_000 + idx;
                    manager.insert_pending_dag(
                        test_cid(label),
                        pending_dag(&format!("doc-{label}"), Instant::now()),
                    );
                }
            }));
        }

        for handle in handles {
            handle.join().expect("insert worker should not panic");
        }

        assert!(manager.pending_dag_count() <= DEFAULT_MAX_PENDING_DAGS);
    }

    #[tokio::test(start_paused = true)]
    async fn claim_bumps_clock_and_suppresses_duplicates() {
        let manager = test_manager();
        let root = test_cid(1);
        let mut dag = pending_dag("doc", Instant::now());
        dag.missing.insert(test_cid(2));
        assert!(manager.insert_pending_dag(root, dag));

        let now = tokio::time::Instant::now();
        // Fresh entry is due immediately (insert leaves next_retry_at = now).
        assert!(manager.try_claim_pending_dag_dispatch(&root, now));
        // Second claim in the same instant is suppressed.
        assert!(!manager.try_claim_pending_dag_dispatch(&root, now));
        // Becomes due again after the backoff rung reached by the first
        // claim (dispatches=1 -> retry_backoff(1) = 4s).
        tokio::time::advance(std::time::Duration::from_secs(4)).await;
        assert!(manager.try_claim_pending_dag_dispatch(&root, tokio::time::Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_doubles_and_caps() {
        use crate::sync::manager::pending::retry_backoff;
        assert_eq!(retry_backoff(0), std::time::Duration::from_secs(2));
        assert_eq!(retry_backoff(1), std::time::Duration::from_secs(4));
        assert_eq!(retry_backoff(4), std::time::Duration::from_secs(32));
        assert_eq!(retry_backoff(5), std::time::Duration::from_secs(60));
        assert_eq!(retry_backoff(30), std::time::Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn expedite_makes_entry_due_now_without_resetting_backoff() {
        let manager = test_manager();
        let root = test_cid(1);
        let mut dag = pending_dag("doc", Instant::now());
        dag.missing.insert(test_cid(2));
        assert!(manager.insert_pending_dag(root, dag));
        let now = tokio::time::Instant::now();
        assert!(manager.try_claim_pending_dag_dispatch(&root, now)); // dispatches -> 1
        manager.expedite_pending_dag_retry(&root);
        assert!(manager.try_claim_pending_dag_dispatch(&root, now)); // dispatches -> 2
                                                                     // Next due time reflects dispatches=2 rung (8s), not a reset.
        tokio::time::advance(std::time::Duration::from_secs(4)).await;
        assert!(!manager.try_claim_pending_dag_dispatch(&root, tokio::time::Instant::now()));
        tokio::time::advance(std::time::Duration::from_secs(4)).await;
        assert!(manager.try_claim_pending_dag_dispatch(&root, tokio::time::Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn claim_due_returns_and_claims_only_due_entries_with_missing_blocks() {
        let manager = test_manager();
        let due = test_cid(1);
        let complete = test_cid(3);
        let mut dag = pending_dag("doc-due", Instant::now());
        dag.missing.insert(test_cid(2));
        assert!(manager.insert_pending_dag(due, dag));
        // Entry with no missing blocks must never be dispatched.
        assert!(manager.insert_pending_dag(complete, pending_dag("doc-done", Instant::now())));

        let claimed = manager.claim_due_pending_dag_retries(tokio::time::Instant::now());
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, due);
        // Claiming consumed due-ness.
        assert!(manager
            .claim_due_pending_dag_retries(tokio::time::Instant::now())
            .is_empty());
    }

    fn lww_leaf(field_name: &str) -> (Cid, Vec<u8>) {
        let block = DefraBlock::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                doc_id: b"doc1".to_vec(),
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

    fn composite_node(link_name: &str, link_cid: Cid, priority: u64) -> (Cid, Vec<u8>) {
        let block = DefraBlock::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: b"doc1".to_vec(),
                schema_version_id: "schema1".to_string(),
                priority,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new(link_name, link_cid)],
        );
        let bytes = block.to_dag_cbor().expect("encode composite block");
        let cid = block.generate_cid().expect("generate composite cid");
        (cid, bytes)
    }

    #[tokio::test]
    async fn block_arrival_updates_missing_incrementally_without_full_walks() {
        // A dropped event receiver would fail the completing `retry_pending_dag`
        // call with `ChannelSend` before the assertions below run, so keep it
        // alive (unlike `test_manager()`, which discards it).
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

        // 3-level DAG: root -> child (composite) -> grandchild (lww).
        let (grandchild_cid, grandchild_bytes) = lww_leaf("name");
        let (child_cid, child_bytes) = composite_node("name", grandchild_cid, 1);
        let (root_cid, root_bytes) = composite_node("composite", child_cid, 2);

        manager
            .blockstore
            .put(&root_cid, &root_bytes)
            .await
            .expect("store root");
        manager
            .blockstore
            .put(&child_cid, &child_bytes)
            .await
            .expect("store child");

        let mut dag = pending_dag("doc1", Instant::now());
        dag.missing.insert(child_cid);
        assert!(manager.insert_pending_dag(root_cid, dag));

        // Child arrives; grandchild is still absent. This must only shrink
        // the frontier (child -> grandchild), not run the full walk.
        let completed = manager
            .retry_pending_dags_waiting_on(&child_cid)
            .await
            .expect("retry on child arrival");
        assert!(completed.is_empty(), "root must not complete yet");
        assert_eq!(manager.pending_dag_missing(&root_cid), vec![grandchild_cid]);
        assert_eq!(
            manager.diagnostics.snapshot().missing_link_retries,
            0,
            "a frontier-shrinking arrival must not trigger the full verification walk"
        );

        // Grandchild arrives; the frontier empties, so the full walk runs
        // exactly once to verify completion and the root resolves.
        manager
            .blockstore
            .put(&grandchild_cid, &grandchild_bytes)
            .await
            .expect("store grandchild");
        let completed = manager
            .retry_pending_dags_waiting_on(&grandchild_cid)
            .await
            .expect("retry on grandchild arrival");
        assert_eq!(completed, vec![root_cid]);
        assert_eq!(manager.diagnostics.snapshot().missing_link_retries, 1);
        assert_eq!(manager.pending_dag_count(), 0);

        match events.try_recv().expect("DagReady event") {
            SyncEvent::DagReady {
                root_cid: event_root,
                ..
            } => assert_eq!(event_root, root_cid),
            other => panic!("expected DagReady, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn quarantine_pending_dag_moves_live_record_and_clears_in_memory_entry() {
        use crate::sync::pending_store::{PendingDagStorage, PendingDagStore, PersistedPendingDag};

        let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, _events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

        let root = test_cid(1);
        let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
        pending_store
            .put(
                &root,
                &PersistedPendingDag {
                    doc_id: "doc".to_string(),
                    collection_id: "collection".to_string(),
                    creator: "creator".to_string(),
                    source_peer: Some("peer".to_string()),
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                },
            )
            .await
            .expect("persist live pending dag record");

        // Hydrates persisted_roots from the store (put must happen first, see
        // install_pending_dag_store's hydration-at-install contract).
        manager
            .install_pending_dag_store(pending_store.clone())
            .await;

        assert!(manager.insert_pending_dag(root, pending_dag("doc", Instant::now())));
        assert_eq!(manager.pending_dag_count(), 1);

        manager
            .quarantine_pending_dag(&root, "unique constraint violation")
            .await;

        let quarantined = pending_store
            .load_quarantined()
            .await
            .expect("load quarantined records");
        assert_eq!(quarantined.len(), 1);
        assert_eq!(quarantined[0].0, root);
        assert_eq!(quarantined[0].1.reason, "unique constraint violation");
        assert_eq!(quarantined[0].1.record.doc_id, "doc");

        assert!(
            pending_store.load_all().await.unwrap().is_empty(),
            "live durable record must be removed once quarantined"
        );
        assert_eq!(
            manager.pending_dag_count(),
            0,
            "in-memory entry must be cleared on quarantine"
        );
        assert_eq!(manager.persisted_pending_count(), 0);
        assert_eq!(
            manager
                .diagnostics
                .snapshot()
                .pending_dag_terminal_quarantined,
            1
        );
        assert_eq!(manager.quarantined_pending_count(), 1);
    }

    #[tokio::test]
    async fn quarantine_pending_dag_synthesizes_record_when_no_durable_record_exists() {
        let manager = test_manager();
        let root = test_cid(1);

        assert!(manager.insert_pending_dag(root, pending_dag("doc-in-memory", Instant::now())));

        // No pending store installed at all: quarantine must still succeed
        // (never fail for lack of provenance) and clear the in-memory entry.
        manager
            .quarantine_pending_dag(&root, "unique constraint violation")
            .await;

        assert_eq!(manager.pending_dag_count(), 0);
        assert_eq!(
            manager
                .diagnostics
                .snapshot()
                .pending_dag_terminal_quarantined,
            1
        );
        assert_eq!(manager.quarantined_pending_count(), 1);
    }

    #[tokio::test]
    async fn resync_deletes_live_leftover_of_quarantined_root_without_redriving() {
        use crate::sync::pending_store::{
            PendingDagStorage, PendingDagStore, PersistedPendingDag, PersistedQuarantinedDag,
        };

        let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

        let root = test_cid(1);
        let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
        let record = PersistedPendingDag {
            doc_id: "doc".to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            source_peer: Some("peer".to_string()),
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        };

        // Simulate the crash window inside `quarantine_pending_dag` between
        // writing the quarantine record and deleting the live one: both
        // records exist on disk simultaneously.
        pending_store
            .put(&root, &record)
            .await
            .expect("persist live leftover record");
        pending_store
            .quarantine(
                &root,
                &PersistedQuarantinedDag {
                    record: record.clone(),
                    reason: "unique constraint violation".to_string(),
                    quarantined_at_unix_secs: PersistedQuarantinedDag::now_unix_secs(),
                },
            )
            .await
            .expect("persist quarantine record");

        manager
            .install_pending_dag_store(pending_store.clone())
            .await;

        let restored = manager.resync_persisted_pending_dags().await;

        assert_eq!(restored, 0, "a quarantined root must not be re-registered");
        assert_eq!(
            manager.pending_dag_count(),
            0,
            "in-memory pending map must stay empty for a quarantined root"
        );
        assert!(
            pending_store.load_all().await.unwrap().is_empty(),
            "the resync sweep must delete the live leftover record"
        );
        assert!(
            pending_store
                .load_quarantined()
                .await
                .unwrap()
                .iter()
                .any(|(cid, _)| *cid == root),
            "the quarantine record itself must survive the sweep"
        );
        assert!(
            events.try_recv().is_err(),
            "no DagNeedsFetch/DagReady must be emitted for a quarantined root"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resync_restore_consumes_retry_clock_claim_before_dispatch() {
        use crate::sync::pending_store::{PendingDagStorage, PendingDagStore, PersistedPendingDag};

        let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

        // The root's block is never put in the blockstore, so the resync
        // sweep falls back to treating the root itself as missing and takes
        // the DagNeedsFetch (non-empty `missing`) path.
        let root = test_cid(1);
        let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
        pending_store
            .put(
                &root,
                &PersistedPendingDag {
                    doc_id: "doc".to_string(),
                    collection_id: "collection".to_string(),
                    creator: "creator".to_string(),
                    source_peer: Some("peer".to_string()),
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                },
            )
            .await
            .expect("persist pending dag record");

        manager.install_pending_dag_store(pending_store).await;

        let restored = manager.resync_persisted_pending_dags().await;
        assert_eq!(restored, 1);

        match events
            .try_recv()
            .expect("DagNeedsFetch event from resync restore")
        {
            SyncEvent::DagNeedsFetch { root_cid, .. } => assert_eq!(root_cid, root),
            other => panic!("expected DagNeedsFetch, got {:?}", other),
        }

        // The restore's direct DagNeedsFetch emission already consumed the
        // immediate claim (mirrors the fresh-registration path in
        // pushlog.rs) -- the retry clock must not also dispatch this root
        // before the backoff rung elapses.
        assert!(manager
            .claim_due_pending_dag_retries(tokio::time::Instant::now())
            .is_empty());

        // Becomes due again only after the backoff rung reached by the
        // restore's claim (dispatches=1 -> retry_backoff(1) = 4s).
        tokio::time::advance(std::time::Duration::from_secs(4)).await;
        let claimed = manager.claim_due_pending_dag_retries(tokio::time::Instant::now());
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].0, root);
    }
}
