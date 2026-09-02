//! Pending DAG registration and retry logic.

use std::collections::HashSet;
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

struct PendingResyncInFlight<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for PendingResyncInFlight<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PendingDagLease {
    root_cid: Cid,
    pending: Arc<parking_lot::RwLock<crate::sync::manager::pending::PendingDagRegistry>>,
}

impl PendingDagLease {
    pub(crate) fn is_current(&self) -> bool {
        self.pending.read().contains_key(&self.root_cid)
    }
}

/// Match the outbound backlog's fairness policy: one peer may occupy at most
/// one quarter of the global pending-DAG capacity.
const PENDING_DAG_PEER_CAPACITY_DIVISOR: usize = 4;

/// Outcome of admitting a pending-DAG registration into the in-memory map.
///
/// The rejection variants carry the limit that tripped so the caller's nack
/// reports the number the operator will see in logs (global vs per-peer).
pub(super) enum PendingDagAdmission {
    Admitted {
        superseded: Box<Option<(Cid, PendingDag)>>,
    },
    CoveredByCurrent,
    GlobalCapacity,
    PeerQuota {
        max_per_peer: usize,
    },
}

#[derive(Debug, Clone)]
pub struct PendingDagFetchFailure {
    pub doc_id: String,
    pub collection_id: String,
    pub source_peer: Option<String>,
    pub missing_count: usize,
    pub fetch_failures: u32,
}

fn evict_expired_pending_dags(
    pending: &mut crate::sync::manager::pending::PendingDagRegistry,
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
    pub(crate) fn pending_dag_lease(&self, root_cid: Cid) -> PendingDagLease {
        PendingDagLease {
            root_cid,
            pending: Arc::clone(&self.pending_dags),
        }
    }
    /// Get the pending DAGs count (for testing/monitoring).
    pub fn pending_dag_count(&self) -> usize {
        self.pending_dags.read().len()
    }

    pub(super) fn max_pending_dags_per_peer(&self) -> usize {
        (self.max_pending_dags / PENDING_DAG_PEER_CAPACITY_DIVISOR).max(1)
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

    /// Milliseconds until the earliest due pending-DAG retry, including a
    /// complete DAG awaiting a terminal merge outcome. `None` means no live
    /// receiver obligation is registered.
    pub fn next_pending_retry_in_ms(&self) -> Option<u64> {
        let now = tokio::time::Instant::now();
        self.pending_dags
            .read()
            .values()
            .map(|dag| dag.next_retry_at.saturating_duration_since(now).as_millis() as u64)
            .min()
    }

    /// Return whether a PushLog can proceed without decoding its block. At
    /// capacity, blocks awaited by existing roots must still enter so those
    /// roots can complete and free their slots.
    pub(super) fn can_process_pushlog(&self, cid: &Cid) -> bool {
        let mut pending = self.pending_dags.write();
        let expired = evict_expired_pending_dags(&mut pending, Instant::now());
        for _ in expired {
            self.diagnostics.record_pending_dag_expired();
        }

        pending.len() < self.max_pending_dags
            || pending.contains_key(cid)
            || pending.has_waiters(cid)
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
        if !self.pending_dags.read().has_waiters(cid) {
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

        let emptied = self
            .pending_dags
            .write()
            .advance_waiters(cid, &absent_links);

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
    /// new entry is rejected so callers can return a backpressure nack (#1088
    /// W1) instead of acking a discarded registration. A source peer may hold
    /// at most one quarter of the global map so one noisy pusher cannot starve
    /// every other peer (#1088 W2).
    /// TTL eviction frees the in-memory slot only: a push-originated entry's
    /// durable record survives (the recovery obligation is discharged solely
    /// by a successful merge) and is re-driven by restart or the durable
    /// resync sweep.
    /// Admit a registration into the in-memory map, reporting `true` on
    /// success. Callers that need to distinguish the failing limit (to nack
    /// with the right number) use [`Self::try_insert_pending_dag`].
    #[cfg(test)]
    pub(super) fn insert_pending_dag(&self, root_cid: Cid, dag: PendingDag) -> bool {
        matches!(
            self.try_insert_pending_dag(root_cid, dag),
            PendingDagAdmission::Admitted { .. } | PendingDagAdmission::CoveredByCurrent
        )
    }

    pub(super) fn try_insert_pending_dag(
        &self,
        root_cid: Cid,
        dag: PendingDag,
    ) -> PendingDagAdmission {
        let mut pending = self.pending_dags.write();
        evict_expired_pending_dags(&mut pending, Instant::now());

        use crate::sync::manager::pending::ScopeHeadDecision;
        let scope_decision = pending.scope_head_decision(root_cid, &dag);
        if matches!(scope_decision, ScopeHeadDecision::CoveredByCurrent) {
            return PendingDagAdmission::CoveredByCurrent;
        }
        let superseded_root = match scope_decision {
            ScopeHeadDecision::Supersedes(root) => Some(root),
            _ => None,
        };

        if pending.len() >= self.max_pending_dags
            && !pending.contains_key(&root_cid)
            && superseded_root.is_none()
        {
            return PendingDagAdmission::GlobalCapacity;
        }

        if let Some(source_peer) = dag.source_peer.as_deref() {
            let existing_source = pending
                .get(&root_cid)
                .and_then(|existing| existing.source_peer.as_deref());
            let max_per_peer = self.max_pending_dags_per_peer();
            if existing_source != Some(source_peer)
                && pending.source_count(source_peer) >= max_per_peer
                && superseded_root.is_none()
            {
                return PendingDagAdmission::PeerQuota { max_per_peer };
            }
        }

        let superseded =
            superseded_root.and_then(|root| pending.remove(&root).map(|previous| (root, previous)));
        pending.insert(root_cid, dag);
        self.diagnostics.observe_pending_dag_depth(pending.len());
        PendingDagAdmission::Admitted {
            superseded: Box::new(superseded),
        }
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
        pending.replace_missing(root_cid, missing)
    }

    /// Remove a pending DAG entry once another fetch path has completed it.
    pub fn clear_pending_dag(&self, root_cid: &Cid) -> bool {
        self.pending_dags.write().remove(root_cid).is_some()
    }

    /// Claim the right to dispatch receiver work for this root. Returns false
    /// when the entry is missing or not yet due. Complete roots remain
    /// eligible because this same clock re-drives a transiently failed merge.
    /// A successful claim advances the clock; registration, completion, and
    /// peer connect may only make the root due.
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
        if !dag.is_recovery_registered || now < dag.next_retry_at {
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

    /// Return due receiver obligations in oldest-due order without claiming
    /// them. The coordinator reserves bounded ownership before advancing a
    /// root's retry rung, so neither fetches nor merge re-drive can accumulate
    /// behind the limiter.
    pub(crate) fn due_pending_dag_retries(
        &self,
        now: tokio::time::Instant,
    ) -> Vec<(Cid, PendingDag)> {
        let pending = self.pending_dags.read();
        let mut due: Vec<_> = pending
            .iter()
            .filter(|(_, dag)| dag.is_recovery_registered && now >= dag.next_retry_at)
            .map(|(cid, dag)| (*cid, dag.clone()))
            .collect();
        due.sort_unstable_by(|(left_cid, left), (right_cid, right)| {
            left.next_retry_at
                .cmp(&right.next_retry_at)
                .then_with(|| left_cid.to_bytes().cmp(&right_cid.to_bytes()))
        });
        due
    }

    /// Test seam that claims every currently due root. Production dispatch
    /// claims roots individually only after reserving bounded task ownership.
    #[cfg(test)]
    pub fn claim_due_pending_dag_retries(
        &self,
        now: tokio::time::Instant,
    ) -> Vec<(Cid, PendingDag)> {
        self.due_pending_dag_retries(now)
            .into_iter()
            .filter(|(cid, _)| self.try_claim_pending_dag_dispatch(cid, now))
            .collect()
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

        // Keep the live registration until merge/mark or quarantine reaches a
        // durable terminal outcome. The receiver clock can then re-drive a
        // transient merge failure without waiting for restart reconciliation.
        if !self.update_pending_dag_missing_if_current(root_cid, info.inserted_at, HashSet::new()) {
            tracing::debug!(
                root_cid = %root_cid,
                "Pending DAG changed before ready event; skipping stale completion"
            );
            return Ok(false);
        }
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
            tracing::error!(
                root_cid = %root_cid,
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
        let _in_flight = PendingResyncInFlight(&self.pending_resync_in_flight);
        self.resync_persisted_pending_dags_inner(store).await
    }

    async fn resync_persisted_pending_dags_inner(
        &self,
        store: Arc<dyn crate::sync::pending_store::PendingDagStorage>,
    ) -> usize {
        // Registration, terminal removal, quarantine, and this authoritative
        // snapshot all use the same writer.  Without it, a sweep can load a
        // record, a terminal path can delete it and clear the accounting set,
        // and the stale snapshot can then re-add the root.  Keeping a live
        // in-memory pending entry is not evidence of durable ownership: only
        // the store is authoritative for this accounting and its hard cap.
        let records = {
            let _metadata_writer = self.pending_metadata_writer.lock().await;
            let records = match store.load_all().await {
                Ok(records) => records,
                Err(error) => {
                    tracing::warn!(error = %error, "Failed to load persisted pending DAG records");
                    return 0;
                }
            };
            let record_roots: std::collections::HashSet<Cid> =
                records.iter().map(|(cid, _)| *cid).collect();
            *self.persisted_roots.write() = record_roots.clone();
            self.persisted_scope_heads
                .write()
                .retain(|_, version| record_roots.contains(&version.cid));
            for (root_cid, record) in &records {
                self.remember_persisted_scope_head(
                    *root_cid,
                    record.source_peer.as_deref(),
                    &record.collection_id,
                    &record.doc_id,
                    record.head_priority,
                );
            }
            records
        };
        if records.is_empty() {
            return 0;
        }

        let mut restored = 0usize;
        for (root_cid, mut record) in records {
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

            let (missing, decoded_head_priority): (Vec<Cid>, Option<u64>) =
                match self.blockstore.get(&root_cid).await {
                    Ok(Some(data)) => {
                        let priority = defra_core::Block::from_dag_cbor(&data)
                            .ok()
                            .map(|block| block.delta.priority());
                        let missing = find_all_missing_links(self.blockstore.as_ref(), &data)
                            .await
                            .unwrap_or_else(|_| vec![root_cid]);
                        (missing, priority)
                    }
                    _ => (vec![root_cid], None),
                };
            let head_priority = decoded_head_priority.or(record.head_priority);

            // One-way upgrade for records written before scope-current
            // coalescing: once the root block is locally decodable, persist
            // its monotonic priority so restart/TTL admission remains able to
            // retire obsolete heads without retaining payload/CID ledgers.
            if record.head_priority.is_none() && head_priority.is_some() {
                record.head_priority = head_priority;
                let _metadata_writer = self.pending_metadata_writer.lock().await;
                match store.put(&root_cid, &record).await {
                    Ok(()) => self.remember_persisted_scope_head(
                        root_cid,
                        record.source_peer.as_deref(),
                        &record.collection_id,
                        &record.doc_id,
                        head_priority,
                    ),
                    Err(error) => tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Failed to upgrade legacy pending DAG scope metadata; obligation remains independently replayable"
                    ),
                }
            }

            let dag = PendingDag {
                doc_id: record.doc_id.clone(),
                collection_id: record.collection_id.clone(),
                head_priority,
                creator: record.creator.clone(),
                missing: missing.iter().copied().collect(),
                source_peer: record.source_peer.clone(),
                alternate_providers: record
                    .alternate_providers
                    .iter()
                    .take(crate::sync::pending_store::MAX_PENDING_DAG_ALTERNATE_PROVIDERS)
                    .cloned()
                    .collect(),
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

            let superseded = match self.try_insert_pending_dag(root_cid, dag.clone()) {
                PendingDagAdmission::Admitted { superseded } => *superseded,
                PendingDagAdmission::CoveredByCurrent => {
                    self.remove_persisted_pending(&root_cid).await;
                    continue;
                }
                PendingDagAdmission::GlobalCapacity | PendingDagAdmission::PeerQuota { .. } => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        doc_id = %record.doc_id,
                        max = self.max_pending_dags,
                        max_per_peer = self.max_pending_dags_per_peer(),
                        source_peer = ?record.source_peer,
                        "Pending DAGs at capacity during resync; record kept for the next sweep"
                    );
                    continue;
                }
            };
            if let Some((old_root, _)) = superseded {
                // Both records survived the restart, so deleting the older
                // one after the newer one is live cannot create an ownership
                // gap. A failed idempotent delete remains visible and the
                // forced resync sweep retries it.
                self.remove_persisted_pending(&old_root).await;
            }

            if missing.is_empty() {
                if let Err(error) = self.retry_pending_dag(&root_cid).await {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Failed to resolve complete restored pending DAG"
                    );
                }
            } else {
                // Restored incomplete roots stay immediately due. The same
                // receiver clock that owns live registrations performs the
                // fetch claim; restart is not an alternate dispatch path.
                tracing::debug!(
                    root_cid = %root_cid,
                    missing_count = missing.len(),
                    "Restored pending DAG left due for receiver clock"
                );
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

        // Registration, successful-merge cleanup, and quarantine are all
        // transitions of the same durable obligation. Keep their storage
        // transactions behind one manager-local writer so duplicate terminal
        // observations cannot exhaust OCC retries against the same keys.
        let _metadata_writer = self.pending_metadata_writer.lock().await;

        // Checked BEFORE the write: a remote re-push of an already-rejected
        // root re-runs this function (repeat rejection is the expected,
        // sender-paced retry — not a bug), and the gauge below counts
        // distinct quarantined *roots*, not quarantine *occurrences*.
        // Without this check a repeat rejection would drift the gauge above
        // the true record count until the next restart's
        // `install_pending_dag_store` hydration silently corrects it. The
        // occurrence-level diagnostic counter
        // (`pending_dag_terminal_quarantined`, below) is deliberately NOT
        // deduped the same way — it exists to count every rejection event
        // for alerting, gauge or no.
        let mut already_quarantined = false;

        if let Some(store) = self.pending_store() {
            already_quarantined = matches!(store.is_quarantined(root_cid).await, Ok(true));

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
            self.forget_persisted_scope_root(root_cid);
        }

        self.pending_dags.write().remove(root_cid);
        if !already_quarantined {
            self.quarantined_pending_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
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
                    head_priority: dag.head_priority,
                    creator: dag.creator.clone(),
                    source_peer: dag.source_peer.clone(),
                    alternate_providers: dag.alternate_providers.clone(),
                    is_explicit_replicator: dag.is_explicit_replicator,
                    explicit_replay_authorization: dag
                        .explicit_replay_authorization
                        .as_ref()
                        .map(Into::into),
                })
                .unwrap_or_else(|| PersistedPendingDag {
                    doc_id: String::new(),
                    collection_id: String::new(),
                    head_priority: None,
                    creator: String::new(),
                    source_peer: None,
                    alternate_providers: Vec::new(),
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
#[path = "pending_dag_tests.rs"]
mod tests;
