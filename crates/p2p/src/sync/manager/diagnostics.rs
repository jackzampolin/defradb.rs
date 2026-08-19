//! Counters for P2P sync observability.
//!
//! Increments are cheap atomic operations; snapshots provide a point-in-time
//! view so integration tests can assert bounded retry behavior without
//! scraping noisy logs (see issue #858).
//!
//! Most counters live on the `SyncDiagnostics` struct and are scoped to one
//! `SyncManager`. Gossip decode failures are kept in a process-global static
//! because the libp2p and iroh gossip paths run in the transport layer and do
//! not have direct access to a `SyncManager`.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
};

use parking_lot::Mutex;

/// Process-global counter of gossip payload decode failures.
///
/// Both the libp2p and iroh gossip paths increment this via
/// [`record_gossip_decode_failure`]. Exposed through every
/// `SyncDiagnostics::snapshot()` so tests see the same number across all
/// managers in the process.
static GOSSIP_DECODE_FAILURES: AtomicU64 = AtomicU64::new(0);
static RECENT_GOSSIP_DECODE_FAILURES: LazyLock<Mutex<VecDeque<GossipDecodeFailureSample>>> =
    LazyLock::new(|| Mutex::new(VecDeque::with_capacity(GOSSIP_DECODE_FAILURE_SAMPLE_LIMIT)));

const GOSSIP_DECODE_FAILURE_SAMPLE_LIMIT: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GossipTransport {
    Iroh,
    Libp2p,
}

impl GossipTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Iroh => "iroh",
            Self::Libp2p => "libp2p",
        }
    }
}

impl std::fmt::Display for GossipTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipDecodeFailureSample {
    pub transport: GossipTransport,
    pub peer_id: String,
    pub topic: String,
    pub message_size: usize,
    pub error: String,
    pub payload_fingerprint: String,
    pub payload_shape_hint: String,
    pub occurrences: u64,
}

/// Record a gossip payload decode failure and return the new total count.
///
/// Return value lets callers apply exponential-backoff sampling (warn on
/// 1st/2nd/4th/8th… occurrence, debug otherwise) without maintaining their
/// own atomic.
///
/// Crate-internal: only the libp2p and iroh transport paths call this.
/// Downstream consumers read the total via `SyncDiagnostics::snapshot()`
/// rather than synthesizing failures.
#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
pub(crate) fn record_gossip_decode_failure() -> u64 {
    GOSSIP_DECODE_FAILURES.fetch_add(1, Ordering::Relaxed) + 1
}

/// Record a gossip payload decode failure and retain a deduplicated sample.
#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
pub(crate) fn record_gossip_decode_failure_sample(mut sample: GossipDecodeFailureSample) -> u64 {
    let total = record_gossip_decode_failure();
    let mut recent = RECENT_GOSSIP_DECODE_FAILURES.lock();

    if let Some(index) = recent.iter().position(|existing| {
        existing.transport == sample.transport
            && existing.peer_id == sample.peer_id
            && existing.topic == sample.topic
            && existing.payload_fingerprint == sample.payload_fingerprint
            && existing.payload_shape_hint == sample.payload_shape_hint
    }) {
        let mut existing = recent
            .remove(index)
            .expect("matched gossip decode failure sample should exist");
        existing.message_size = sample.message_size;
        existing.error = sample.error;
        existing.occurrences += 1;
        recent.push_back(existing);
    } else {
        sample.occurrences = 1;
        recent.push_back(sample);
        while recent.len() > GOSSIP_DECODE_FAILURE_SAMPLE_LIMIT {
            recent.pop_front();
        }
    }

    total
}

#[derive(Debug, Default)]
pub struct SyncDiagnostics {
    car_empty_responses: AtomicU64,
    car_no_blocks_served: AtomicU64,
    car_requested_cids: AtomicU64,
    car_present_cids: AtomicU64,
    car_served_cids: AtomicU64,
    car_filtered_cids: AtomicU64,
    provider_rotations: AtomicU64,
    missing_link_retries: AtomicU64,
    pending_dag_resolved: AtomicU64,
    pending_dag_registered: AtomicU64,
    pending_dag_expired: AtomicU64,
    pending_dag_high_water: AtomicU64,
    persisted_pending_dag_high_water: AtomicU64,
    single_flight_suppressed: AtomicU64,
    already_merged_fast_path: AtomicU64,
    pending_dag_capacity_shed: AtomicU64,
    pending_dag_retry_dispatched: AtomicU64,
    pending_dag_retry_suppressed: AtomicU64,
    pending_dag_fetch_deferred_unavailable: AtomicU64,
    pending_dag_fetch_deferred_contention: AtomicU64,
    pending_dag_fetch_exhausted: AtomicU64,
    pending_dag_terminal_merged: AtomicU64,
    pending_dag_terminal_quarantined: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncDiagnosticsSnapshot {
    pub car_empty_responses: u64,
    pub car_no_blocks_served: u64,
    pub car_requested_cids: u64,
    pub car_present_cids: u64,
    pub car_served_cids: u64,
    pub car_filtered_cids: u64,
    pub provider_rotations: u64,
    pub missing_link_retries: u64,
    pub pending_dag_resolved: u64,
    pub pending_dag_registered: u64,
    pub pending_dag_expired: u64,
    pub pending_dag_high_water: u64,
    pub persisted_pending_dag_high_water: u64,
    pub single_flight_suppressed: u64,
    pub already_merged_fast_path: u64,
    pub pending_dag_capacity_shed: u64,
    pub pending_dag_retry_dispatched: u64,
    pub pending_dag_retry_suppressed: u64,
    pub pending_dag_fetch_deferred_unavailable: u64,
    pub pending_dag_fetch_deferred_contention: u64,
    pub pending_dag_fetch_exhausted: u64,
    pub pending_dag_terminal_merged: u64,
    /// Pending-DAG registrations moved to quarantine after a deterministic
    /// merge rejection (#1128). See `SyncManager::quarantine_pending_dag`.
    pub pending_dag_terminal_quarantined: u64,
    /// Process-global; see [`record_gossip_decode_failure`].
    pub gossip_decode_failures: u64,
    /// Process-global recent samples captured by
    /// [`record_gossip_decode_failure_sample`].
    pub recent_gossip_decode_failures: Vec<GossipDecodeFailureSample>,
}

impl SyncDiagnostics {
    pub fn snapshot(&self) -> SyncDiagnosticsSnapshot {
        SyncDiagnosticsSnapshot {
            car_empty_responses: self.car_empty_responses.load(Ordering::Relaxed),
            car_no_blocks_served: self.car_no_blocks_served.load(Ordering::Relaxed),
            car_requested_cids: self.car_requested_cids.load(Ordering::Relaxed),
            car_present_cids: self.car_present_cids.load(Ordering::Relaxed),
            car_served_cids: self.car_served_cids.load(Ordering::Relaxed),
            car_filtered_cids: self.car_filtered_cids.load(Ordering::Relaxed),
            provider_rotations: self.provider_rotations.load(Ordering::Relaxed),
            missing_link_retries: self.missing_link_retries.load(Ordering::Relaxed),
            pending_dag_resolved: self.pending_dag_resolved.load(Ordering::Relaxed),
            pending_dag_registered: self.pending_dag_registered.load(Ordering::Relaxed),
            pending_dag_expired: self.pending_dag_expired.load(Ordering::Relaxed),
            pending_dag_high_water: self.pending_dag_high_water.load(Ordering::Relaxed),
            persisted_pending_dag_high_water: self
                .persisted_pending_dag_high_water
                .load(Ordering::Relaxed),
            single_flight_suppressed: self.single_flight_suppressed.load(Ordering::Relaxed),
            already_merged_fast_path: self.already_merged_fast_path.load(Ordering::Relaxed),
            pending_dag_capacity_shed: self.pending_dag_capacity_shed.load(Ordering::Relaxed),
            pending_dag_retry_dispatched: self.pending_dag_retry_dispatched.load(Ordering::Relaxed),
            pending_dag_retry_suppressed: self.pending_dag_retry_suppressed.load(Ordering::Relaxed),
            pending_dag_fetch_deferred_unavailable: self
                .pending_dag_fetch_deferred_unavailable
                .load(Ordering::Relaxed),
            pending_dag_fetch_deferred_contention: self
                .pending_dag_fetch_deferred_contention
                .load(Ordering::Relaxed),
            pending_dag_fetch_exhausted: self.pending_dag_fetch_exhausted.load(Ordering::Relaxed),
            pending_dag_terminal_merged: self.pending_dag_terminal_merged.load(Ordering::Relaxed),
            pending_dag_terminal_quarantined: self
                .pending_dag_terminal_quarantined
                .load(Ordering::Relaxed),
            gossip_decode_failures: GOSSIP_DECODE_FAILURES.load(Ordering::Relaxed),
            recent_gossip_decode_failures: RECENT_GOSSIP_DECODE_FAILURES
                .lock()
                .iter()
                .cloned()
                .collect(),
        }
    }

    pub fn record_car_empty_response(&self) {
        self.car_empty_responses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_car_no_blocks_served(&self) {
        self.car_no_blocks_served.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_car_serve_counts(
        &self,
        requested: usize,
        present: usize,
        served: usize,
        filtered: usize,
    ) {
        self.car_requested_cids
            .fetch_add(requested as u64, Ordering::Relaxed);
        self.car_present_cids
            .fetch_add(present as u64, Ordering::Relaxed);
        self.car_served_cids
            .fetch_add(served as u64, Ordering::Relaxed);
        self.car_filtered_cids
            .fetch_add(filtered as u64, Ordering::Relaxed);
    }

    pub fn record_provider_rotation(&self) {
        self.provider_rotations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_missing_link_retry(&self) {
        self.missing_link_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_resolved(&self) {
        self.pending_dag_resolved.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_registered(&self) {
        self.pending_dag_registered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_expired(&self) {
        self.pending_dag_expired.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_pending_dag_depth(&self, current: usize) {
        self.pending_dag_high_water
            .fetch_max(current as u64, Ordering::Relaxed);
    }

    pub fn observe_persisted_pending_dag_depth(&self, current: usize) {
        self.persisted_pending_dag_high_water
            .fetch_max(current as u64, Ordering::Relaxed);
    }

    pub fn record_single_flight_suppressed(&self) {
        self.single_flight_suppressed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_already_merged_fast_path(&self) {
        self.already_merged_fast_path
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_capacity_shed(&self) {
        self.pending_dag_capacity_shed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_retry_dispatched(&self) {
        self.pending_dag_retry_dispatched
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_retry_suppressed(&self) {
        self.pending_dag_retry_suppressed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_fetch_deferred_unavailable(&self) {
        self.pending_dag_fetch_deferred_unavailable
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_fetch_deferred_contention(&self) {
        self.pending_dag_fetch_deferred_contention
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_fetch_exhausted(&self) {
        self.pending_dag_fetch_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_terminal_merged(&self) {
        self.pending_dag_terminal_merged
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_terminal_quarantined(&self) {
        self.pending_dag_terminal_quarantined
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_manager_counters_start_at_zero() {
        let diag = SyncDiagnostics::default();
        let snap = diag.snapshot();
        assert_eq!(snap.car_empty_responses, 0);
        assert_eq!(snap.car_no_blocks_served, 0);
        assert_eq!(snap.provider_rotations, 0);
        assert_eq!(snap.missing_link_retries, 0);
        assert_eq!(snap.pending_dag_resolved, 0);
        assert_eq!(snap.pending_dag_registered, 0);
        assert_eq!(snap.pending_dag_expired, 0);
        assert_eq!(snap.pending_dag_high_water, 0);
        assert_eq!(snap.persisted_pending_dag_high_water, 0);
        assert_eq!(snap.single_flight_suppressed, 0);
        assert_eq!(snap.already_merged_fast_path, 0);
        assert_eq!(snap.pending_dag_capacity_shed, 0);
        assert_eq!(snap.pending_dag_retry_dispatched, 0);
        assert_eq!(snap.pending_dag_retry_suppressed, 0);
        assert_eq!(snap.pending_dag_fetch_deferred_unavailable, 0);
        assert_eq!(snap.pending_dag_fetch_deferred_contention, 0);
        assert_eq!(snap.pending_dag_fetch_exhausted, 0);
        assert_eq!(snap.pending_dag_terminal_merged, 0);
        assert_eq!(snap.pending_dag_terminal_quarantined, 0);
    }

    #[test]
    fn each_per_manager_record_increments_its_counter() {
        let diag = SyncDiagnostics::default();
        diag.record_car_empty_response();
        diag.record_car_empty_response();
        diag.record_car_no_blocks_served();
        diag.record_provider_rotation();
        diag.record_missing_link_retry();
        diag.record_pending_dag_resolved();
        diag.record_pending_dag_registered();
        diag.record_pending_dag_expired();
        diag.observe_pending_dag_depth(2);
        diag.observe_pending_dag_depth(1);
        diag.observe_persisted_pending_dag_depth(3);
        diag.record_single_flight_suppressed();
        diag.record_already_merged_fast_path();
        diag.record_pending_dag_capacity_shed();
        diag.record_pending_dag_retry_dispatched();
        diag.record_pending_dag_retry_suppressed();
        diag.record_pending_dag_fetch_deferred_unavailable();
        diag.record_pending_dag_fetch_deferred_contention();
        diag.record_pending_dag_fetch_exhausted();
        diag.record_pending_dag_terminal_merged();
        diag.record_pending_dag_terminal_quarantined();

        let snap = diag.snapshot();
        assert_eq!(snap.car_empty_responses, 2);
        assert_eq!(snap.car_no_blocks_served, 1);
        assert_eq!(snap.provider_rotations, 1);
        assert_eq!(snap.missing_link_retries, 1);
        assert_eq!(snap.pending_dag_resolved, 1);
        assert_eq!(snap.pending_dag_registered, 1);
        assert_eq!(snap.pending_dag_expired, 1);
        assert_eq!(snap.pending_dag_high_water, 2);
        assert_eq!(snap.persisted_pending_dag_high_water, 3);
        assert_eq!(snap.single_flight_suppressed, 1);
        assert_eq!(snap.already_merged_fast_path, 1);
        assert_eq!(snap.pending_dag_capacity_shed, 1);
        assert_eq!(snap.pending_dag_retry_dispatched, 1);
        assert_eq!(snap.pending_dag_retry_suppressed, 1);
        assert_eq!(snap.pending_dag_fetch_deferred_unavailable, 1);
        assert_eq!(snap.pending_dag_fetch_deferred_contention, 1);
        assert_eq!(snap.pending_dag_fetch_exhausted, 1);
        assert_eq!(snap.pending_dag_terminal_merged, 1);
        assert_eq!(snap.pending_dag_terminal_quarantined, 1);
    }

    #[test]
    fn provider_rotations_are_manager_local() {
        let first = SyncDiagnostics::default();
        let second = SyncDiagnostics::default();
        first.record_provider_rotation();

        assert_eq!(first.snapshot().provider_rotations, 1);
        assert_eq!(second.snapshot().provider_rotations, 0);
    }

    #[test]
    fn record_gossip_decode_failure_returns_monotonic_total() {
        // Global counter may already be non-zero from other tests in the
        // process — only assert monotonicity.
        let first = record_gossip_decode_failure();
        let second = record_gossip_decode_failure();
        assert_eq!(second, first + 1);
        assert!(SyncDiagnostics::default().snapshot().gossip_decode_failures >= second);
    }

    #[test]
    fn record_gossip_decode_failure_sample_surfaces_recent_metadata() {
        let topic = format!("topic-{}", uuid::Uuid::new_v4());
        let prefix = format!("deadbeef-{}", uuid::Uuid::new_v4());
        let sample = GossipDecodeFailureSample {
            transport: GossipTransport::Iroh,
            peer_id: "peer-123".to_string(),
            topic: topic.clone(),
            message_size: 128,
            error: "Hit the end of buffer".to_string(),
            payload_fingerprint: prefix.clone(),
            payload_shape_hint: "cbor_map(keys=[Unexpected])".to_string(),
            occurrences: 0,
        };

        let total = record_gossip_decode_failure_sample(sample);
        let snapshot = SyncDiagnostics::default().snapshot();
        assert!(snapshot.gossip_decode_failures >= total);
        assert!(snapshot.recent_gossip_decode_failures.iter().any(|entry| {
            entry.transport == GossipTransport::Iroh
                && entry.topic == topic
                && entry.payload_fingerprint == prefix
                && entry.occurrences >= 1
        }));
    }

    #[test]
    fn concurrent_increments_are_not_lost() {
        use std::sync::Arc;
        use std::thread;

        let diag = Arc::new(SyncDiagnostics::default());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let d = Arc::clone(&diag);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    d.record_car_empty_response();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(diag.snapshot().car_empty_responses, 8_000);
    }
}
