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
    missing_link_retries: AtomicU64,
    pending_dag_resolved: AtomicU64,
    pending_dag_expired: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncDiagnosticsSnapshot {
    pub car_empty_responses: u64,
    pub car_no_blocks_served: u64,
    pub missing_link_retries: u64,
    pub pending_dag_resolved: u64,
    pub pending_dag_expired: u64,
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
            missing_link_retries: self.missing_link_retries.load(Ordering::Relaxed),
            pending_dag_resolved: self.pending_dag_resolved.load(Ordering::Relaxed),
            pending_dag_expired: self.pending_dag_expired.load(Ordering::Relaxed),
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

    pub fn record_missing_link_retry(&self) {
        self.missing_link_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_resolved(&self) {
        self.pending_dag_resolved.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pending_dag_expired(&self) {
        self.pending_dag_expired.fetch_add(1, Ordering::Relaxed);
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
        assert_eq!(snap.missing_link_retries, 0);
        assert_eq!(snap.pending_dag_resolved, 0);
        assert_eq!(snap.pending_dag_expired, 0);
    }

    #[test]
    fn each_per_manager_record_increments_its_counter() {
        let diag = SyncDiagnostics::default();
        diag.record_car_empty_response();
        diag.record_car_empty_response();
        diag.record_car_no_blocks_served();
        diag.record_missing_link_retry();
        diag.record_pending_dag_resolved();
        diag.record_pending_dag_expired();

        let snap = diag.snapshot();
        assert_eq!(snap.car_empty_responses, 2);
        assert_eq!(snap.car_no_blocks_served, 1);
        assert_eq!(snap.missing_link_retries, 1);
        assert_eq!(snap.pending_dag_resolved, 1);
        assert_eq!(snap.pending_dag_expired, 1);
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
