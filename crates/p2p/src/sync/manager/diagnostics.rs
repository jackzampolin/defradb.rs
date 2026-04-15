//! Counters for P2P sync observability.
//!
//! Increments are cheap atomic operations; snapshots provide a point-in-time
//! view so integration tests can assert bounded retry behavior without
//! scraping noisy logs (see issue #858).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct SyncDiagnostics {
    car_empty_responses: AtomicU64,
    car_no_blocks_served: AtomicU64,
    missing_link_retries: AtomicU64,
    pending_dag_resolved: AtomicU64,
    pending_dag_expired: AtomicU64,
    gossip_decode_failures: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncDiagnosticsSnapshot {
    pub car_empty_responses: u64,
    pub car_no_blocks_served: u64,
    pub missing_link_retries: u64,
    pub pending_dag_resolved: u64,
    pub pending_dag_expired: u64,
    pub gossip_decode_failures: u64,
}

impl SyncDiagnostics {
    pub fn snapshot(&self) -> SyncDiagnosticsSnapshot {
        SyncDiagnosticsSnapshot {
            car_empty_responses: self.car_empty_responses.load(Ordering::Relaxed),
            car_no_blocks_served: self.car_no_blocks_served.load(Ordering::Relaxed),
            missing_link_retries: self.missing_link_retries.load(Ordering::Relaxed),
            pending_dag_resolved: self.pending_dag_resolved.load(Ordering::Relaxed),
            pending_dag_expired: self.pending_dag_expired.load(Ordering::Relaxed),
            gossip_decode_failures: self.gossip_decode_failures.load(Ordering::Relaxed),
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

    pub fn record_gossip_decode_failure(&self) {
        self.gossip_decode_failures.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_starts_at_zero() {
        let diag = SyncDiagnostics::default();
        assert_eq!(diag.snapshot(), SyncDiagnosticsSnapshot::default());
    }

    #[test]
    fn each_record_increments_its_counter() {
        let diag = SyncDiagnostics::default();
        diag.record_car_empty_response();
        diag.record_car_empty_response();
        diag.record_car_no_blocks_served();
        diag.record_missing_link_retry();
        diag.record_pending_dag_resolved();
        diag.record_pending_dag_expired();
        diag.record_gossip_decode_failure();

        let snap = diag.snapshot();
        assert_eq!(snap.car_empty_responses, 2);
        assert_eq!(snap.car_no_blocks_served, 1);
        assert_eq!(snap.missing_link_retries, 1);
        assert_eq!(snap.pending_dag_resolved, 1);
        assert_eq!(snap.pending_dag_expired, 1);
        assert_eq!(snap.gossip_decode_failures, 1);
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
