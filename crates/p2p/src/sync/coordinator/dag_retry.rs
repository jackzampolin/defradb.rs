//! Bounded retry and provider rotation for poll-based DAG fetches (#1093).

use std::time::Duration;

use crate::transport::PeerId;

/// Total fetch attempts per root before the failure is escalated to ERROR.
///
/// Each attempt spends up to 10s on the CAR try plus a stall budget of one
/// full provider rotation (providers × 30s) on stalled selective windows, so
/// three attempts bound the worst-case dead-provider task lifetime at roughly
/// 6.5 minutes at the 4-provider cap — independent of how many batches the
/// missing frontier spans — while still surviving a transiently overloaded or
/// reconnecting provider set.
pub(super) const MAX_FETCH_ATTEMPTS: u32 = 3;

/// Base backoff between attempts; doubles per retry (2s, then 4s).
///
/// The backoff mainly de-synchronizes fleet-wide retry waves — the per-batch
/// 30s fetch window is the dominant wait, not the backoff itself.
const FETCH_RETRY_BACKOFF_BASE: Duration = Duration::from_secs(2);

/// Backoff to sleep before `attempt` (1-based; only called for attempts >= 2).
pub(super) fn retry_backoff(attempt: u32) -> Duration {
    FETCH_RETRY_BACKOFF_BASE * 2u32.saturating_pow(attempt.saturating_sub(2))
}

/// Round-robin cursor over the fetch providers for one DAG root.
///
/// The cursor persists across batches and attempts so a provider that just
/// timed out is not immediately re-tried while alternates remain.
pub(super) struct ProviderRotation {
    peers: Vec<PeerId>,
    cursor: usize,
}

impl ProviderRotation {
    pub(super) fn new(peers: Vec<PeerId>) -> Self {
        debug_assert!(!peers.is_empty(), "DagFetchContext always has source_peer");
        Self { peers, cursor: 0 }
    }

    pub(super) fn current(&self) -> &PeerId {
        &self.peers[self.cursor % self.peers.len()]
    }

    pub(super) fn advance(&mut self) {
        self.cursor = (self.cursor + 1) % self.peers.len();
    }

    pub(super) fn len(&self) -> usize {
        self.peers.len()
    }

    pub(super) fn peers(&self) -> &[PeerId] {
        &self.peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str) -> PeerId {
        PeerId::new(id.to_string())
    }

    #[test]
    fn provider_rotation_wraps_around() {
        let mut rotation = ProviderRotation::new(vec![peer("a"), peer("b"), peer("c")]);
        assert_eq!(rotation.len(), 3);
        assert_eq!(rotation.current(), &peer("a"));
        rotation.advance();
        assert_eq!(rotation.current(), &peer("b"));
        rotation.advance();
        assert_eq!(rotation.current(), &peer("c"));
        rotation.advance();
        assert_eq!(rotation.current(), &peer("a"));
    }

    #[test]
    fn retry_backoff_doubles_per_attempt() {
        assert_eq!(retry_backoff(2), Duration::from_secs(2));
        assert_eq!(retry_backoff(3), Duration::from_secs(4));
    }
}
