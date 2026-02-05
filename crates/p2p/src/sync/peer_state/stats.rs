//! Peer statistics.

/// Statistics about tracked peers.
///
/// Use accessor methods to read values. This ensures the invariant
/// that `connected_peers <= total_peers` is always maintained.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PeerStats {
    /// Total number of peers being tracked (connected + disconnected).
    total_peers: usize,
    /// Number of currently connected peers.
    connected_peers: usize,
    /// Total CIDs tracked across all peers.
    total_tracked_cids: usize,
}

impl PeerStats {
    /// Create new peer statistics (internal use only).
    pub(crate) fn new(
        total_peers: usize,
        connected_peers: usize,
        total_tracked_cids: usize,
    ) -> Self {
        debug_assert!(
            connected_peers <= total_peers,
            "connected_peers ({}) must be <= total_peers ({})",
            connected_peers,
            total_peers
        );
        Self {
            total_peers,
            connected_peers,
            total_tracked_cids,
        }
    }

    /// Get the total number of peers being tracked.
    pub fn total_peers(&self) -> usize {
        self.total_peers
    }

    /// Get the number of currently connected peers.
    pub fn connected_peers(&self) -> usize {
        self.connected_peers
    }

    /// Get the number of disconnected peers.
    pub fn disconnected_peers(&self) -> usize {
        self.total_peers - self.connected_peers
    }

    /// Get the total CIDs tracked across all peers.
    pub fn total_tracked_cids(&self) -> usize {
        self.total_tracked_cids
    }
}
