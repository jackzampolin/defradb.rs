// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Peer state tracking for P2P synchronization.
//!
//! Tracks which blocks each peer has, enabling:
//! - Efficient block requests (ask peers who have the block)
//! - Avoiding redundant sends (don't send blocks peers already have)
//! - Replication status monitoring

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use cid::Cid;
use libp2p::PeerId;

/// Default maximum number of CIDs to track per peer.
/// This prevents unbounded memory growth in long-running nodes.
const DEFAULT_MAX_CIDS_PER_PEER: usize = 10_000;

/// Default maximum total CIDs across all peers.
/// With 100 peers at 10k CIDs each = ~40MB memory usage.
const DEFAULT_MAX_TOTAL_CIDS: usize = 1_000_000;

/// Default maximum number of tracked peers.
const DEFAULT_MAX_PEERS: usize = 1_000;

/// Information about a single peer's sync state.
#[derive(Debug)]
struct PeerInfo {
    /// CIDs this peer has announced or we've sent to them
    known_cids: HashSet<Cid>,
    /// Insertion order for LRU eviction (oldest first)
    cid_order: VecDeque<Cid>,
    /// Collections this peer is subscribed to
    subscribed_collections: HashSet<String>,
    /// When we last heard from this peer
    last_seen: Instant,
    /// Whether peer is currently connected
    connected: bool,
}

impl PeerInfo {
    fn new() -> Self {
        Self {
            known_cids: HashSet::new(),
            cid_order: VecDeque::new(),
            subscribed_collections: HashSet::new(),
            last_seen: Instant::now(),
            connected: false,
        }
    }

    /// Add a CID with LRU eviction if at capacity.
    fn add_cid(&mut self, cid: Cid, max_cids: usize) {
        // If already present, don't add again (maintains LRU order)
        if self.known_cids.contains(&cid) {
            return;
        }

        // Evict oldest if at capacity
        while self.known_cids.len() >= max_cids {
            if let Some(oldest) = self.cid_order.pop_front() {
                self.known_cids.remove(&oldest);
            } else {
                break;
            }
        }

        // Add the new CID
        self.known_cids.insert(cid);
        self.cid_order.push_back(cid);
    }
}

/// Tracks the sync state of all known peers.
///
/// Thread-safe: can be shared across tasks.
///
/// # Memory Limits
///
/// To prevent unbounded memory growth, the tracker enforces three limits:
/// - `max_cids_per_peer`: Maximum CIDs tracked for any single peer (LRU eviction)
/// - `max_total_cids`: Maximum CIDs across ALL peers (oldest peers evicted first)
/// - `max_peers`: Maximum number of tracked peers (oldest disconnected peers evicted)
pub struct PeerStateTracker {
    /// Per-peer state
    peers: RwLock<HashMap<PeerId, PeerInfo>>,
    /// How long to keep peer info after disconnect
    peer_ttl: Duration,
    /// Maximum CIDs to track per peer (prevents memory exhaustion)
    max_cids_per_peer: usize,
    /// Maximum total CIDs across all peers
    max_total_cids: usize,
    /// Maximum number of tracked peers
    max_peers: usize,
}

impl Default for PeerStateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerStateTracker {
    /// Create a new peer state tracker with default settings.
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl: Duration::from_secs(3600), // 1 hour default
            max_cids_per_peer: DEFAULT_MAX_CIDS_PER_PEER,
            max_total_cids: DEFAULT_MAX_TOTAL_CIDS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    /// Create with custom peer TTL.
    pub fn with_ttl(peer_ttl: Duration) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
            max_cids_per_peer: DEFAULT_MAX_CIDS_PER_PEER,
            max_total_cids: DEFAULT_MAX_TOTAL_CIDS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    /// Create with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `peer_ttl` - How long to keep disconnected peer info
    /// * `max_cids_per_peer` - Max CIDs per peer (0 = use default)
    ///
    /// # Logging
    ///
    /// Logs a warning if `max_cids_per_peer` is 0 (falls back to default).
    pub fn with_config(peer_ttl: Duration, max_cids_per_peer: usize) -> Self {
        let max_cids = if max_cids_per_peer == 0 {
            tracing::warn!(
                "max_cids_per_peer was 0, using default value {}",
                DEFAULT_MAX_CIDS_PER_PEER
            );
            DEFAULT_MAX_CIDS_PER_PEER
        } else {
            max_cids_per_peer
        };
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
            max_cids_per_peer: max_cids,
            max_total_cids: DEFAULT_MAX_TOTAL_CIDS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    /// Create with full custom configuration including global limits.
    ///
    /// # Arguments
    ///
    /// * `peer_ttl` - How long to keep disconnected peer info
    /// * `max_cids_per_peer` - Max CIDs per peer (0 = use default)
    /// * `max_total_cids` - Max total CIDs across all peers (0 = use default)
    /// * `max_peers` - Max tracked peers (0 = use default)
    pub fn with_full_config(
        peer_ttl: Duration,
        max_cids_per_peer: usize,
        max_total_cids: usize,
        max_peers: usize,
    ) -> Self {
        let max_cids = if max_cids_per_peer == 0 {
            tracing::warn!(
                "max_cids_per_peer was 0, using default value {}",
                DEFAULT_MAX_CIDS_PER_PEER
            );
            DEFAULT_MAX_CIDS_PER_PEER
        } else {
            max_cids_per_peer
        };
        let max_total = if max_total_cids == 0 {
            tracing::warn!(
                "max_total_cids was 0, using default value {}",
                DEFAULT_MAX_TOTAL_CIDS
            );
            DEFAULT_MAX_TOTAL_CIDS
        } else {
            max_total_cids
        };
        let max_p = if max_peers == 0 {
            tracing::warn!("max_peers was 0, using default value {}", DEFAULT_MAX_PEERS);
            DEFAULT_MAX_PEERS
        } else {
            max_peers
        };
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
            max_cids_per_peer: max_cids,
            max_total_cids: max_total,
            max_peers: max_p,
        }
    }

    /// Enforce global limits by evicting oldest disconnected peers and their CIDs.
    ///
    /// Called internally when adding peers or CIDs.
    fn enforce_global_limits(&self, peers: &mut HashMap<PeerId, PeerInfo>) {
        // Check peer count limit - evict oldest disconnected peers first
        while peers.len() > self.max_peers {
            // Find the oldest disconnected peer
            let oldest_disconnected = peers
                .iter()
                .filter(|(_, info)| !info.connected)
                .min_by_key(|(_, info)| info.last_seen)
                .map(|(id, _)| *id);

            if let Some(peer_id) = oldest_disconnected {
                tracing::debug!(
                    peer_id = %peer_id,
                    "Evicting oldest disconnected peer to stay under max_peers limit"
                );
                peers.remove(&peer_id);
            } else {
                // All peers are connected, can't evict
                tracing::warn!(
                    current = peers.len(),
                    max = self.max_peers,
                    "Cannot evict peers - all are connected"
                );
                break;
            }
        }

        // Check total CID count limit - evict CIDs from peers with most CIDs
        let total_cids: usize = peers.values().map(|info| info.known_cids.len()).sum();
        if total_cids > self.max_total_cids {
            let excess = total_cids - self.max_total_cids;
            let mut evicted = 0;

            // Evict from peers with the most CIDs (disconnected first)
            let mut peer_cid_counts: Vec<_> = peers
                .iter()
                .map(|(id, info)| (*id, info.known_cids.len(), info.connected))
                .collect();

            // Sort by: disconnected first, then by CID count descending
            peer_cid_counts.sort_by(|a, b| {
                // Disconnected peers should come first
                match (a.2, b.2) {
                    (false, true) => std::cmp::Ordering::Less,
                    (true, false) => std::cmp::Ordering::Greater,
                    _ => b.1.cmp(&a.1), // More CIDs first
                }
            });

            for (peer_id, _, _) in peer_cid_counts {
                if evicted >= excess {
                    break;
                }
                if let Some(info) = peers.get_mut(&peer_id) {
                    // Evict oldest CIDs from this peer
                    while evicted < excess && !info.cid_order.is_empty() {
                        if let Some(cid) = info.cid_order.pop_front() {
                            info.known_cids.remove(&cid);
                            evicted += 1;
                        }
                    }
                }
            }

            if evicted > 0 {
                tracing::debug!(
                    evicted = evicted,
                    "Evicted CIDs to stay under max_total_cids limit"
                );
            }
        }
    }

    /// Record that a peer connected.
    pub fn peer_connected(&self, peer_id: PeerId) {
        let mut peers = self.peers.write();
        let info = peers.entry(peer_id).or_insert_with(PeerInfo::new);
        info.connected = true;
        info.last_seen = Instant::now();
        self.enforce_global_limits(&mut peers);
    }

    /// Record that a peer disconnected.
    pub fn peer_disconnected(&self, peer_id: &PeerId) {
        let mut peers = self.peers.write();
        if let Some(info) = peers.get_mut(peer_id) {
            info.connected = false;
            info.last_seen = Instant::now();
        }
    }

    /// Record that a peer has a specific CID.
    ///
    /// Call this when:
    /// - Receiving a block from a peer (they definitely have it)
    /// - Successfully sending a block to a peer (they now have it)
    ///
    /// Creates a peer entry if one doesn't exist (handles race conditions
    /// where CID announcements arrive before connection events).
    ///
    /// Note: CID tracking is bounded by `max_cids_per_peer` (per-peer LRU)
    /// and `max_total_cids` (global limit). When limits are reached, oldest
    /// CIDs are evicted.
    pub fn peer_has_cid(&self, peer_id: &PeerId, cid: Cid) {
        let mut peers = self.peers.write();
        let max_cids = self.max_cids_per_peer;
        let info = peers.entry(*peer_id).or_insert_with(PeerInfo::new);
        info.add_cid(cid, max_cids);
        info.last_seen = Instant::now();
        self.enforce_global_limits(&mut peers);
    }

    /// Record multiple CIDs for a peer.
    ///
    /// Creates a peer entry if one doesn't exist.
    ///
    /// Note: CID tracking is bounded by `max_cids_per_peer` (per-peer LRU)
    /// and `max_total_cids` (global limit). When limits are reached, oldest
    /// CIDs are evicted.
    pub fn peer_has_cids(&self, peer_id: &PeerId, cids: impl IntoIterator<Item = Cid>) {
        let mut peers = self.peers.write();
        let max_cids = self.max_cids_per_peer;
        let info = peers.entry(*peer_id).or_insert_with(PeerInfo::new);
        for cid in cids {
            info.add_cid(cid, max_cids);
        }
        info.last_seen = Instant::now();
        self.enforce_global_limits(&mut peers);
    }

    /// Record that a peer subscribed to a collection.
    pub fn peer_subscribed(&self, peer_id: &PeerId, collection_id: String) {
        let mut peers = self.peers.write();
        let info = peers.entry(*peer_id).or_insert_with(PeerInfo::new);
        info.subscribed_collections.insert(collection_id);
        info.last_seen = Instant::now();
    }

    /// Record that a peer unsubscribed from a collection.
    pub fn peer_unsubscribed(&self, peer_id: &PeerId, collection_id: &str) {
        let mut peers = self.peers.write();
        if let Some(info) = peers.get_mut(peer_id) {
            info.subscribed_collections.remove(collection_id);
            info.last_seen = Instant::now();
        }
    }

    /// Check if a peer likely has a CID.
    pub fn peer_has(&self, peer_id: &PeerId, cid: &Cid) -> bool {
        let peers = self.peers.read();
        peers
            .get(peer_id)
            .map(|info| info.known_cids.contains(cid))
            .unwrap_or(false)
    }

    /// Get all connected peers that might have a CID.
    ///
    /// Returns peers that:
    /// - Are currently connected
    /// - Have announced this CID
    pub fn peers_with_cid(&self, cid: &Cid) -> Vec<PeerId> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| info.connected && info.known_cids.contains(cid))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Get all connected peers subscribed to a collection.
    pub fn peers_for_collection(&self, collection_id: &str) -> Vec<PeerId> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| {
                info.connected && info.subscribed_collections.contains(collection_id)
            })
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Get all connected peers.
    pub fn connected_peers(&self) -> Vec<PeerId> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| info.connected)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Get peers that DON'T have a CID (potential recipients for broadcast).
    ///
    /// Returns connected peers that haven't announced having this CID.
    pub fn peers_without_cid(&self, cid: &Cid) -> Vec<PeerId> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| info.connected && !info.known_cids.contains(cid))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Get number of CIDs known for a peer.
    pub fn peer_cid_count(&self, peer_id: &PeerId) -> usize {
        let peers = self.peers.read();
        peers
            .get(peer_id)
            .map(|info| info.known_cids.len())
            .unwrap_or(0)
    }

    /// Check if a peer is connected.
    pub fn is_connected(&self, peer_id: &PeerId) -> bool {
        let peers = self.peers.read();
        peers
            .get(peer_id)
            .map(|info| info.connected)
            .unwrap_or(false)
    }

    /// Remove stale peer entries that have been disconnected longer than TTL.
    pub fn cleanup_stale(&self) {
        let mut peers = self.peers.write();
        let now = Instant::now();
        peers
            .retain(|_, info| info.connected || now.duration_since(info.last_seen) < self.peer_ttl);
    }

    /// Get statistics about tracked peers.
    pub fn stats(&self) -> PeerStats {
        let peers = self.peers.read();
        let connected = peers.values().filter(|info| info.connected).count();
        let total_cids: usize = peers.values().map(|info| info.known_cids.len()).sum();

        PeerStats::new(peers.len(), connected, total_cids)
    }
}

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
    pub(crate) fn new(total_peers: usize, connected_peers: usize, total_tracked_cids: usize) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_peer_id() -> PeerId {
        PeerId::random()
    }

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    #[test]
    fn test_peer_connect_disconnect() {
        let tracker = PeerStateTracker::new();
        let peer = test_peer_id();

        assert!(!tracker.is_connected(&peer));

        tracker.peer_connected(peer);
        assert!(tracker.is_connected(&peer));

        tracker.peer_disconnected(&peer);
        assert!(!tracker.is_connected(&peer));
    }

    #[test]
    fn test_peer_has_cid() {
        let tracker = PeerStateTracker::new();
        let peer = test_peer_id();
        let cid = test_cid();

        tracker.peer_connected(peer);
        assert!(!tracker.peer_has(&peer, &cid));

        tracker.peer_has_cid(&peer, cid);
        assert!(tracker.peer_has(&peer, &cid));
    }

    #[test]
    fn test_peers_with_cid() {
        let tracker = PeerStateTracker::new();
        let peer1 = test_peer_id();
        let peer2 = test_peer_id();
        let cid = test_cid();

        tracker.peer_connected(peer1);
        tracker.peer_connected(peer2);

        // Only peer1 has the CID
        tracker.peer_has_cid(&peer1, cid);

        let peers_with = tracker.peers_with_cid(&cid);
        assert_eq!(peers_with.len(), 1);
        assert!(peers_with.contains(&peer1));
        assert!(!peers_with.contains(&peer2));
    }

    #[test]
    fn test_peers_without_cid() {
        let tracker = PeerStateTracker::new();
        let peer1 = test_peer_id();
        let peer2 = test_peer_id();
        let cid = test_cid();

        tracker.peer_connected(peer1);
        tracker.peer_connected(peer2);

        // Only peer1 has the CID
        tracker.peer_has_cid(&peer1, cid);

        let peers_without = tracker.peers_without_cid(&cid);
        assert_eq!(peers_without.len(), 1);
        assert!(!peers_without.contains(&peer1));
        assert!(peers_without.contains(&peer2));
    }

    #[test]
    fn test_collection_subscription() {
        let tracker = PeerStateTracker::new();
        let peer = test_peer_id();

        tracker.peer_connected(peer);
        tracker.peer_subscribed(&peer, "users".to_string());

        let peers = tracker.peers_for_collection("users");
        assert_eq!(peers.len(), 1);
        assert!(peers.contains(&peer));

        let peers = tracker.peers_for_collection("posts");
        assert!(peers.is_empty());

        tracker.peer_unsubscribed(&peer, "users");
        let peers = tracker.peers_for_collection("users");
        assert!(peers.is_empty());
    }

    #[test]
    fn test_disconnected_peer_not_in_results() {
        let tracker = PeerStateTracker::new();
        let peer = test_peer_id();
        let cid = test_cid();

        tracker.peer_connected(peer);
        tracker.peer_has_cid(&peer, cid);

        // While connected, peer shows up
        assert_eq!(tracker.peers_with_cid(&cid).len(), 1);

        // After disconnect, peer doesn't show up in active queries
        tracker.peer_disconnected(&peer);
        assert!(tracker.peers_with_cid(&cid).is_empty());

        // But we still remember they have it
        assert!(tracker.peer_has(&peer, &cid));
    }

    #[test]
    fn test_stats() {
        let tracker = PeerStateTracker::new();
        let peer1 = test_peer_id();
        let peer2 = test_peer_id();
        let cid1 = test_cid();

        tracker.peer_connected(peer1);
        tracker.peer_connected(peer2);
        tracker.peer_has_cid(&peer1, cid1);

        let stats = tracker.stats();
        assert_eq!(stats.total_peers(), 2);
        assert_eq!(stats.connected_peers(), 2);
        assert_eq!(stats.total_tracked_cids(), 1);

        tracker.peer_disconnected(&peer2);
        let stats = tracker.stats();
        assert_eq!(stats.connected_peers(), 1);
    }

    #[test]
    fn test_cleanup_stale() {
        let tracker = PeerStateTracker::with_ttl(Duration::from_millis(10));
        let peer = test_peer_id();

        tracker.peer_connected(peer);
        tracker.peer_disconnected(&peer);

        // Peer still exists right after disconnect
        let stats = tracker.stats();
        assert_eq!(stats.total_peers(), 1);

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(20));

        // Cleanup should remove the stale peer
        tracker.cleanup_stale();
        let stats = tracker.stats();
        assert_eq!(stats.total_peers(), 0);
    }

    #[test]
    fn test_connected_peers() {
        let tracker = PeerStateTracker::new();
        let peer1 = test_peer_id();
        let peer2 = test_peer_id();

        tracker.peer_connected(peer1);
        tracker.peer_connected(peer2);

        let connected = tracker.connected_peers();
        assert_eq!(connected.len(), 2);

        tracker.peer_disconnected(&peer1);
        let connected = tracker.connected_peers();
        assert_eq!(connected.len(), 1);
        assert!(connected.contains(&peer2));
    }

    #[test]
    fn test_peer_has_cid_creates_entry_for_unknown_peer() {
        // Test that peer_has_cid creates a peer entry if one doesn't exist
        // This handles race conditions where CID announcements arrive before connection events
        let tracker = PeerStateTracker::new();
        let peer = test_peer_id();
        let cid = test_cid();

        // Peer is not connected yet
        assert!(!tracker.is_connected(&peer));
        assert_eq!(tracker.stats().total_peers(), 0);

        // Record that the peer has a CID (before peer_connected is called)
        tracker.peer_has_cid(&peer, cid);

        // Peer entry should be created (but not connected)
        assert_eq!(tracker.stats().total_peers(), 1);
        assert!(!tracker.is_connected(&peer)); // Still not connected
        assert!(tracker.peer_has(&peer, &cid)); // But we track the CID

        // peers_with_cid should NOT return this peer since they're not connected
        assert!(tracker.peers_with_cid(&cid).is_empty());

        // Now connect the peer
        tracker.peer_connected(peer);
        assert!(tracker.is_connected(&peer));

        // Now they should appear in peers_with_cid
        let peers_with = tracker.peers_with_cid(&cid);
        assert_eq!(peers_with.len(), 1);
        assert!(peers_with.contains(&peer));
    }

    #[test]
    fn test_peer_has_cids_creates_entry_for_unknown_peer() {
        // Test that peer_has_cids also creates a peer entry if one doesn't exist
        let tracker = PeerStateTracker::new();
        let peer = test_peer_id();
        let cid1 = test_cid();
        let cid2 =
            Cid::from_str("bafybeibdqagjfxgsqiafpmyohldmiu4qn6ucudpzqlxkfrmb6dzbggbkxy").unwrap();

        // Record multiple CIDs before peer is connected
        tracker.peer_has_cids(&peer, vec![cid1, cid2]);

        // Peer entry should be created
        assert_eq!(tracker.stats().total_peers(), 1);
        assert_eq!(tracker.stats().total_tracked_cids(), 2);
        assert!(tracker.peer_has(&peer, &cid1));
        assert!(tracker.peer_has(&peer, &cid2));

        // Not connected yet
        assert!(!tracker.is_connected(&peer));
        assert!(tracker.peers_with_cid(&cid1).is_empty());
    }

    #[test]
    fn test_lru_eviction_when_max_cids_exceeded() {
        // Create tracker with a small limit for testing
        let tracker = PeerStateTracker::with_config(Duration::from_secs(3600), 3);
        let peer = test_peer_id();
        tracker.peer_connected(peer);

        // Create 5 different CIDs
        let cid1 = Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();
        let cid2 = Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
            .unwrap();
        let cid3 = Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")
            .unwrap();
        let cid4 = Cid::from_str("bafybeibdqagjfxgsqiafpmyohldmiu4qn6ucudpzqlxkfrmb6dzbggbkxy")
            .unwrap();
        let cid5 = Cid::from_str("bafkreigaknpexyvxt76zgkitavbwx6ejgfheup5oybpm77oxmxbyjaoj4i")
            .unwrap();

        // Add first 3 CIDs - all should be present
        tracker.peer_has_cid(&peer, cid1);
        tracker.peer_has_cid(&peer, cid2);
        tracker.peer_has_cid(&peer, cid3);

        assert!(tracker.peer_has(&peer, &cid1));
        assert!(tracker.peer_has(&peer, &cid2));
        assert!(tracker.peer_has(&peer, &cid3));
        assert_eq!(tracker.peer_cid_count(&peer), 3);

        // Add 4th CID - should evict cid1 (oldest)
        tracker.peer_has_cid(&peer, cid4);

        assert!(!tracker.peer_has(&peer, &cid1)); // Evicted
        assert!(tracker.peer_has(&peer, &cid2));
        assert!(tracker.peer_has(&peer, &cid3));
        assert!(tracker.peer_has(&peer, &cid4));
        assert_eq!(tracker.peer_cid_count(&peer), 3);

        // Add 5th CID - should evict cid2
        tracker.peer_has_cid(&peer, cid5);

        assert!(!tracker.peer_has(&peer, &cid1)); // Evicted earlier
        assert!(!tracker.peer_has(&peer, &cid2)); // Evicted now
        assert!(tracker.peer_has(&peer, &cid3));
        assert!(tracker.peer_has(&peer, &cid4));
        assert!(tracker.peer_has(&peer, &cid5));
        assert_eq!(tracker.peer_cid_count(&peer), 3);
    }

    #[test]
    fn test_adding_same_cid_twice_does_not_evict() {
        let tracker = PeerStateTracker::with_config(Duration::from_secs(3600), 3);
        let peer = test_peer_id();
        tracker.peer_connected(peer);

        let cid1 = Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();
        let cid2 = Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
            .unwrap();
        let cid3 = Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")
            .unwrap();

        // Add 3 CIDs
        tracker.peer_has_cid(&peer, cid1);
        tracker.peer_has_cid(&peer, cid2);
        tracker.peer_has_cid(&peer, cid3);

        // Re-add cid1 multiple times - should not cause eviction
        tracker.peer_has_cid(&peer, cid1);
        tracker.peer_has_cid(&peer, cid1);
        tracker.peer_has_cid(&peer, cid1);

        // All 3 should still be present
        assert!(tracker.peer_has(&peer, &cid1));
        assert!(tracker.peer_has(&peer, &cid2));
        assert!(tracker.peer_has(&peer, &cid3));
        assert_eq!(tracker.peer_cid_count(&peer), 3);
    }
}
