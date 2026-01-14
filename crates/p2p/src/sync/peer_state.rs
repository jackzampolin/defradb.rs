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

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use cid::Cid;
use libp2p::PeerId;

/// Information about a single peer's sync state.
#[derive(Debug)]
struct PeerInfo {
    /// CIDs this peer has announced or we've sent to them
    known_cids: HashSet<Cid>,
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
            subscribed_collections: HashSet::new(),
            last_seen: Instant::now(),
            connected: false,
        }
    }
}

/// Tracks the sync state of all known peers.
///
/// Thread-safe: can be shared across tasks.
pub struct PeerStateTracker {
    /// Per-peer state
    peers: RwLock<HashMap<PeerId, PeerInfo>>,
    /// How long to keep peer info after disconnect
    peer_ttl: Duration,
}

impl Default for PeerStateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerStateTracker {
    /// Create a new peer state tracker.
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl: Duration::from_secs(3600), // 1 hour default
        }
    }

    /// Create with custom peer TTL.
    pub fn with_ttl(peer_ttl: Duration) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
        }
    }

    /// Record that a peer connected.
    pub fn peer_connected(&self, peer_id: PeerId) {
        let mut peers = self.peers.write();
        let info = peers.entry(peer_id).or_insert_with(PeerInfo::new);
        info.connected = true;
        info.last_seen = Instant::now();
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
    pub fn peer_has_cid(&self, peer_id: &PeerId, cid: Cid) {
        let mut peers = self.peers.write();
        let info = peers.entry(*peer_id).or_insert_with(PeerInfo::new);
        info.known_cids.insert(cid);
        info.last_seen = Instant::now();
    }

    /// Record multiple CIDs for a peer.
    ///
    /// Creates a peer entry if one doesn't exist.
    pub fn peer_has_cids(&self, peer_id: &PeerId, cids: impl IntoIterator<Item = Cid>) {
        let mut peers = self.peers.write();
        let info = peers.entry(*peer_id).or_insert_with(PeerInfo::new);
        info.known_cids.extend(cids);
        info.last_seen = Instant::now();
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

        PeerStats {
            total_peers: peers.len(),
            connected_peers: connected,
            total_tracked_cids: total_cids,
        }
    }
}

/// Statistics about tracked peers.
#[derive(Debug, Clone)]
pub struct PeerStats {
    /// Total peers (connected + recently disconnected)
    pub total_peers: usize,
    /// Currently connected peers
    pub connected_peers: usize,
    /// Total CIDs tracked across all peers
    pub total_tracked_cids: usize,
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
        assert_eq!(stats.total_peers, 2);
        assert_eq!(stats.connected_peers, 2);
        assert_eq!(stats.total_tracked_cids, 1);

        tracker.peer_disconnected(&peer2);
        let stats = tracker.stats();
        assert_eq!(stats.connected_peers, 1);
    }

    #[test]
    fn test_cleanup_stale() {
        let tracker = PeerStateTracker::with_ttl(Duration::from_millis(10));
        let peer = test_peer_id();

        tracker.peer_connected(peer);
        tracker.peer_disconnected(&peer);

        // Peer still exists right after disconnect
        let stats = tracker.stats();
        assert_eq!(stats.total_peers, 1);

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(20));

        // Cleanup should remove the stale peer
        tracker.cleanup_stale();
        let stats = tracker.stats();
        assert_eq!(stats.total_peers, 0);
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
        assert_eq!(tracker.stats().total_peers, 0);

        // Record that the peer has a CID (before peer_connected is called)
        tracker.peer_has_cid(&peer, cid);

        // Peer entry should be created (but not connected)
        assert_eq!(tracker.stats().total_peers, 1);
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
        assert_eq!(tracker.stats().total_peers, 1);
        assert_eq!(tracker.stats().total_tracked_cids, 2);
        assert!(tracker.peer_has(&peer, &cid1));
        assert!(tracker.peer_has(&peer, &cid2));

        // Not connected yet
        assert!(!tracker.is_connected(&peer));
        assert!(tracker.peers_with_cid(&cid1).is_empty());
    }
}
