//! Peer lifecycle, queries, and maintenance operations.

use std::time::Instant;

use cid::Cid;

use super::{PeerInfo, PeerStateTracker};
use crate::sync::peer_state::stats::PeerStats;
use crate::topics::{DOC_SYNC_TOPIC, ENCRYPTION_TOPIC, SYNC_BRANCHABLE_TOPIC};

fn is_topic_or_subtopic(topic: &str, base: &str) -> bool {
    topic == base
        || topic
            .strip_prefix(base)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn is_data_subscription_topic(topic: &str) -> bool {
    topic != ENCRYPTION_TOPIC
        && !is_topic_or_subtopic(topic, DOC_SYNC_TOPIC)
        && !is_topic_or_subtopic(topic, SYNC_BRANCHABLE_TOPIC)
}

impl PeerStateTracker {
    /// Record that a peer connected.
    pub fn peer_connected(&self, peer_id: &str) {
        let mut peers = self.peers.write();
        let info = peers
            .entry(peer_id.to_string())
            .or_insert_with(PeerInfo::new);
        info.connected = true;
        info.last_seen = Instant::now();
        self.enforce_global_limits(&mut peers);
    }

    /// Record that a peer disconnected.
    pub fn peer_disconnected(&self, peer_id: &str) {
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
    pub fn peer_has_cid(&self, peer_id: &str, cid: Cid) {
        let mut peers = self.peers.write();
        let max_cids = self.max_cids_per_peer;
        let info = peers
            .entry(peer_id.to_string())
            .or_insert_with(PeerInfo::new);
        info.add_cid(cid, max_cids);
        info.debug_assert_cid_tracking_consistent();
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
    pub fn peer_has_cids(&self, peer_id: &str, cids: impl IntoIterator<Item = Cid>) {
        let mut peers = self.peers.write();
        let max_cids = self.max_cids_per_peer;
        let info = peers
            .entry(peer_id.to_string())
            .or_insert_with(PeerInfo::new);
        for cid in cids {
            info.add_cid(cid, max_cids);
        }
        info.debug_assert_cid_tracking_consistent();
        info.last_seen = Instant::now();
        self.enforce_global_limits(&mut peers);
    }

    /// Record that a peer subscribed to a collection.
    pub fn peer_subscribed(&self, peer_id: &str, collection_id: String) {
        let mut peers = self.peers.write();
        let info = peers
            .entry(peer_id.to_string())
            .or_insert_with(PeerInfo::new);
        info.subscribed_collections.insert(collection_id);
        info.last_seen = Instant::now();
    }

    /// Record that a peer unsubscribed from a collection.
    pub fn peer_unsubscribed(&self, peer_id: &str, collection_id: &str) {
        let mut peers = self.peers.write();
        if let Some(info) = peers.get_mut(peer_id) {
            info.subscribed_collections.remove(collection_id);
            info.last_seen = Instant::now();
        }
    }

    /// Check if a peer likely has a CID.
    pub fn peer_has(&self, peer_id: &str, cid: &Cid) -> bool {
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
    pub fn peers_with_cid(&self, cid: &Cid) -> Vec<String> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| info.connected && info.known_cids.contains(cid))
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    /// Get all connected peers subscribed to a collection.
    pub fn peers_for_collection(&self, collection_id: &str) -> Vec<String> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| {
                info.connected && info.subscribed_collections.contains(collection_id)
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    /// Check if a peer has advertised interest in any data topic.
    ///
    /// System RPC topics are excluded because every libp2p node joins
    /// them at startup; accepting those would collapse Controlled mode back
    /// to "any connected sync peer".
    pub fn peer_has_data_subscription(&self, peer_id: &str) -> bool {
        let peers = self.peers.read();
        peers
            .get(peer_id)
            .map(|info| {
                info.subscribed_collections
                    .iter()
                    .any(|topic| is_data_subscription_topic(topic))
            })
            .unwrap_or(false)
    }

    /// Check if a peer has advertised interest in a specific data collection.
    pub fn peer_subscribed_to_collection(&self, peer_id: &str, collection_id: &str) -> bool {
        let peers = self.peers.read();
        peers
            .get(peer_id)
            .map(|info| info.subscribed_collections.contains(collection_id))
            .unwrap_or(false)
    }

    /// Get all connected peers.
    pub fn connected_peers(&self) -> Vec<String> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| info.connected)
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    /// Get peers that DON'T have a CID (potential recipients for broadcast).
    ///
    /// Returns connected peers that haven't announced having this CID.
    pub fn peers_without_cid(&self, cid: &Cid) -> Vec<String> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, info)| info.connected && !info.known_cids.contains(cid))
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    /// Get number of CIDs known for a peer.
    pub fn peer_cid_count(&self, peer_id: &str) -> usize {
        let peers = self.peers.read();
        peers
            .get(peer_id)
            .map(|info| info.known_cids.len())
            .unwrap_or(0)
    }

    /// Check if a peer is connected.
    pub fn is_connected(&self, peer_id: &str) -> bool {
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
