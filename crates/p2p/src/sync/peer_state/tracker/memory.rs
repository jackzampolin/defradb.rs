//! Global memory limit enforcement for the peer state tracker.

use std::collections::HashMap;

use super::{PeerInfo, PeerStateTracker};

impl PeerStateTracker {
    /// Enforce global limits by evicting oldest disconnected peers and their CIDs.
    ///
    /// Called internally when adding peers or CIDs.
    pub(super) fn enforce_global_limits(&self, peers: &mut HashMap<String, PeerInfo>) {
        // Check peer count limit - evict oldest disconnected peers first
        while peers.len() > self.max_peers {
            // Find the oldest disconnected peer
            let oldest_disconnected = peers
                .iter()
                .filter(|(_, info)| !info.connected)
                .min_by_key(|(_, info)| info.last_seen)
                .map(|(id, _)| id.clone());

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
                .map(|(id, info)| (id.clone(), info.known_cids.len(), info.connected))
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
                    info.debug_assert_cid_tracking_consistent();
                    // Evict oldest CIDs from this peer
                    while evicted < excess && !info.cid_order.is_empty() {
                        if let Some(cid) = info.cid_order.pop_front() {
                            info.known_cids.remove(&cid);
                            evicted += 1;
                        }
                    }
                    info.debug_assert_cid_tracking_consistent();
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
}
