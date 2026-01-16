// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Access control for block exchange (Bitswap).
//!
//! This module provides access control mechanisms that mirror Go DefraDB's
//! `hasAccess` callback pattern. In Go, the callback is registered with
//! `host.SetBlockAccessFunc(p.hasAccess)` and receives (ctx, peerID, cid).
//!
//! # Current Limitations
//!
//! The Rust libp2p-bitswap-next crate doesn't support per-request access control
//! callbacks. The `BitswapStore::get` method doesn't receive peer information.
//!
//! For now, access control is implemented at the **sync coordinator level**,
//! where we have peer context. This provides:
//! - Replicator-based access (fast path)
//! - Collection subscription validation
//!
//! Full ACP integration will require modifications to the bitswap layer.
//!
//! # Go DefraDB Pattern
//!
//! Go's access control logic (from `internal/db/p2p/p2p.go:hasAccess`):
//! 1. If ACP not configured → allow all
//! 2. Check if peer is a replicator for the collection → allow
//! 3. Get peer's identity token and verify it
//! 4. Check document-level ACP permissions → allow/deny

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cid::Cid;
use libp2p::PeerId;
use parking_lot::RwLock;

use crate::replicator::ReplicatorInfo;

/// Type alias for block access check functions.
///
/// Mirrors Go's `BlockAccessFunc = func(ctx context.Context, peerID string, c cid.Cid) bool`.
pub type BlockAccessFn = Arc<dyn Fn(&PeerId, &Cid) -> bool + Send + Sync>;

/// Tracks which peers are authorized replicators for which collections.
///
/// This is the fast-path access check used by Go DefraDB. Replicators
/// automatically have access to all blocks in their subscribed collections.
#[derive(Debug, Default)]
pub struct ReplicatorRegistry {
    /// Map of collection_id -> set of authorized peer IDs
    replicators: RwLock<HashMap<String, HashSet<PeerId>>>,
}

impl ReplicatorRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            replicators: RwLock::new(HashMap::new()),
        }
    }

    /// Register a peer as a replicator for a collection.
    pub fn add_replicator(&self, collection_id: &str, peer_id: PeerId) {
        let mut replicators = self.replicators.write();
        replicators
            .entry(collection_id.to_string())
            .or_default()
            .insert(peer_id);
    }

    /// Remove a peer as a replicator for a collection.
    pub fn remove_replicator(&self, collection_id: &str, peer_id: &PeerId) {
        let mut replicators = self.replicators.write();
        if let Some(peers) = replicators.get_mut(collection_id) {
            peers.remove(peer_id);
            if peers.is_empty() {
                replicators.remove(collection_id);
            }
        }
    }

    /// Remove a peer from all collections.
    pub fn remove_peer(&self, peer_id: &PeerId) {
        let mut replicators = self.replicators.write();
        for peers in replicators.values_mut() {
            peers.remove(peer_id);
        }
        replicators.retain(|_, peers| !peers.is_empty());
    }

    /// Check if a peer is a replicator for a collection.
    pub fn is_replicator(&self, collection_id: &str, peer_id: &PeerId) -> bool {
        let replicators = self.replicators.read();
        replicators
            .get(collection_id)
            .map(|peers| peers.contains(peer_id))
            .unwrap_or(false)
    }

    /// Check if a peer is a replicator for any collection.
    pub fn is_any_replicator(&self, peer_id: &PeerId) -> bool {
        let replicators = self.replicators.read();
        replicators.values().any(|peers| peers.contains(peer_id))
    }

    /// Get all replicators for a collection.
    pub fn get_replicators(&self, collection_id: &str) -> Vec<PeerId> {
        let replicators = self.replicators.read();
        replicators
            .get(collection_id)
            .map(|peers| peers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get all collections a peer is replicating.
    pub fn get_collections(&self, peer_id: &PeerId) -> Vec<String> {
        let replicators = self.replicators.read();
        replicators
            .iter()
            .filter(|(_, peers)| peers.contains(peer_id))
            .map(|(col_id, _)| col_id.clone())
            .collect()
    }

    /// Get all registered replicators as ReplicatorInfo.
    ///
    /// This is used for persistence - exporting current state to storage.
    pub fn get_all_replicator_info(&self) -> Vec<ReplicatorInfo> {
        let replicators = self.replicators.read();

        // Build a map of peer_id -> collections
        let mut peer_collections: HashMap<PeerId, Vec<String>> = HashMap::new();

        for (collection_id, peers) in replicators.iter() {
            for peer in peers {
                peer_collections
                    .entry(*peer)
                    .or_default()
                    .push(collection_id.clone());
            }
        }

        // Convert to Vec<ReplicatorInfo>
        peer_collections
            .into_iter()
            .map(|(peer_id, collections)| ReplicatorInfo::new(peer_id, collections))
            .collect()
    }

    /// Load replicators from ReplicatorInfo records.
    ///
    /// This is used for persistence - loading state from storage on startup.
    /// Existing state is cleared before loading.
    ///
    /// Returns a tuple of (loaded_count, skipped_count) where skipped_count
    /// is the number of entries with invalid peer IDs.
    pub fn load_from_infos(&self, infos: &[ReplicatorInfo]) -> (usize, usize) {
        let mut replicators = self.replicators.write();
        replicators.clear();

        let mut loaded = 0;
        let mut skipped = 0;

        for info in infos {
            if let Some(peer_id) = info.peer_id() {
                for collection_id in &info.collections {
                    replicators
                        .entry(collection_id.clone())
                        .or_default()
                        .insert(peer_id);
                }
                loaded += 1;
            } else {
                tracing::warn!(
                    peer_id_str = %info.peer_id_str(),
                    collections = ?info.collections,
                    "Skipping replicator with invalid peer ID during load"
                );
                skipped += 1;
            }
        }

        (loaded, skipped)
    }

    /// Get replicator info for a specific peer.
    ///
    /// Returns None if the peer is not a replicator.
    pub fn get_replicator_info(&self, peer_id: &PeerId) -> Option<ReplicatorInfo> {
        let collections = self.get_collections(peer_id);
        if collections.is_empty() {
            None
        } else {
            Some(ReplicatorInfo::new(*peer_id, collections))
        }
    }

    /// Get all unique peer IDs that are replicators.
    pub fn get_all_peer_ids(&self) -> Vec<PeerId> {
        let replicators = self.replicators.read();
        let mut peers: HashSet<PeerId> = HashSet::new();

        for peer_set in replicators.values() {
            peers.extend(peer_set.iter().copied());
        }

        peers.into_iter().collect()
    }
}

/// Access control mode for block exchange.
///
/// Replaces boolean `acp_enabled` flag for better type clarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessMode {
    /// No access control - all requests are allowed.
    /// This is the default mode when ACP is not configured.
    #[default]
    Open,

    /// Access control enabled - check replicator status.
    /// Only replicators for the collection have access.
    Controlled,
}

impl AccessMode {
    /// Returns true if access control is enabled.
    pub fn is_controlled(&self) -> bool {
        matches!(self, AccessMode::Controlled)
    }

    /// Returns true if access is open (no control).
    pub fn is_open(&self) -> bool {
        matches!(self, AccessMode::Open)
    }
}

/// Block access controller that combines replicator checks with ACP.
///
/// This is the main entry point for access control decisions.
pub struct BlockAccessController {
    /// Replicator registry for fast-path checks
    replicators: Arc<ReplicatorRegistry>,

    /// Access control mode
    mode: AccessMode,
}

impl BlockAccessController {
    /// Create a new access controller with the specified mode.
    pub fn new(replicators: Arc<ReplicatorRegistry>, mode: AccessMode) -> Self {
        Self { replicators, mode }
    }

    /// Create a new access controller with open access (no ACP).
    pub fn open(replicators: Arc<ReplicatorRegistry>) -> Self {
        Self::new(replicators, AccessMode::Open)
    }

    /// Create a new access controller with controlled access (ACP enabled).
    pub fn controlled(replicators: Arc<ReplicatorRegistry>) -> Self {
        Self::new(replicators, AccessMode::Controlled)
    }

    /// Get the current access mode.
    pub fn mode(&self) -> AccessMode {
        self.mode
    }

    /// Check if a peer has access to a block.
    ///
    /// This mirrors Go's `hasAccess` function logic:
    /// 1. If mode is Open → allow
    /// 2. If peer is replicator for block's collection → allow
    /// 3. If no replicator match, deny access
    ///
    /// # Arguments
    /// * `peer_id` - The peer requesting access
    /// * `cid` - The block being requested
    /// * `collection_id` - The collection the block belongs to (if known)
    pub fn has_access(&self, peer_id: &PeerId, _cid: &Cid, collection_id: Option<&str>) -> bool {
        // Fast path: Open mode allows all access
        if self.mode.is_open() {
            return true;
        }

        // Fast path: peer is a replicator for this collection
        if let Some(col_id) = collection_id {
            if self.replicators.is_replicator(col_id, peer_id) {
                return true;
            }
        }

        // Fast path: peer is a replicator for any collection
        // (more permissive, but useful when collection is unknown)
        if self.replicators.is_any_replicator(peer_id) {
            return true;
        }

        // Default: deny in Controlled mode when no replicator match
        false
    }

    /// Create a closure that can be used as a BlockAccessFn.
    ///
    /// Note: This version doesn't have collection context, so it
    /// uses the more permissive `is_any_replicator` check.
    pub fn as_access_fn(self: Arc<Self>) -> BlockAccessFn {
        Arc::new(move |peer_id, cid| self.has_access(peer_id, cid, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicator_registry_add_remove() {
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        registry.add_replicator("users", peer);
        assert!(registry.is_replicator("users", &peer));
        assert!(!registry.is_replicator("posts", &peer));

        registry.remove_replicator("users", &peer);
        assert!(!registry.is_replicator("users", &peer));
    }

    #[test]
    fn test_replicator_registry_multiple_collections() {
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        registry.add_replicator("users", peer);
        registry.add_replicator("posts", peer);

        assert!(registry.is_replicator("users", &peer));
        assert!(registry.is_replicator("posts", &peer));
        assert!(registry.is_any_replicator(&peer));

        let collections = registry.get_collections(&peer);
        assert_eq!(collections.len(), 2);
    }

    #[test]
    fn test_replicator_registry_remove_peer() {
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        registry.add_replicator("users", peer);
        registry.add_replicator("posts", peer);

        registry.remove_peer(&peer);

        assert!(!registry.is_replicator("users", &peer));
        assert!(!registry.is_replicator("posts", &peer));
        assert!(!registry.is_any_replicator(&peer));
    }

    #[test]
    fn test_access_controller_open_mode() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let controller = BlockAccessController::open(registry);
        let peer = PeerId::random();
        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // With Open mode, all access should be allowed
        assert!(controller.mode().is_open());
        assert!(controller.has_access(&peer, &cid, None));
        assert!(controller.has_access(&peer, &cid, Some("users")));
    }

    #[test]
    fn test_access_controller_replicator_allowed() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let peer = PeerId::random();
        registry.add_replicator("users", peer);

        let controller = BlockAccessController::controlled(registry);
        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // Replicator should have access to their collection
        assert!(controller.mode().is_controlled());
        assert!(controller.has_access(&peer, &cid, Some("users")));

        // And to any block when collection is unknown (is_any_replicator)
        assert!(controller.has_access(&peer, &cid, None));
    }

    #[test]
    fn test_access_controller_non_replicator_denied() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let replicator = PeerId::random();
        let stranger = PeerId::random();
        registry.add_replicator("users", replicator);

        let controller = BlockAccessController::controlled(registry);
        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // Stranger should be denied in Controlled mode
        assert!(!controller.has_access(&stranger, &cid, Some("users")));
        assert!(!controller.has_access(&stranger, &cid, None));
    }

    #[test]
    fn test_access_fn_creation() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let peer = PeerId::random();
        registry.add_replicator("users", peer);

        let controller = Arc::new(BlockAccessController::controlled(registry));
        let access_fn = controller.as_access_fn();

        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // Test the closure
        assert!(access_fn(&peer, &cid));
        assert!(!access_fn(&PeerId::random(), &cid));
    }

    #[test]
    fn test_access_mode_helpers() {
        assert!(AccessMode::Open.is_open());
        assert!(!AccessMode::Open.is_controlled());
        assert!(AccessMode::Controlled.is_controlled());
        assert!(!AccessMode::Controlled.is_open());
        assert_eq!(AccessMode::default(), AccessMode::Open);
    }

    #[test]
    fn test_access_controller_new_with_mode() {
        let registry = Arc::new(ReplicatorRegistry::new());

        let open = BlockAccessController::new(Arc::clone(&registry), AccessMode::Open);
        assert!(open.mode().is_open());

        let controlled = BlockAccessController::new(registry, AccessMode::Controlled);
        assert!(controlled.mode().is_controlled());
    }

    #[test]
    fn test_replicator_registry_add_same_peer_twice() {
        // Test idempotency - adding the same peer twice should work
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        registry.add_replicator("users", peer);
        registry.add_replicator("users", peer); // Add again

        // Should still only have one entry
        let replicators = registry.get_replicators("users");
        assert_eq!(replicators.len(), 1);
        assert!(replicators.contains(&peer));
    }

    #[test]
    fn test_replicator_registry_remove_nonexistent() {
        // Removing a non-existent replicator should not panic
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        // Remove from non-existent collection
        registry.remove_replicator("nonexistent", &peer);

        // Remove non-existent peer from existing collection
        let other_peer = PeerId::random();
        registry.add_replicator("users", other_peer);
        registry.remove_replicator("users", &peer); // peer was never added

        // other_peer should still be there
        assert!(registry.is_replicator("users", &other_peer));
    }

    #[test]
    fn test_replicator_registry_concurrent_modifications() {
        use std::thread;

        let registry = Arc::new(ReplicatorRegistry::new());
        let mut handles = vec![];

        // Spawn multiple threads modifying the registry concurrently
        for i in 0..10 {
            let registry_clone = Arc::clone(&registry);
            let handle = thread::spawn(move || {
                let peer = PeerId::random();
                let collection = format!("collection_{}", i % 3);

                // Add and remove operations
                registry_clone.add_replicator(&collection, peer);
                assert!(registry_clone.is_any_replicator(&peer));

                // Sometimes remove
                if i % 2 == 0 {
                    registry_clone.remove_replicator(&collection, &peer);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Registry should be in a consistent state (no panic, no corruption)
        // We can't assert specific state due to non-deterministic interleaving
        let _ = registry.get_replicators("collection_0");
        let _ = registry.get_replicators("collection_1");
        let _ = registry.get_replicators("collection_2");
    }

    #[test]
    fn test_replicator_registry_get_all_replicator_info() {
        let registry = ReplicatorRegistry::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        registry.add_replicator("users", peer1);
        registry.add_replicator("posts", peer1);
        registry.add_replicator("users", peer2);

        let infos = registry.get_all_replicator_info();
        assert_eq!(infos.len(), 2);

        // Find peer1's info
        let peer1_info = infos.iter().find(|i| i.peer_id() == Some(peer1)).unwrap();
        assert_eq!(peer1_info.collections.len(), 2);
        assert!(peer1_info.collections.contains(&"users".to_string()));
        assert!(peer1_info.collections.contains(&"posts".to_string()));

        // Find peer2's info
        let peer2_info = infos.iter().find(|i| i.peer_id() == Some(peer2)).unwrap();
        assert_eq!(peer2_info.collections.len(), 1);
        assert!(peer2_info.collections.contains(&"users".to_string()));
    }

    #[test]
    fn test_replicator_registry_load_from_infos() {
        let registry = ReplicatorRegistry::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        // Create ReplicatorInfo records
        let infos = vec![
            ReplicatorInfo::new(peer1, vec!["users".to_string(), "posts".to_string()]),
            ReplicatorInfo::new(peer2, vec!["comments".to_string()]),
        ];

        // Load from infos
        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 2);
        assert_eq!(skipped, 0);

        // Verify loaded state
        assert!(registry.is_replicator("users", &peer1));
        assert!(registry.is_replicator("posts", &peer1));
        assert!(!registry.is_replicator("comments", &peer1));

        assert!(registry.is_replicator("comments", &peer2));
        assert!(!registry.is_replicator("users", &peer2));
    }

    #[test]
    fn test_replicator_registry_load_clears_existing() {
        let registry = ReplicatorRegistry::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        // Add initial data
        registry.add_replicator("users", peer1);
        assert!(registry.is_replicator("users", &peer1));

        // Load new data (should clear existing)
        let infos = vec![ReplicatorInfo::new(peer2, vec!["comments".to_string()])];
        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 1);
        assert_eq!(skipped, 0);

        // Old data should be gone
        assert!(!registry.is_replicator("users", &peer1));
        assert!(!registry.is_any_replicator(&peer1));

        // New data should be present
        assert!(registry.is_replicator("comments", &peer2));
    }

    #[test]
    fn test_replicator_registry_get_replicator_info() {
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        // No replicator info initially
        assert!(registry.get_replicator_info(&peer).is_none());

        // Add peer to collections
        registry.add_replicator("users", peer);
        registry.add_replicator("posts", peer);

        // Get replicator info
        let info = registry.get_replicator_info(&peer).unwrap();
        assert_eq!(info.peer_id(), Some(peer));
        assert_eq!(info.collections.len(), 2);
        assert!(info.collections.contains(&"users".to_string()));
        assert!(info.collections.contains(&"posts".to_string()));
    }

    #[test]
    fn test_replicator_registry_get_all_peer_ids() {
        let registry = ReplicatorRegistry::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();
        let peer3 = PeerId::random();

        registry.add_replicator("users", peer1);
        registry.add_replicator("users", peer2);
        registry.add_replicator("posts", peer2);
        registry.add_replicator("comments", peer3);

        let peer_ids = registry.get_all_peer_ids();
        assert_eq!(peer_ids.len(), 3);
        assert!(peer_ids.contains(&peer1));
        assert!(peer_ids.contains(&peer2));
        assert!(peer_ids.contains(&peer3));
    }

    #[test]
    fn test_replicator_registry_load_skips_invalid_peer_ids() {
        let registry = ReplicatorRegistry::new();
        let valid_peer = PeerId::random();

        // Create a mix of valid and invalid ReplicatorInfo records
        let infos = vec![
            ReplicatorInfo::new(valid_peer, vec!["users".to_string()]),
            ReplicatorInfo::from_raw(
                "invalid-peer-id".to_string(),
                vec!["posts".to_string()],
                vec![],
            ),
        ];

        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 1);
        assert_eq!(skipped, 1);

        // Only valid peer should be loaded
        assert!(registry.is_replicator("users", &valid_peer));
        assert_eq!(registry.get_all_peer_ids().len(), 1);
    }

    #[test]
    fn test_replicator_registry_load_empty_collections() {
        let registry = ReplicatorRegistry::new();
        let peer = PeerId::random();

        // Peer with empty collections
        let infos = vec![ReplicatorInfo::new(peer, vec![])];

        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 1); // Still counted as loaded
        assert_eq!(skipped, 0);

        // Peer is not a replicator for any collection
        assert!(!registry.is_any_replicator(&peer));
        assert!(registry.get_all_peer_ids().is_empty());
    }

    #[test]
    fn test_replicator_registry_roundtrip() {
        // Test that export -> load preserves state
        let registry1 = ReplicatorRegistry::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        registry1.add_replicator("users", peer1);
        registry1.add_replicator("posts", peer1);
        registry1.add_replicator("users", peer2);
        registry1.add_replicator("comments", peer2);

        // Export
        let infos = registry1.get_all_replicator_info();

        // Load into new registry
        let registry2 = ReplicatorRegistry::new();
        let (loaded, skipped) = registry2.load_from_infos(&infos);
        assert_eq!(loaded, 2);
        assert_eq!(skipped, 0);

        // Verify same state
        assert_eq!(
            registry1.is_replicator("users", &peer1),
            registry2.is_replicator("users", &peer1)
        );
        assert_eq!(
            registry1.is_replicator("posts", &peer1),
            registry2.is_replicator("posts", &peer1)
        );
        assert_eq!(
            registry1.is_replicator("users", &peer2),
            registry2.is_replicator("users", &peer2)
        );
        assert_eq!(
            registry1.is_replicator("comments", &peer2),
            registry2.is_replicator("comments", &peer2)
        );
    }
}
