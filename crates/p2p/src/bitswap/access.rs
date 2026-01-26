//! Access control primitives for P2P synchronization.
//!
//! This module provides the building blocks for access control:
//! - `ReplicatorRegistry`: Tracks which peers are authorized replicators for which collections
//! - `AccessMode`: Controls whether access control is enabled (Open vs Controlled)
//!
//! # Security Model
//!
//! Access control is enforced at the **SyncCoordinator level**, not at the Bitswap level.
//! The SyncCoordinator checks access on incoming PushLog and GossipSub messages before
//! blocks are stored. This means:
//!
//! 1. Unauthorized peers cannot push blocks to this node
//! 2. Bitswap inherently only serves blocks that passed the coordinator's access check
//! 3. Per-collection authorization is enforced (a replicator for collection A cannot
//!    access collection B)
//!
//! This follows the Go DefraDB security model where each replicator is authorized
//! per-collection.

use std::collections::{HashMap, HashSet};

use libp2p::PeerId;
use parking_lot::RwLock;

use crate::replicator::ReplicatorInfo;

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

/// Access control mode for P2P synchronization.
///
/// Controls whether access control is enforced at the SyncCoordinator level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessMode {
    /// No access control - all requests are allowed.
    /// This is the default mode when ACP is not configured.
    #[default]
    Open,

    /// Access control enabled - check replicator status.
    /// Only replicators for the specific collection have access.
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
    fn test_access_mode_helpers() {
        assert!(AccessMode::Open.is_open());
        assert!(!AccessMode::Open.is_controlled());
        assert!(AccessMode::Controlled.is_controlled());
        assert!(!AccessMode::Controlled.is_open());
        assert_eq!(AccessMode::default(), AccessMode::Open);
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

        let registry = std::sync::Arc::new(ReplicatorRegistry::new());
        let mut handles = vec![];

        // Spawn multiple threads modifying the registry concurrently
        for i in 0..10 {
            let registry_clone = std::sync::Arc::clone(&registry);
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
