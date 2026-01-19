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
//! # Access Control Flow
//!
//! Go's access control logic (from `internal/db/p2p/p2p.go:hasAccess`):
//! 1. If ACP not configured (Open mode) → allow all
//! 2. Check if peer is a replicator for the collection → allow (fast path)
//! 3. Look up peer's DID from PeerIdentityRegistry
//! 4. Check document-level ACP permissions via DocumentACP → allow/deny
//!
//! # Components
//!
//! - `ReplicatorRegistry`: Tracks which peers are authorized replicators
//! - `PeerIdentityRegistry`: Maps PeerId to DID for ACP lookups
//! - `BlockAccessController`: Main entry point for access decisions

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission};
use cid::Cid;
use identity::Did;
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

/// Registry that maps peer IDs to their decentralized identifiers (DIDs).
///
/// When a peer connects and provides an identity token, their DID is registered
/// here. This allows the BlockAccessController to look up a peer's identity
/// for document-level ACP checks.
#[derive(Debug, Default)]
pub struct PeerIdentityRegistry {
    /// Map of peer ID to their DID
    identities: RwLock<HashMap<PeerId, Did>>,
}

impl PeerIdentityRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            identities: RwLock::new(HashMap::new()),
        }
    }

    /// Register a peer's DID.
    ///
    /// Called when a peer provides a valid identity token.
    pub fn register(&self, peer_id: PeerId, did: Did) {
        self.identities.write().insert(peer_id, did);
    }

    /// Unregister a peer's DID.
    ///
    /// Called when a peer disconnects or their token expires.
    pub fn unregister(&self, peer_id: &PeerId) {
        self.identities.write().remove(peer_id);
    }

    /// Get the DID for a peer, if registered.
    pub fn get_did(&self, peer_id: &PeerId) -> Option<Did> {
        self.identities.read().get(peer_id).cloned()
    }

    /// Check if a peer has a registered identity.
    pub fn has_identity(&self, peer_id: &PeerId) -> bool {
        self.identities.read().contains_key(peer_id)
    }
}

/// Block access controller that combines replicator checks with ACP.
///
/// This is the main entry point for access control decisions.
pub struct BlockAccessController {
    /// Replicator registry for fast-path checks
    replicators: Arc<ReplicatorRegistry>,

    /// Peer identity registry for DID lookups
    peer_identities: Arc<PeerIdentityRegistry>,

    /// Optional document ACP for permission checks
    document_acp: Option<Arc<dyn DocumentACP>>,

    /// Access control mode
    mode: AccessMode,
}

impl BlockAccessController {
    /// Create a new access controller with the specified mode.
    pub fn new(replicators: Arc<ReplicatorRegistry>, mode: AccessMode) -> Self {
        Self {
            replicators,
            peer_identities: Arc::new(PeerIdentityRegistry::new()),
            document_acp: None,
            mode,
        }
    }

    /// Create a new access controller with full configuration.
    pub fn with_acp(
        replicators: Arc<ReplicatorRegistry>,
        peer_identities: Arc<PeerIdentityRegistry>,
        document_acp: Arc<dyn DocumentACP>,
        mode: AccessMode,
    ) -> Self {
        Self {
            replicators,
            peer_identities,
            document_acp: Some(document_acp),
            mode,
        }
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

    /// Get the peer identity registry.
    pub fn peer_identities(&self) -> &Arc<PeerIdentityRegistry> {
        &self.peer_identities
    }

    /// Check if a peer has access to a block (synchronous, fast path only).
    ///
    /// This mirrors Go's `hasAccess` function logic:
    /// 1. If mode is Open → allow
    /// 2. If peer is replicator for block's collection → allow
    /// 3. If no replicator match, deny access
    ///
    /// Note: This method does NOT perform ACP checks. Use `has_access_acp`
    /// for full access control including document-level permissions.
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

    /// Check if a peer has access to a document with full ACP checks.
    ///
    /// This is the full access control path that includes document-level
    /// permission checks:
    /// 1. If mode is Open → allow
    /// 2. If peer is replicator for the collection → allow (fast path)
    /// 3. Look up peer's DID from identity registry
    /// 4. Check document-level ACP permissions → allow/deny
    ///
    /// # Arguments
    /// * `peer_id` - The peer requesting access
    /// * `permission` - The permission being requested (Read/Update/Delete)
    /// * `policy_id` - The policy ID from the collection
    /// * `resource_name` - The resource name from the policy
    /// * `doc_id` - The document being accessed
    pub async fn has_access_acp(
        &self,
        peer_id: &PeerId,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> bool {
        // Fast path: Open mode allows all access
        if self.mode.is_open() {
            return true;
        }

        // Fast path: peer is a replicator for this collection
        if self.replicators.is_replicator(resource_name, peer_id) {
            return true;
        }

        // No ACP configured - fall back to replicator-only mode
        let acp = match &self.document_acp {
            Some(acp) => acp,
            None => return false, // Deny if no ACP and not a replicator
        };

        // Look up peer's DID
        let did = self.peer_identities.get_did(peer_id);

        // Check document-level ACP permissions
        // Fail-closed: deny access on any error to prevent security bypass
        acp.check_doc_access(did.as_ref(), permission, policy_id, resource_name, doc_id)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    resource_name = %resource_name,
                    permission = %permission,
                    error = %e,
                    "ACP check failed, denying access"
                );
                false
            })
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

    // PeerIdentityRegistry tests

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
    }

    #[test]
    fn test_peer_identity_registry_register_get() {
        let registry = PeerIdentityRegistry::new();
        let peer = PeerId::random();
        let did = test_did();

        assert!(!registry.has_identity(&peer));
        assert!(registry.get_did(&peer).is_none());

        registry.register(peer, did.clone());

        assert!(registry.has_identity(&peer));
        assert_eq!(registry.get_did(&peer), Some(did));
    }

    #[test]
    fn test_peer_identity_registry_unregister() {
        let registry = PeerIdentityRegistry::new();
        let peer = PeerId::random();
        let did = test_did();

        registry.register(peer, did);
        assert!(registry.has_identity(&peer));

        registry.unregister(&peer);
        assert!(!registry.has_identity(&peer));
        assert!(registry.get_did(&peer).is_none());
    }

    #[test]
    fn test_peer_identity_registry_multiple_peers() {
        let registry = PeerIdentityRegistry::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();
        let did1 = test_did();
        let did2 = test_did2();

        registry.register(peer1, did1.clone());
        registry.register(peer2, did2.clone());

        assert_eq!(registry.get_did(&peer1), Some(did1));
        assert_eq!(registry.get_did(&peer2), Some(did2));
    }

    #[test]
    fn test_peer_identity_registry_overwrite() {
        let registry = PeerIdentityRegistry::new();
        let peer = PeerId::random();
        let did1 = test_did();
        let did2 = test_did2();

        registry.register(peer, did1);
        registry.register(peer, did2.clone());

        // Second registration should overwrite
        assert_eq!(registry.get_did(&peer), Some(did2));
    }

    // ACP-aware access tests

    #[tokio::test]
    async fn test_access_controller_acp_open_mode_allows_all() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let acp = Arc::new(acp::LocalDocumentACP::new(Arc::new(
            acp::MemoryAcpStore::new(),
        )));

        let controller =
            BlockAccessController::with_acp(replicators, peer_identities, acp, AccessMode::Open);

        let peer = PeerId::random();
        assert!(
            controller
                .has_access_acp(&peer, DocumentPermission::Read, "policy1", "users", "doc1")
                .await
        );
    }

    #[tokio::test]
    async fn test_access_controller_acp_replicator_fast_path() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer = PeerId::random();
        replicators.add_replicator("users", peer);

        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let acp = Arc::new(acp::LocalDocumentACP::new(Arc::new(
            acp::MemoryAcpStore::new(),
        )));

        let controller = BlockAccessController::with_acp(
            replicators,
            peer_identities,
            acp,
            AccessMode::Controlled,
        );

        // Replicator should have access without ACP check
        assert!(
            controller
                .has_access_acp(&peer, DocumentPermission::Read, "policy1", "users", "doc1")
                .await
        );
    }

    #[tokio::test]
    async fn test_access_controller_acp_registered_doc_owner_allowed() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let store = Arc::new(acp::MemoryAcpStore::new());
        let acp = Arc::new(acp::LocalDocumentACP::new(store));

        let peer = PeerId::random();
        let did = test_did();

        // Register peer's identity
        peer_identities.register(peer, did.clone());

        // Register document with owner
        acp.register_doc_object(&did, "policy1", "users", "doc1")
            .await
            .unwrap();

        let controller = BlockAccessController::with_acp(
            replicators,
            peer_identities,
            acp,
            AccessMode::Controlled,
        );

        // Owner should have access
        assert!(
            controller
                .has_access_acp(&peer, DocumentPermission::Read, "policy1", "users", "doc1")
                .await
        );
    }

    #[tokio::test]
    async fn test_access_controller_acp_non_owner_denied() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let store = Arc::new(acp::MemoryAcpStore::new());
        let acp = Arc::new(acp::LocalDocumentACP::new(store));

        let owner_peer = PeerId::random();
        let stranger_peer = PeerId::random();
        let owner_did = test_did();
        let stranger_did = test_did2();

        // Register both peers' identities
        peer_identities.register(owner_peer, owner_did.clone());
        peer_identities.register(stranger_peer, stranger_did);

        // Register document with owner
        acp.register_doc_object(&owner_did, "policy1", "users", "doc1")
            .await
            .unwrap();

        let controller = BlockAccessController::with_acp(
            replicators,
            peer_identities,
            acp,
            AccessMode::Controlled,
        );

        // Stranger should be denied
        assert!(
            !controller
                .has_access_acp(
                    &stranger_peer,
                    DocumentPermission::Read,
                    "policy1",
                    "users",
                    "doc1"
                )
                .await
        );
    }

    #[tokio::test]
    async fn test_access_controller_acp_anonymous_peer_denied() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let store = Arc::new(acp::MemoryAcpStore::new());
        let acp = Arc::new(acp::LocalDocumentACP::new(store));

        let owner_did = test_did();

        // Register document with owner
        acp.register_doc_object(&owner_did, "policy1", "users", "doc1")
            .await
            .unwrap();

        let controller = BlockAccessController::with_acp(
            replicators,
            peer_identities,
            acp,
            AccessMode::Controlled,
        );

        // Peer without registered identity should be denied
        let anonymous_peer = PeerId::random();
        assert!(
            !controller
                .has_access_acp(
                    &anonymous_peer,
                    DocumentPermission::Read,
                    "policy1",
                    "users",
                    "doc1"
                )
                .await
        );
    }

    #[tokio::test]
    async fn test_access_controller_acp_unregistered_doc_allows_all() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let acp = Arc::new(acp::LocalDocumentACP::new(Arc::new(
            acp::MemoryAcpStore::new(),
        )));

        let controller = BlockAccessController::with_acp(
            replicators,
            peer_identities,
            acp,
            AccessMode::Controlled,
        );

        // Anonymous peer can access unregistered (public) document
        let peer = PeerId::random();
        assert!(
            controller
                .has_access_acp(
                    &peer,
                    DocumentPermission::Read,
                    "policy1",
                    "users",
                    "unregistered_doc"
                )
                .await
        );
    }

    // Fail-closed behavior tests

    /// Mock ACP that always returns an error for check_doc_access
    struct FailingAcp;

    #[async_trait::async_trait]
    impl acp::DocumentACP for FailingAcp {
        async fn register_doc_object(
            &self,
            _identity: &Did,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Err(acp::Error::Storage("simulated storage failure".to_string()))
        }

        async fn is_doc_registered(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("simulated storage failure".to_string()))
        }

        async fn check_doc_access(
            &self,
            _identity: Option<&Did>,
            _permission: DocumentPermission,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("simulated storage failure".to_string()))
        }

        async fn add_actor_relationship(
            &self,
            _requestor: &Did,
            _target_actor: &Did,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("simulated storage failure".to_string()))
        }

        async fn delete_actor_relationship(
            &self,
            _requestor: &Did,
            _target_actor: &Did,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
        ) -> acp::Result<bool> {
            Err(acp::Error::Storage("simulated storage failure".to_string()))
        }
    }

    #[tokio::test]
    async fn test_access_controller_acp_denies_on_error() {
        let replicators = Arc::new(ReplicatorRegistry::new());
        let peer_identities = Arc::new(PeerIdentityRegistry::new());
        let failing_acp = Arc::new(FailingAcp);

        let peer = PeerId::random();
        let did = test_did();
        peer_identities.register(peer, did);

        let controller = BlockAccessController::with_acp(
            replicators,
            peer_identities,
            failing_acp,
            AccessMode::Controlled,
        );

        // When ACP check fails with an error, access should be DENIED (fail-closed)
        assert!(
            !controller
                .has_access_acp(&peer, DocumentPermission::Read, "policy1", "users", "doc1")
                .await,
            "fail-closed: ACP error should result in access denied"
        );
    }
}
