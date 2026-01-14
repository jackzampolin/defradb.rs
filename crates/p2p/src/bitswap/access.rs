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
}

/// Block access controller that combines replicator checks with ACP.
///
/// This is the main entry point for access control decisions.
pub struct BlockAccessController {
    /// Replicator registry for fast-path checks
    replicators: Arc<ReplicatorRegistry>,

    /// Whether ACP is enabled (if false, all access is allowed)
    acp_enabled: bool,
}

impl BlockAccessController {
    /// Create a new access controller.
    pub fn new(replicators: Arc<ReplicatorRegistry>) -> Self {
        Self {
            replicators,
            acp_enabled: false, // ACP integration pending
        }
    }

    /// Create a new access controller with ACP enabled.
    pub fn with_acp(replicators: Arc<ReplicatorRegistry>) -> Self {
        Self {
            replicators,
            acp_enabled: true,
        }
    }

    /// Check if a peer has access to a block.
    ///
    /// This mirrors Go's `hasAccess` function logic:
    /// 1. If ACP not enabled → allow
    /// 2. If peer is replicator for block's collection → allow
    /// 3. Check ACP permissions (not yet implemented)
    ///
    /// # Arguments
    /// * `peer_id` - The peer requesting access
    /// * `cid` - The block being requested
    /// * `collection_id` - The collection the block belongs to (if known)
    pub fn has_access(
        &self,
        peer_id: &PeerId,
        _cid: &Cid,
        collection_id: Option<&str>,
    ) -> bool {
        // Fast path: ACP not enabled
        if !self.acp_enabled {
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

        // TODO: Full ACP integration
        // This would involve:
        // 1. Getting the block from the blockstore
        // 2. Extracting the document ID from the block
        // 3. Looking up the collection's ACP policy
        // 4. Getting the peer's identity (via identity protocol)
        // 5. Checking document access permissions

        // Default: deny if ACP enabled and no explicit permission
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
    fn test_access_controller_acp_disabled() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let controller = BlockAccessController::new(registry);
        let peer = PeerId::random();
        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // With ACP disabled, all access should be allowed
        assert!(controller.has_access(&peer, &cid, None));
        assert!(controller.has_access(&peer, &cid, Some("users")));
    }

    #[test]
    fn test_access_controller_replicator_allowed() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let peer = PeerId::random();
        registry.add_replicator("users", peer);

        let controller = BlockAccessController::with_acp(registry);
        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // Replicator should have access to their collection
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

        let controller = BlockAccessController::with_acp(registry);
        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // Stranger should be denied when ACP is enabled
        assert!(!controller.has_access(&stranger, &cid, Some("users")));
        assert!(!controller.has_access(&stranger, &cid, None));
    }

    #[test]
    fn test_access_fn_creation() {
        let registry = Arc::new(ReplicatorRegistry::new());
        let peer = PeerId::random();
        registry.add_replicator("users", peer);

        let controller = Arc::new(BlockAccessController::with_acp(registry));
        let access_fn = controller.as_access_fn();

        let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();

        // Test the closure
        assert!(access_fn(&peer, &cid));
        assert!(!access_fn(&PeerId::random(), &cid));
    }
}
