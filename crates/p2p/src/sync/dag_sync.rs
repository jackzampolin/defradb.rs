// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! DAG synchronization using Bitswap.
//!
//! This module provides DAG sync capabilities that mirror Go DefraDB's `syncDAG`
//! pattern. When a block is received via PushLog, this module:
//! 1. Stores the block in the local blockstore
//! 2. Extracts all links from the block
//! 3. Fetches missing linked blocks via Bitswap
//! 4. Recursively syncs linked blocks
//!
//! # Go Implementation Reference
//!
//! Go's `syncDAG` in `internal/db/p2p/sync_dag.go`:
//! - Uses a LinkSystem for IPLD storage
//! - Recursively loads all linked blocks
//! - Concurrent fetching with context cancellation
//!
//! # Example
//!
//! ```ignore
//! use p2p::sync::DagSync;
//!
//! let dag_sync = DagSync::new(blockstore, peer_state);
//!
//! // When receiving a PushLog message with a block:
//! let missing = dag_sync.get_missing_links(&block).await?;
//! if !missing.is_empty() {
//!     // Start Bitswap sync for missing blocks
//!     let query_id = behaviour.bitswap_sync(block_cid, peers, missing.iter().cloned());
//! }
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use cid::Cid;
use libp2p::PeerId;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::error::Result;
use crate::sync::PeerStateTracker;

/// Configuration for DAG sync operations.
#[derive(Debug, Clone)]
pub struct DagSyncConfig {
    /// Timeout for fetching a single block via Bitswap.
    pub block_fetch_timeout: Duration,

    /// Maximum depth to recursively sync (0 = unlimited).
    pub max_depth: usize,

    /// Maximum concurrent block fetches.
    pub max_concurrent_fetches: usize,
}

impl Default for DagSyncConfig {
    fn default() -> Self {
        Self {
            block_fetch_timeout: Duration::from_secs(30),
            max_depth: 0, // Unlimited
            max_concurrent_fetches: 16,
        }
    }
}

/// Tracks ongoing DAG sync operations.
///
/// This is used to:
/// - Prevent duplicate sync requests for the same CID
/// - Track which blocks are being fetched
/// - Cancel sync operations when needed
#[derive(Default)]
pub struct DagSyncState {
    /// CIDs currently being synced
    syncing: RwLock<HashSet<Cid>>,
    /// CIDs that have been synced in this session
    synced: RwLock<HashSet<Cid>>,
}

impl DagSyncState {
    /// Create a new sync state tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a CID is currently being synced.
    pub async fn is_syncing(&self, cid: &Cid) -> bool {
        self.syncing.read().await.contains(cid)
    }

    /// Check if a CID has already been synced.
    pub async fn is_synced(&self, cid: &Cid) -> bool {
        self.synced.read().await.contains(cid)
    }

    /// Mark a CID as currently syncing.
    ///
    /// Returns false if already syncing or synced.
    pub async fn start_sync(&self, cid: Cid) -> bool {
        if self.is_synced(&cid).await {
            return false;
        }

        let mut syncing = self.syncing.write().await;
        syncing.insert(cid)
    }

    /// Mark a CID as successfully synced.
    pub async fn complete_sync(&self, cid: Cid) {
        let mut syncing = self.syncing.write().await;
        syncing.remove(&cid);
        drop(syncing);

        let mut synced = self.synced.write().await;
        synced.insert(cid);
    }

    /// Cancel a sync operation (e.g., on error).
    pub async fn cancel_sync(&self, cid: &Cid) {
        let mut syncing = self.syncing.write().await;
        syncing.remove(cid);
    }

    /// Get all CIDs currently being synced.
    pub async fn syncing_cids(&self) -> Vec<Cid> {
        self.syncing.read().await.iter().cloned().collect()
    }

    /// Clear all state (for testing or reset).
    pub async fn clear(&self) {
        self.syncing.write().await.clear();
        self.synced.write().await.clear();
    }
}

/// DAG synchronizer that coordinates Bitswap block fetching.
///
/// This doesn't directly perform Bitswap operations (those happen in the
/// behaviour), but provides the logic for determining what blocks need
/// to be fetched and tracking sync state.
pub struct DagSync {
    /// Configuration
    config: DagSyncConfig,

    /// Sync state tracker
    state: Arc<DagSyncState>,

    /// Peer state for selecting providers
    peer_state: Arc<PeerStateTracker>,
}

impl DagSync {
    /// Create a new DAG sync manager.
    pub fn new(peer_state: Arc<PeerStateTracker>) -> Self {
        Self {
            config: DagSyncConfig::default(),
            state: Arc::new(DagSyncState::new()),
            peer_state,
        }
    }

    /// Create with custom configuration.
    pub fn with_config(peer_state: Arc<PeerStateTracker>, config: DagSyncConfig) -> Self {
        Self {
            config,
            state: Arc::new(DagSyncState::new()),
            peer_state,
        }
    }

    /// Get the sync state tracker for external access.
    pub fn state(&self) -> Arc<DagSyncState> {
        Arc::clone(&self.state)
    }

    /// Get the configuration.
    pub fn config(&self) -> &DagSyncConfig {
        &self.config
    }

    /// Prepare a sync operation for a block and its links.
    ///
    /// Returns the CIDs that need to be fetched via Bitswap.
    /// Call this after receiving a block via PushLog.
    ///
    /// # Arguments
    /// * `block_cid` - CID of the received block
    /// * `block_links` - All links extracted from the block (use `Block::all_links()`)
    /// * `local_has` - Function to check if a CID exists locally
    pub async fn prepare_sync<F>(
        &self,
        block_cid: Cid,
        block_links: &[Cid],
        local_has: F,
    ) -> Result<SyncPlan>
    where
        F: Fn(&Cid) -> bool,
    {
        // Check if we're already syncing this block
        if self.state.is_syncing(&block_cid).await {
            debug!("Already syncing CID: {}", block_cid);
            return Ok(SyncPlan::AlreadySyncing);
        }

        // Check if already synced
        if self.state.is_synced(&block_cid).await {
            debug!("Already synced CID: {}", block_cid);
            return Ok(SyncPlan::AlreadySynced);
        }

        // Find missing links
        let mut missing: Vec<Cid> = Vec::new();
        for link in block_links {
            // Skip if we already have it locally
            if local_has(link) {
                continue;
            }

            // Skip if already synced or syncing
            if self.state.is_synced(link).await || self.state.is_syncing(link).await {
                continue;
            }

            missing.push(*link);
        }

        if missing.is_empty() {
            // Mark as synced since we have all blocks
            self.state.complete_sync(block_cid).await;
            return Ok(SyncPlan::Complete);
        }

        // Mark the root block as syncing
        self.state.start_sync(block_cid).await;

        // Mark all missing blocks as syncing
        for cid in &missing {
            self.state.start_sync(*cid).await;
        }

        // Get potential providers from peer state
        let providers = self.get_providers(&missing);

        Ok(SyncPlan::NeedsFetch {
            root: block_cid,
            missing,
            providers,
        })
    }

    /// Get potential providers for a set of CIDs.
    ///
    /// Returns peers that might have the blocks, based on peer state tracking.
    fn get_providers(&self, cids: &[Cid]) -> Vec<PeerId> {
        let mut providers = HashSet::new();

        // Add peers known to have any of the missing CIDs
        for cid in cids {
            for peer in self.peer_state.peers_with_cid(cid) {
                providers.insert(peer);
            }
        }

        // If no specific providers found, use all connected peers
        if providers.is_empty() {
            for peer in self.peer_state.connected_peers() {
                providers.insert(peer);
            }
        }

        providers.into_iter().collect()
    }

    /// Handle completion of a Bitswap sync query.
    ///
    /// Call this when Bitswap reports a query completed (success or failure).
    pub async fn handle_sync_complete(&self, root: Cid, success: bool) {
        if success {
            self.state.complete_sync(root).await;
            debug!("DAG sync completed for {}", root);
        } else {
            self.state.cancel_sync(&root).await;
            warn!("DAG sync failed for {}", root);
        }
    }

    /// Handle a block being received via Bitswap.
    ///
    /// Call this when Bitswap stores a block. Returns the block's links
    /// so the caller can check if more blocks need fetching.
    pub async fn handle_block_received(&self, cid: Cid) {
        // Mark as synced
        self.state.complete_sync(cid).await;
        debug!("Block received via Bitswap: {}", cid);
    }
}

/// Result of preparing a sync operation.
#[derive(Debug)]
pub enum SyncPlan {
    /// Already syncing this block.
    AlreadySyncing,

    /// Already synced this block.
    AlreadySynced,

    /// All blocks are available locally, no fetch needed.
    Complete,

    /// Need to fetch missing blocks via Bitswap.
    NeedsFetch {
        /// Root block CID that triggered the sync.
        root: Cid,
        /// CIDs that need to be fetched.
        missing: Vec<Cid>,
        /// Potential providers for the blocks.
        providers: Vec<PeerId>,
    },
}

impl SyncPlan {
    /// Check if a fetch is needed.
    pub fn needs_fetch(&self) -> bool {
        matches!(self, SyncPlan::NeedsFetch { .. })
    }

    /// Get the missing CIDs if a fetch is needed.
    pub fn missing(&self) -> Option<&[Cid]> {
        match self {
            SyncPlan::NeedsFetch { missing, .. } => Some(missing),
            _ => None,
        }
    }

    /// Get the providers if a fetch is needed.
    pub fn providers(&self) -> Option<&[PeerId]> {
        match self {
            SyncPlan::NeedsFetch { providers, .. } => Some(providers),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    fn test_cid2() -> Cid {
        Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
    }

    fn test_cid3() -> Cid {
        Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap()
    }

    #[tokio::test]
    async fn test_sync_state_lifecycle() {
        let state = DagSyncState::new();
        let cid = test_cid();

        // Initially not syncing or synced
        assert!(!state.is_syncing(&cid).await);
        assert!(!state.is_synced(&cid).await);

        // Start sync
        assert!(state.start_sync(cid).await);
        assert!(state.is_syncing(&cid).await);
        assert!(!state.is_synced(&cid).await);

        // Can't start again while syncing
        assert!(!state.start_sync(cid).await);

        // Complete sync
        state.complete_sync(cid).await;
        assert!(!state.is_syncing(&cid).await);
        assert!(state.is_synced(&cid).await);

        // Can't start after synced
        assert!(!state.start_sync(cid).await);
    }

    #[tokio::test]
    async fn test_sync_state_cancel() {
        let state = DagSyncState::new();
        let cid = test_cid();

        state.start_sync(cid).await;
        assert!(state.is_syncing(&cid).await);

        state.cancel_sync(&cid).await;
        assert!(!state.is_syncing(&cid).await);
        assert!(!state.is_synced(&cid).await);

        // Can start again after cancel
        assert!(state.start_sync(cid).await);
    }

    #[tokio::test]
    async fn test_dag_sync_no_missing_links() {
        let peer_state = Arc::new(PeerStateTracker::new());
        let dag_sync = DagSync::new(peer_state);

        let root = test_cid();
        let links = vec![test_cid2(), test_cid3()];

        // All links exist locally
        let local_has = |_: &Cid| true;

        let plan = dag_sync.prepare_sync(root, &links, local_has).await.unwrap();

        assert!(matches!(plan, SyncPlan::Complete));
        assert!(dag_sync.state.is_synced(&root).await);
    }

    #[tokio::test]
    async fn test_dag_sync_with_missing_links() {
        let peer_state = Arc::new(PeerStateTracker::new());
        let peer = PeerId::random();
        peer_state.peer_connected(peer);

        let dag_sync = DagSync::new(peer_state);

        let root = test_cid();
        let links = vec![test_cid2(), test_cid3()];

        // Only first link exists locally
        let cid2 = test_cid2();
        let local_has = move |cid: &Cid| *cid == cid2;

        let plan = dag_sync.prepare_sync(root, &links, local_has).await.unwrap();

        match plan {
            SyncPlan::NeedsFetch {
                root: r,
                missing,
                providers,
            } => {
                assert_eq!(r, root);
                assert_eq!(missing.len(), 1);
                assert_eq!(missing[0], test_cid3());
                assert!(!providers.is_empty());
            }
            _ => panic!("Expected NeedsFetch"),
        }

        // Root should be marked as syncing
        assert!(dag_sync.state.is_syncing(&root).await);
    }

    #[tokio::test]
    async fn test_dag_sync_already_syncing() {
        let peer_state = Arc::new(PeerStateTracker::new());
        let dag_sync = DagSync::new(peer_state);

        let root = test_cid();
        let links = vec![];

        // Start first sync
        dag_sync.state.start_sync(root).await;

        // Try to sync again
        let plan = dag_sync
            .prepare_sync(root, &links, |_| false)
            .await
            .unwrap();

        assert!(matches!(plan, SyncPlan::AlreadySyncing));
    }

    #[tokio::test]
    async fn test_dag_sync_already_synced() {
        let peer_state = Arc::new(PeerStateTracker::new());
        let dag_sync = DagSync::new(peer_state);

        let root = test_cid();
        let links = vec![];

        // Mark as synced
        dag_sync.state.complete_sync(root).await;

        // Try to sync again
        let plan = dag_sync
            .prepare_sync(root, &links, |_| false)
            .await
            .unwrap();

        assert!(matches!(plan, SyncPlan::AlreadySynced));
    }

    #[tokio::test]
    async fn test_dag_sync_handle_complete() {
        let peer_state = Arc::new(PeerStateTracker::new());
        let dag_sync = DagSync::new(peer_state);

        let root = test_cid();
        dag_sync.state.start_sync(root).await;

        // Success case
        dag_sync.handle_sync_complete(root, true).await;
        assert!(dag_sync.state.is_synced(&root).await);
        assert!(!dag_sync.state.is_syncing(&root).await);

        // Failure case
        let root2 = test_cid2();
        dag_sync.state.start_sync(root2).await;
        dag_sync.handle_sync_complete(root2, false).await;
        assert!(!dag_sync.state.is_synced(&root2).await);
        assert!(!dag_sync.state.is_syncing(&root2).await);
    }

    #[tokio::test]
    async fn test_sync_plan_accessors() {
        let plan = SyncPlan::NeedsFetch {
            root: test_cid(),
            missing: vec![test_cid2()],
            providers: vec![PeerId::random()],
        };

        assert!(plan.needs_fetch());
        assert_eq!(plan.missing().unwrap().len(), 1);
        assert_eq!(plan.providers().unwrap().len(), 1);

        let plan2 = SyncPlan::Complete;
        assert!(!plan2.needs_fetch());
        assert!(plan2.missing().is_none());
        assert!(plan2.providers().is_none());
    }

    #[tokio::test]
    async fn test_dag_sync_uses_peer_state_for_providers() {
        let peer_state = Arc::new(PeerStateTracker::new());
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        peer_state.peer_connected(peer1);
        peer_state.peer_connected(peer2);

        // peer1 has cid2
        let cid2 = test_cid2();
        peer_state.peer_has_cid(&peer1, cid2);

        let dag_sync = DagSync::new(peer_state);

        let root = test_cid();
        let links = vec![cid2];

        let plan = dag_sync
            .prepare_sync(root, &links, |_| false)
            .await
            .unwrap();

        match plan {
            SyncPlan::NeedsFetch { providers, .. } => {
                // Should prefer peer1 since it has the CID
                assert!(providers.contains(&peer1));
            }
            _ => panic!("Expected NeedsFetch"),
        }
    }
}
