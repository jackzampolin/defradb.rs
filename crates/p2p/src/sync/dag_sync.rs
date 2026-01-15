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
use std::num::NonZeroUsize;
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
    block_fetch_timeout: Duration,

    /// Maximum depth to recursively sync (None = unlimited).
    max_depth: Option<NonZeroUsize>,

    /// Maximum concurrent block fetches (guaranteed non-zero).
    max_concurrent_fetches: NonZeroUsize,
}

impl DagSyncConfig {
    /// Create a new DagSyncConfig with validation.
    ///
    /// # Arguments
    ///
    /// * `block_fetch_timeout` - Timeout for fetching blocks (must be > 0)
    /// * `max_depth` - Maximum sync depth (None = unlimited)
    /// * `max_concurrent_fetches` - Max concurrent fetches (guaranteed non-zero)
    ///
    /// # Panics
    ///
    /// Panics if `block_fetch_timeout` is zero.
    pub fn new(
        block_fetch_timeout: Duration,
        max_depth: Option<NonZeroUsize>,
        max_concurrent_fetches: NonZeroUsize,
    ) -> Self {
        assert!(
            !block_fetch_timeout.is_zero(),
            "block_fetch_timeout must be greater than zero"
        );
        Self {
            block_fetch_timeout,
            max_depth,
            max_concurrent_fetches,
        }
    }

    /// Get the block fetch timeout.
    pub fn block_fetch_timeout(&self) -> Duration {
        self.block_fetch_timeout
    }

    /// Get the maximum sync depth (None = unlimited).
    pub fn max_depth(&self) -> Option<NonZeroUsize> {
        self.max_depth
    }

    /// Get the maximum concurrent fetches.
    pub fn max_concurrent_fetches(&self) -> NonZeroUsize {
        self.max_concurrent_fetches
    }

    /// Builder method to set block fetch timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "timeout must be greater than zero");
        self.block_fetch_timeout = timeout;
        self
    }

    /// Builder method to set max depth.
    pub fn with_max_depth(mut self, depth: Option<NonZeroUsize>) -> Self {
        self.max_depth = depth;
        self
    }

    /// Builder method to set max concurrent fetches.
    pub fn with_max_concurrent_fetches(mut self, count: NonZeroUsize) -> Self {
        self.max_concurrent_fetches = count;
        self
    }
}

impl Default for DagSyncConfig {
    fn default() -> Self {
        Self {
            block_fetch_timeout: Duration::from_secs(30),
            max_depth: None, // Unlimited
            // SAFETY: 16 is non-zero
            max_concurrent_fetches: NonZeroUsize::new(16).unwrap(),
        }
    }
}

/// Internal state for DagSyncState, protected by a single lock.
#[derive(Default)]
struct SyncStateInner {
    /// CIDs currently being synced
    syncing: HashSet<Cid>,
    /// CIDs that have been synced in this session
    synced: HashSet<Cid>,
}

/// Tracks ongoing DAG sync operations.
///
/// This is used to:
/// - Prevent duplicate sync requests for the same CID
/// - Track which blocks are being fetched
/// - Cancel sync operations when needed
///
/// All state is protected by a single lock to prevent race conditions
/// between checking and modifying sync state.
#[derive(Default)]
pub struct DagSyncState {
    /// Combined state protected by a single lock
    state: RwLock<SyncStateInner>,
}

impl DagSyncState {
    /// Create a new sync state tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a CID is currently being synced.
    pub async fn is_syncing(&self, cid: &Cid) -> bool {
        self.state.read().await.syncing.contains(cid)
    }

    /// Check if a CID has already been synced.
    pub async fn is_synced(&self, cid: &Cid) -> bool {
        self.state.read().await.synced.contains(cid)
    }

    /// Mark a CID as currently syncing.
    ///
    /// Returns false if already syncing or synced.
    /// This operation is atomic - no race condition between check and insert.
    pub async fn start_sync(&self, cid: Cid) -> bool {
        let mut state = self.state.write().await;

        // Atomically check both conditions and insert
        if state.synced.contains(&cid) || state.syncing.contains(&cid) {
            return false;
        }

        state.syncing.insert(cid)
    }

    /// Mark a CID as successfully synced.
    pub async fn complete_sync(&self, cid: Cid) {
        let mut state = self.state.write().await;
        state.syncing.remove(&cid);
        state.synced.insert(cid);
    }

    /// Cancel a sync operation (e.g., on error).
    pub async fn cancel_sync(&self, cid: &Cid) {
        let mut state = self.state.write().await;
        state.syncing.remove(cid);
    }

    /// Get all CIDs currently being synced.
    pub async fn syncing_cids(&self) -> Vec<Cid> {
        self.state.read().await.syncing.iter().cloned().collect()
    }

    /// Clear all state (for testing or reset).
    pub async fn clear(&self) {
        let mut state = self.state.write().await;
        state.syncing.clear();
        state.synced.clear();
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
        // Atomically try to start sync - this prevents race conditions where
        // multiple concurrent calls could both proceed past separate checks.
        // start_sync returns false if already syncing or synced.
        if !self.state.start_sync(block_cid).await {
            // Already syncing or synced - determine which
            if self.state.is_synced(&block_cid).await {
                debug!("Already synced CID: {}", block_cid);
                return Ok(SyncPlan::AlreadySynced);
            }
            debug!("Already syncing CID: {}", block_cid);
            return Ok(SyncPlan::AlreadySyncing);
        }

        // We now have exclusive rights to sync this CID.
        // Find missing links.
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
    ///
    /// # Arguments
    ///
    /// * `root` - The root CID of the sync operation
    /// * `success` - Whether the sync completed successfully
    /// * `failure_reason` - Optional reason for failure (if success is false)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Sync completed successfully
    /// * `Err(Error::DagSyncFailed)` - Sync failed, callers should handle retry logic
    pub async fn handle_sync_complete(
        &self,
        root: Cid,
        success: bool,
        failure_reason: Option<&str>,
    ) -> Result<()> {
        if success {
            self.state.complete_sync(root).await;
            debug!("DAG sync completed for {}", root);
            Ok(())
        } else {
            self.state.cancel_sync(&root).await;
            let reason = failure_reason.unwrap_or("unknown failure").to_string();
            warn!(cid = %root, reason = %reason, "DAG sync failed");
            Err(crate::error::Error::DagSyncFailed {
                cid: root.to_string(),
                reason,
            })
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
#[non_exhaustive]
pub enum SyncPlan {
    /// Already syncing this block.
    AlreadySyncing,

    /// Already synced this block.
    AlreadySynced,

    /// All blocks are available locally, no fetch needed.
    Complete,

    /// Need to fetch missing blocks via Bitswap.
    ///
    /// Use `SyncPlan::needs_fetch_new()` to create this variant,
    /// which enforces the invariant that `missing` is non-empty.
    NeedsFetch {
        /// Root block CID that triggered the sync.
        root: Cid,
        /// CIDs that need to be fetched (guaranteed non-empty).
        missing: Vec<Cid>,
        /// Potential providers for the blocks.
        providers: Vec<PeerId>,
    },
}

impl SyncPlan {
    /// Create a NeedsFetch plan with validation.
    ///
    /// Returns `None` if `missing` is empty (use `SyncPlan::Complete` instead).
    pub fn needs_fetch_new(root: Cid, missing: Vec<Cid>, providers: Vec<PeerId>) -> Option<Self> {
        if missing.is_empty() {
            None
        } else {
            Some(Self::NeedsFetch {
                root,
                missing,
                providers,
            })
        }
    }

    /// Get the root CID if this is a NeedsFetch plan.
    pub fn root(&self) -> Option<Cid> {
        match self {
            SyncPlan::NeedsFetch { root, .. } => Some(*root),
            _ => None,
        }
    }
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

        // Success case - should return Ok
        let result = dag_sync.handle_sync_complete(root, true, None).await;
        assert!(result.is_ok());
        assert!(dag_sync.state.is_synced(&root).await);
        assert!(!dag_sync.state.is_syncing(&root).await);

        // Failure case - should return Err with reason
        let root2 = test_cid2();
        dag_sync.state.start_sync(root2).await;
        let result = dag_sync
            .handle_sync_complete(root2, false, Some("timeout"))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timeout"));
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

    #[tokio::test]
    async fn test_dag_sync_state_concurrent_operations() {
        // Test that concurrent start_sync operations are handled atomically
        let state = Arc::new(DagSyncState::new());
        let cid = test_cid();

        // Spawn multiple tasks trying to start sync for the same CID
        let mut handles = Vec::new();
        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(tokio::spawn(async move { state_clone.start_sync(cid).await }));
        }

        // Collect results
        let mut successes = 0;
        let mut failures = 0;
        for handle in handles {
            if handle.await.unwrap() {
                successes += 1;
            } else {
                failures += 1;
            }
        }

        // Exactly one task should have succeeded
        assert_eq!(
            successes, 1,
            "Exactly one task should acquire the sync lock"
        );
        assert_eq!(failures, 9, "Other tasks should fail to acquire");

        // CID should be syncing
        assert!(state.is_syncing(&cid).await);
        assert!(!state.is_synced(&cid).await);
    }

    #[tokio::test]
    async fn test_dag_sync_state_concurrent_complete() {
        let state = Arc::new(DagSyncState::new());
        let cid = test_cid();

        // Start sync
        assert!(state.start_sync(cid).await);

        // Spawn multiple tasks trying to complete sync
        let mut handles = Vec::new();
        for _ in 0..10 {
            let state_clone = Arc::clone(&state);
            handles.push(tokio::spawn(async move {
                state_clone.complete_sync(cid).await;
            }));
        }

        // Wait for all to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // CID should be synced (not syncing)
        assert!(!state.is_syncing(&cid).await);
        assert!(state.is_synced(&cid).await);

        // Can't start sync again
        assert!(!state.start_sync(cid).await);
    }

    #[test]
    #[should_panic(expected = "block_fetch_timeout must be greater than zero")]
    fn test_dag_sync_config_zero_timeout_panics() {
        // DagSyncConfig::new should panic if block_fetch_timeout is zero
        DagSyncConfig::new(
            Duration::ZERO,
            None,
            NonZeroUsize::new(16).unwrap(),
        );
    }

    #[test]
    #[should_panic(expected = "timeout must be greater than zero")]
    fn test_dag_sync_config_with_timeout_zero_panics() {
        // with_timeout builder method should also panic on zero
        DagSyncConfig::default().with_timeout(Duration::ZERO);
    }

    #[test]
    fn test_dag_sync_config_default_values() {
        let config = DagSyncConfig::default();

        // Verify default values
        assert_eq!(config.block_fetch_timeout(), Duration::from_secs(30));
        assert!(config.max_depth().is_none()); // Unlimited
        assert_eq!(config.max_concurrent_fetches().get(), 16);
    }

    #[test]
    fn test_dag_sync_config_builder() {
        let config = DagSyncConfig::default()
            .with_timeout(Duration::from_secs(60))
            .with_max_depth(Some(NonZeroUsize::new(10).unwrap()))
            .with_max_concurrent_fetches(NonZeroUsize::new(32).unwrap());

        assert_eq!(config.block_fetch_timeout(), Duration::from_secs(60));
        assert_eq!(config.max_depth(), Some(NonZeroUsize::new(10).unwrap()));
        assert_eq!(config.max_concurrent_fetches().get(), 32);
    }
}
