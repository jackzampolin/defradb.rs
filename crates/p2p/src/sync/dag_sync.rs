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

use std::collections::{HashSet, VecDeque};
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
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if `block_fetch_timeout` is zero.
    pub fn new(
        block_fetch_timeout: Duration,
        max_depth: Option<NonZeroUsize>,
        max_concurrent_fetches: NonZeroUsize,
    ) -> Result<Self> {
        if block_fetch_timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfig(
                "block_fetch_timeout must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            block_fetch_timeout,
            max_depth,
            max_concurrent_fetches,
        })
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
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if `timeout` is zero.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfig(
                "timeout must be greater than zero".to_string(),
            ));
        }
        self.block_fetch_timeout = timeout;
        Ok(self)
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

/// Default maximum number of synced CIDs to track before eviction.
const DEFAULT_MAX_SYNCED_CIDS: usize = 100_000;

/// Internal state for DagSyncState, protected by a single lock.
struct SyncStateInner {
    /// CIDs currently being synced
    syncing: HashSet<Cid>,
    /// CIDs that have been synced in this session
    synced: HashSet<Cid>,
    /// Order of synced CIDs for FIFO eviction (oldest first)
    synced_order: VecDeque<Cid>,
    /// Maximum number of synced CIDs before eviction
    max_synced: usize,
}

impl Default for SyncStateInner {
    fn default() -> Self {
        Self {
            syncing: HashSet::new(),
            synced: HashSet::new(),
            synced_order: VecDeque::new(),
            max_synced: DEFAULT_MAX_SYNCED_CIDS,
        }
    }
}

/// Tracks ongoing DAG sync operations.
///
/// This is used to:
/// - Prevent duplicate sync requests for the same CID
/// - Track which blocks are being fetched
/// - Cancel sync operations when needed
///
/// The synced set has a configurable maximum size. When the limit is reached,
/// the oldest synced CIDs are evicted to make room for new ones. This prevents
/// unbounded memory growth in long-running nodes.
///
/// All state is protected by a single lock to prevent race conditions
/// between checking and modifying sync state.
pub struct DagSyncState {
    /// Combined state protected by a single lock
    state: RwLock<SyncStateInner>,
}

impl Default for DagSyncState {
    fn default() -> Self {
        Self::new()
    }
}

impl DagSyncState {
    /// Create a new sync state tracker with default settings.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(SyncStateInner::default()),
        }
    }

    /// Create a new sync state tracker with custom max synced limit.
    ///
    /// # Arguments
    ///
    /// * `max_synced` - Maximum number of synced CIDs to track. When exceeded,
    ///   the oldest synced CIDs are evicted.
    pub fn with_max_synced(max_synced: usize) -> Self {
        Self {
            state: RwLock::new(SyncStateInner {
                max_synced,
                ..Default::default()
            }),
        }
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
    ///
    /// If the synced set exceeds the maximum size, the oldest synced CIDs
    /// are evicted to make room.
    pub async fn complete_sync(&self, cid: Cid) {
        let mut state = self.state.write().await;
        state.syncing.remove(&cid);

        // Only add if not already synced (avoid duplicate in order queue)
        if state.synced.insert(cid) {
            state.synced_order.push_back(cid);

            // Evict oldest synced CIDs if over limit
            while state.synced.len() > state.max_synced {
                if let Some(old_cid) = state.synced_order.pop_front() {
                    state.synced.remove(&old_cid);
                    debug!(
                        cid = %old_cid,
                        synced_count = state.synced.len(),
                        max_synced = state.max_synced,
                        "Evicted old synced CID to stay within memory limit"
                    );
                } else {
                    // Order queue is empty but synced set isn't - shouldn't happen
                    // but handle gracefully by clearing everything
                    warn!("Synced order queue empty but synced set is not - clearing synced set");
                    state.synced.clear();
                    break;
                }
            }
        }
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

    /// Get the number of synced CIDs being tracked.
    pub async fn synced_count(&self) -> usize {
        self.state.read().await.synced.len()
    }

    /// Clear all state (for testing or reset).
    pub async fn clear(&self) {
        let mut state = self.state.write().await;
        state.syncing.clear();
        state.synced.clear();
        state.synced_order.clear();
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
    pub state: Arc<DagSyncState>,

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

        // Use validated constructor - will always return Some since we checked missing.is_empty() above
        Ok(SyncPlan::needs_fetch_new(block_cid, missing, providers)
            .expect("missing is non-empty, validated above"))
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

/// Data for a NeedsFetch sync plan with enforced invariants.
///
/// The `missing` field is guaranteed to be non-empty.
/// Use `NeedsFetchData::new()` to construct.
#[derive(Debug, Clone)]
pub struct NeedsFetchData {
    /// Root block CID that triggered the sync.
    root: Cid,
    /// CIDs that need to be fetched (guaranteed non-empty).
    missing: Vec<Cid>,
    /// Potential providers for the blocks.
    providers: Vec<PeerId>,
}

impl NeedsFetchData {
    /// Create a new NeedsFetchData with validation.
    ///
    /// Returns `None` if `missing` is empty.
    pub fn new(root: Cid, missing: Vec<Cid>, providers: Vec<PeerId>) -> Option<Self> {
        if missing.is_empty() {
            None
        } else {
            Some(Self {
                root,
                missing,
                providers,
            })
        }
    }

    /// Get the root CID.
    pub fn root(&self) -> Cid {
        self.root
    }

    /// Get the missing CIDs (guaranteed non-empty).
    pub fn missing(&self) -> &[Cid] {
        &self.missing
    }

    /// Get the providers.
    pub fn providers(&self) -> &[PeerId] {
        &self.providers
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
    /// Use `SyncPlan::needs_fetch_new()` or `NeedsFetchData::new()` to create,
    /// which enforces the invariant that `missing` is non-empty.
    NeedsFetch(NeedsFetchData),
}

impl SyncPlan {
    /// Create a NeedsFetch plan with validation.
    ///
    /// Returns `None` if `missing` is empty (use `SyncPlan::Complete` instead).
    pub fn needs_fetch_new(root: Cid, missing: Vec<Cid>, providers: Vec<PeerId>) -> Option<Self> {
        NeedsFetchData::new(root, missing, providers).map(Self::NeedsFetch)
    }

    /// Get the root CID if this is a NeedsFetch plan.
    pub fn root(&self) -> Option<Cid> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data.root()),
            _ => None,
        }
    }

    /// Check if a fetch is needed.
    pub fn needs_fetch(&self) -> bool {
        matches!(self, SyncPlan::NeedsFetch(_))
    }

    /// Get the missing CIDs if a fetch is needed.
    pub fn missing(&self) -> Option<&[Cid]> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data.missing()),
            _ => None,
        }
    }

    /// Get the providers if a fetch is needed.
    pub fn providers(&self) -> Option<&[PeerId]> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data.providers()),
            _ => None,
        }
    }

    /// Get the NeedsFetchData if this is a NeedsFetch plan.
    pub fn fetch_data(&self) -> Option<&NeedsFetchData> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data),
            _ => None,
        }
    }
}
