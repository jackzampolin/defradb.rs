//! DAG synchronizer implementation.

use std::collections::HashSet;
use std::sync::Arc;

use cid::Cid;
use tracing::{debug, warn};

use crate::error::Result;
use crate::sync::PeerStateTracker;

use super::config::DagSyncConfig;
use super::plan::SyncPlan;
use super::state::DagSyncState;

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
    fn get_providers(&self, cids: &[Cid]) -> Vec<String> {
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
