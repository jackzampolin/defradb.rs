//! DAG sync state tracking.

use std::collections::{HashSet, VecDeque};

use cid::Cid;
use tokio::sync::RwLock;
use tracing::{debug, warn};

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

    /// Check if a CID has recently been synced.
    ///
    /// This is a bounded in-memory hint used to avoid duplicate work during a
    /// session, not a persistent claim. A `false` result may mean the CID was
    /// never synced or that it was evicted from the recent synced cache.
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
