//! Peer state tracking for P2P synchronization.
//!
//! Tracks which blocks each peer has, enabling:
//! - Efficient block requests (ask peers who have the block)
//! - Avoiding redundant sends (don't send blocks peers already have)
//! - Replication status monitoring

mod memory;
mod operations;
#[cfg(test)]
pub mod tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use cid::Cid;

/// Default maximum number of CIDs to track per peer.
/// This prevents unbounded memory growth in long-running nodes.
const DEFAULT_MAX_CIDS_PER_PEER: usize = 10_000;

/// Default maximum total CIDs across all peers.
/// With 100 peers at 10k CIDs each = ~40MB memory usage.
const DEFAULT_MAX_TOTAL_CIDS: usize = 1_000_000;

/// Default maximum number of tracked peers.
const DEFAULT_MAX_PEERS: usize = 1_000;

/// Information about a single peer's sync state.
#[derive(Debug)]
pub(super) struct PeerInfo {
    /// CIDs this peer has announced or we've sent to them
    pub(super) known_cids: HashSet<Cid>,
    /// Insertion order for LRU eviction (oldest first)
    pub(super) cid_order: VecDeque<Cid>,
    /// Collections this peer is subscribed to
    pub(super) subscribed_collections: HashSet<String>,
    /// When we last heard from this peer
    pub(super) last_seen: Instant,
    /// Whether peer is currently connected
    pub(super) connected: bool,
}

impl PeerInfo {
    pub(super) fn new() -> Self {
        Self {
            known_cids: HashSet::new(),
            cid_order: VecDeque::new(),
            subscribed_collections: HashSet::new(),
            last_seen: Instant::now(),
            connected: false,
        }
    }

    /// Add a CID with LRU eviction if at capacity.
    pub(super) fn add_cid(&mut self, cid: Cid, max_cids: usize) {
        // If already present, don't add again (maintains LRU order)
        if self.known_cids.contains(&cid) {
            return;
        }

        // Evict oldest if at capacity
        while self.known_cids.len() >= max_cids {
            if let Some(oldest) = self.cid_order.pop_front() {
                self.known_cids.remove(&oldest);
            } else {
                break;
            }
        }

        // Add the new CID
        self.known_cids.insert(cid);
        self.cid_order.push_back(cid);
    }
}

/// Tracks the sync state of all known peers.
///
/// Thread-safe: can be shared across tasks.
///
/// # Memory Limits
///
/// To prevent unbounded memory growth, the tracker enforces three limits:
/// - `max_cids_per_peer`: Maximum CIDs tracked for any single peer (LRU eviction)
/// - `max_total_cids`: Maximum CIDs across ALL peers (oldest peers evicted first)
/// - `max_peers`: Maximum number of tracked peers (oldest disconnected peers evicted)
pub struct PeerStateTracker {
    /// Per-peer state
    pub(super) peers: RwLock<HashMap<String, PeerInfo>>,
    /// How long to keep peer info after disconnect
    pub(super) peer_ttl: Duration,
    /// Maximum CIDs to track per peer (prevents memory exhaustion)
    pub(super) max_cids_per_peer: usize,
    /// Maximum total CIDs across all peers
    pub(super) max_total_cids: usize,
    /// Maximum number of tracked peers
    pub(super) max_peers: usize,
}

impl Default for PeerStateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerStateTracker {
    /// Create a new peer state tracker with default settings.
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl: Duration::from_secs(3600), // 1 hour default
            max_cids_per_peer: DEFAULT_MAX_CIDS_PER_PEER,
            max_total_cids: DEFAULT_MAX_TOTAL_CIDS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    /// Create with custom peer TTL.
    pub fn with_ttl(peer_ttl: Duration) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
            max_cids_per_peer: DEFAULT_MAX_CIDS_PER_PEER,
            max_total_cids: DEFAULT_MAX_TOTAL_CIDS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    /// Create with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `peer_ttl` - How long to keep disconnected peer info
    /// * `max_cids_per_peer` - Max CIDs per peer (0 = use default)
    ///
    /// # Logging
    ///
    /// Logs a warning if `max_cids_per_peer` is 0 (falls back to default).
    pub fn with_config(peer_ttl: Duration, max_cids_per_peer: usize) -> Self {
        let max_cids = if max_cids_per_peer == 0 {
            tracing::warn!(
                "max_cids_per_peer was 0, using default value {}",
                DEFAULT_MAX_CIDS_PER_PEER
            );
            DEFAULT_MAX_CIDS_PER_PEER
        } else {
            max_cids_per_peer
        };
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
            max_cids_per_peer: max_cids,
            max_total_cids: DEFAULT_MAX_TOTAL_CIDS,
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    /// Create with full custom configuration including global limits.
    ///
    /// # Arguments
    ///
    /// * `peer_ttl` - How long to keep disconnected peer info
    /// * `max_cids_per_peer` - Max CIDs per peer (0 = use default)
    /// * `max_total_cids` - Max total CIDs across all peers (0 = use default)
    /// * `max_peers` - Max tracked peers (0 = use default)
    pub fn with_full_config(
        peer_ttl: Duration,
        max_cids_per_peer: usize,
        max_total_cids: usize,
        max_peers: usize,
    ) -> Self {
        let max_cids = if max_cids_per_peer == 0 {
            tracing::warn!(
                "max_cids_per_peer was 0, using default value {}",
                DEFAULT_MAX_CIDS_PER_PEER
            );
            DEFAULT_MAX_CIDS_PER_PEER
        } else {
            max_cids_per_peer
        };
        let max_total = if max_total_cids == 0 {
            tracing::warn!(
                "max_total_cids was 0, using default value {}",
                DEFAULT_MAX_TOTAL_CIDS
            );
            DEFAULT_MAX_TOTAL_CIDS
        } else {
            max_total_cids
        };
        let max_p = if max_peers == 0 {
            tracing::warn!("max_peers was 0, using default value {}", DEFAULT_MAX_PEERS);
            DEFAULT_MAX_PEERS
        } else {
            max_peers
        };
        Self {
            peers: RwLock::new(HashMap::new()),
            peer_ttl,
            max_cids_per_peer: max_cids,
            max_total_cids: max_total,
            max_peers: max_p,
        }
    }
}
