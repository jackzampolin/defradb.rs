//! Pending DAG tracking for Bitswap synchronization.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use cid::Cid;

use crate::ExplicitReplayAuthorization;

/// Time-to-live for a pending DAG entry.
///
/// Entries older than this are evicted during insertion to prevent
/// indefinitely stale DAGs from accumulating.
pub const PENDING_DAG_TTL: Duration = Duration::from_secs(300);

/// Metadata for a pending DAG sync waiting for Bitswap to complete.
#[derive(Debug, Clone)]
pub struct PendingDag {
    /// Document ID from the original PushLog message
    pub doc_id: String,
    /// Collection ID from the original PushLog message
    pub collection_id: String,
    /// Creator from the original PushLog message
    pub creator: String,
    /// CIDs still missing (gets smaller as blocks arrive via Bitswap)
    #[allow(dead_code)] // Used for tracking Bitswap progress
    pub missing: HashSet<Cid>,
    /// The peer that originally provided this DAG (e.g. DocSync reply sender).
    /// Always included in the Bitswap provider list during retries so the
    /// blocks can be fetched even if the peer isn't in connected_peers().
    pub source_peer: Option<String>,
    /// True when the DAG originated from an explicit replicator push.
    pub is_explicit_replicator: bool,
    /// Capability-based explicit replay authorization carried by two-stream pushes.
    pub explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    /// Whether a success reply can rely on this entry for eventual recovery.
    pub is_recovery_registered: bool,
    /// When this entry was inserted (for TTL eviction).
    pub inserted_at: Instant,
    /// How many times `retry_pending_dag` has been invoked for this root.
    ///
    /// Surfaced in diagnostic logs so the single aggregated WARN on terminal
    /// failure can carry an attempt count without scraping intermediate noise.
    pub attempts: u32,
    /// Number of Bitswap/CAR fetch rounds that exhausted providers without
    /// yielding a complete DAG for this root.
    pub fetch_failures: u32,
    /// Most recent provider-exhaustion error for this root, if any.
    pub last_fetch_error: Option<String>,
    /// Earliest instant a fetch dispatch may be claimed for this root.
    /// Every dispatch site must win `try_claim_pending_dag_dispatch`,
    /// which advances this by `retry_backoff(dispatches)` — the receiver's
    /// re-arm pacing (#1112 inbound half, #1116 stage 2).
    pub next_retry_at: tokio::time::Instant,
    /// Fetch dispatches claimed for this root (drives the backoff rung).
    pub dispatches: u32,
}

/// Base delay before the first scheduled re-dispatch of a pending root.
pub const PENDING_RETRY_BASE: Duration = Duration::from_secs(2);
/// Ceiling for the per-root dispatch backoff.
pub const PENDING_RETRY_CAP: Duration = Duration::from_secs(60);

/// Capped exponential backoff for pending-DAG fetch dispatches: 2s, 4s,
/// 8s, 16s, 32s, then 60s forever.
pub fn retry_backoff(dispatches: u32) -> Duration {
    let shifted = PENDING_RETRY_BASE.saturating_mul(1u32 << dispatches.min(5));
    shifted.min(PENDING_RETRY_CAP)
}
