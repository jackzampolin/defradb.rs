//! Result types for the replication loop.

use cid::Cid;

use crate::QueryId;

/// Result of a replication loop iteration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReplicationResult {
    /// Block was merged successfully
    Merged {
        cid: Cid,
        doc_id: String,
        collection_id: String,
    },
    /// Block was merged but re-broadcast failed (replication to other nodes may be incomplete)
    MergedButBroadcastFailed {
        cid: Cid,
        doc_id: String,
        collection_id: String,
        broadcast_error: String,
    },
    /// Block was skipped (already applied or rejected)
    Skipped {
        cid: Cid,
        doc_id: String,
        collection_id: String,
        reason: String,
        terminal: bool,
    },
    /// Merge failed
    Failed { cid: Cid, error: String },
    /// Merge succeeded but failed to mark as merged (will be reprocessed on restart)
    MergedButNotMarked { cid: Cid, error: String },
    /// Event channel closed
    ChannelClosed,
    /// Bitswap fetch started for missing blocks
    BitswapFetchStarted { root_cid: Cid, query_id: QueryId },
}
