//! Sync manager events.

use cid::Cid;

use crate::ExplicitReplayAuthorization;

/// Events emitted by the SyncManager for higher layers to process.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SyncEvent {
    /// A new block was received and stored, needs CRDT merge.
    ///
    /// The database layer should process this by:
    /// 1. Loading the block from blockstore
    /// 2. Applying CRDT merge
    /// 3. Calling blockstore.mark_as_merged()
    BlockReceived {
        /// The CID of the received block
        cid: Cid,
        /// Document ID this block belongs to
        doc_id: String,
        /// Collection ID
        collection_id: String,
        /// Creator peer ID
        creator: String,
        /// The actual transport peer that sent this block to us.
        sender_peer: Option<String>,
        /// True when this block arrived via the explicit replicator push path.
        is_explicit_replicator: bool,
        /// Capability-based explicit replay authorization carried by two-stream pushes.
        explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    },

    /// Failed to process a sync request.
    SyncError { cid: Cid, error: String },

    /// DAG has missing blocks that need to be fetched.
    ///
    /// The coordinator should:
    /// 1. Prefer a source-peer DAG fetch when the announcing peer is known
    /// 2. Fall back to transport block sync when only provider hints are available
    DagNeedsFetch {
        /// Root CID of the DAG being synced
        root_cid: Cid,
        /// CIDs of missing blocks to fetch
        missing: Vec<Cid>,
        /// Suggested providers (peers that may have the blocks)
        providers: Vec<String>,
        /// Document ID for the root block
        doc_id: String,
        /// Collection ID for the root block
        collection_id: String,
        /// Creator of the root block
        creator: String,
        /// The actual transport peer that sent the root block to us.
        sender_peer: Option<String>,
        /// True when the root block arrived via the explicit replicator push path.
        is_explicit_replicator: bool,
        /// Capability-based explicit replay authorization carried by two-stream pushes.
        explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    },

    /// DAG is ready for merge after Bitswap fetch completed.
    ///
    /// All missing blocks have been fetched. The database layer should now
    /// process the complete DAG for CRDT merge.
    DagReady {
        /// Root CID of the completed DAG
        root_cid: Cid,
        /// Document ID
        doc_id: String,
        /// Collection ID
        collection_id: String,
        /// Creator of the root block.
        creator: String,
        /// The actual transport peer that sent the root block to us.
        sender_peer: Option<String>,
        /// True when the root block arrived via the explicit replicator push path.
        is_explicit_replicator: bool,
        /// Capability-based explicit replay authorization carried by two-stream pushes.
        explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    },
}
