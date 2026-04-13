//! Sync manager events.

use cid::Cid;

use acp::ReplicatedDocActorRelationships;

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
        /// Optional local-ACP actor relationship snapshot for the document.
        acp_actor_relationships: Option<ReplicatedDocActorRelationships>,
    },

    /// A block was already merged (received duplicate).
    BlockAlreadyMerged {
        cid: Cid,
        doc_id: String,
        collection_id: String,
        acp_actor_relationships: Option<ReplicatedDocActorRelationships>,
    },

    /// Failed to process a sync request.
    SyncError { cid: Cid, error: String },

    /// DAG has missing blocks that need to be fetched via Bitswap.
    ///
    /// The coordinator should:
    /// 1. Call host.bitswap_sync() with the missing CIDs
    /// 2. Register the QueryId with manager.register_query()
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
        /// Optional local-ACP actor relationship snapshot for the document.
        acp_actor_relationships: Option<ReplicatedDocActorRelationships>,
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
        /// Optional local-ACP actor relationship snapshot for the document.
        acp_actor_relationships: Option<ReplicatedDocActorRelationships>,
    },
}
