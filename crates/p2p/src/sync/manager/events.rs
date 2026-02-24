//! Sync manager events.

use cid::Cid;

/// Events emitted by the SyncManager for higher layers to process.
#[derive(Debug, Clone)]
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
    },

    /// A block was already merged (received duplicate).
    BlockAlreadyMerged { cid: Cid },

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
        /// Schema version ID
        schema_version_id: String,
    },
}
