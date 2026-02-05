//! Pending DAG tracking for Bitswap synchronization.

use std::collections::HashSet;

use cid::Cid;

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
}
