//! Pending DAG tracking for Bitswap synchronization.

use std::collections::HashSet;

use cid::Cid;
use libp2p::PeerId;

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
    pub source_peer: Option<PeerId>,
}
