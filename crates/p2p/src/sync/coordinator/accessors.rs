//! Accessor methods for the sync coordinator.

use std::sync::Arc;

use blockstore::Blockstore;

use super::SyncCoordinator;
use crate::bitswap::ReplicatorRegistry;
use crate::host::P2PHostHandle;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::manager::SyncManager;
use crate::sync::peer_state::PeerStateTracker;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Get the replicator registry.
    pub fn replicators(&self) -> &Arc<ReplicatorRegistry> {
        &self.replicators
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        self.manager.blockstore()
    }

    /// Get the broadcaster reference.
    pub fn broadcaster(&self) -> &Broadcaster {
        &self.broadcaster
    }

    /// Get the local peer ID.
    pub fn local_peer_id(&self) -> &str {
        &self.local_peer_id
    }

    /// Get the peer state tracker reference.
    pub fn peer_state(&self) -> &PeerStateTracker {
        &self.peer_state
    }

    /// Get the host handle for direct peer communication.
    pub fn host(&self) -> &P2PHostHandle {
        &self.host
    }

    /// Get the sync manager reference.
    pub fn manager(&self) -> &SyncManager<B> {
        &self.manager
    }

    // Note: The request_block and request_block_from_any_peer methods were removed.
    // They didn't interoperate with Go DefraDB (which uses Bitswap).
    //
    // For block fetching, use the DagSync module with Bitswap:
    //   - DagSync::prepare_sync() to identify missing blocks
    //   - behaviour.bitswap_sync() to fetch via Bitswap protocol
}
