//! Accessor methods for the sync coordinator.

use std::sync::Arc;

use blockstore::Blockstore;

use super::SyncCoordinator;
use crate::bitswap::ReplicatorRegistry;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::manager::SyncManager;
use crate::sync::peer_state::PeerStateTracker;
use crate::transport::P2PTransport;

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Get the replicator registry.
    pub fn replicators(&self) -> &Arc<ReplicatorRegistry> {
        &self.replicators
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        self.manager.blockstore()
    }

    /// Get the broadcaster reference.
    pub fn broadcaster(&self) -> &Broadcaster<T> {
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

    /// Get the transport reference.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Get the sync manager reference.
    pub fn manager(&self) -> &SyncManager<B> {
        &self.manager
    }
}
