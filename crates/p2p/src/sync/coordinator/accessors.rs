//! Accessor methods for the sync coordinator.

use std::sync::Arc;

use acp::DocumentACP;
use blockstore::Blockstore;

use super::{DagFetchLimiter, SyncCoordinator, SyncShutdownHandle};
use crate::bitswap::ReplicatorRegistry;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::manager::SyncManager;
use crate::sync::peer_state::PeerStateTracker;
use crate::transport::P2PTransport;

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Get the replicator registry.
    pub fn replicators(&self) -> &Arc<ReplicatorRegistry> {
        &self.access.replicators
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        self.manager.blockstore()
    }

    /// Get the broadcaster reference.
    pub fn broadcaster(&self) -> &Broadcaster<T> {
        &self.runtime.broadcaster
    }

    /// Get the local peer ID.
    pub fn local_peer_id(&self) -> &str {
        &self.access.local_peer_id
    }

    /// Get the peer state tracker reference.
    pub fn peer_state(&self) -> &PeerStateTracker {
        &self.access.peer_state
    }

    /// Get the transport reference.
    pub fn transport(&self) -> &T {
        &self.runtime.transport
    }

    /// Get the shutdown handle for coordinator-owned background tasks.
    pub fn background_shutdown_handle(&self) -> SyncShutdownHandle {
        self.runtime.shutdown.clone()
    }

    /// Get the shared DAG fetch limiter.
    pub(crate) fn dag_fetch_limiter(&self) -> DagFetchLimiter {
        self.runtime.dag_fetch_limiter.clone()
    }

    /// Get the sync manager reference.
    pub fn manager(&self) -> &SyncManager<B> {
        &self.manager
    }

    /// Wire document ACP into the coordinator.
    pub fn set_document_acp(&self, acp: Arc<dyn DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }
}
