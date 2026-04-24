//! Peer authorization for sync ingress.
//!
//! Centralises the "may this peer ask us to sync?" decision so the
//! two-stream path (`SyncCoordinator::check_peer_is_replicator` /
//! `check_access_str`) and the `pubsub_rpc` path
//! (`HandlerContext::peer_may_*`) can't drift. Drift here is a Go
//! parity hazard: during the `PeerState` cache-miss window right after
//! a peer connects, a copy-paste-shortened version of the check will
//! deny requests that the four-step version accepts.

use std::sync::Arc;

use async_trait::async_trait;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::sync::peer_state::PeerStateTracker;
use crate::transport::{P2PTransport, PeerId};

/// Object-safe authorization backend used by the `pubsub_rpc` handlers.
///
/// Implemented by [`RuntimeAuthorizer`] in production; tests provide
/// their own fakes so the pubsub path can be exercised without a real
/// transport.
#[async_trait]
pub(super) trait AccessAuthorizer: Send + Sync {
    /// Returns `true` when the peer is allowed to send an any-collection
    /// sync request (DocSync).
    async fn peer_authorized_for_any(&self, peer_id_str: &str) -> bool;

    /// Returns `true` when the peer is allowed to send a
    /// collection-scoped sync request (BranchableSync, collection
    /// push-log access).
    async fn peer_authorized_for_collection(&self, peer_id_str: &str, collection_id: &str) -> bool;
}

/// Production authorizer. Threads the live transport state with the
/// in-memory caches (`ReplicatorRegistry`, `PeerStateTracker`) and
/// backfills caches on hits so subsequent checks stay hot.
pub(in crate::sync) struct RuntimeAuthorizer<T: P2PTransport> {
    transport: T,
    peer_state: Arc<PeerStateTracker>,
    replicators: Arc<ReplicatorRegistry>,
    access_mode: AccessMode,
}

impl<T: P2PTransport> RuntimeAuthorizer<T> {
    pub(in crate::sync) fn new(
        transport: T,
        peer_state: Arc<PeerStateTracker>,
        replicators: Arc<ReplicatorRegistry>,
        access_mode: AccessMode,
    ) -> Self {
        Self {
            transport,
            peer_state,
            replicators,
            access_mode,
        }
    }

    /// The transport is the source of truth for active connections.
    /// `PeerStateTracker` is a best-effort cache populated by transport
    /// events and can lag during bootstrap; on a miss we consult the
    /// transport directly and backfill the cache.
    async fn transport_reports_connected_peer(&self, peer_id_str: &str) -> bool {
        match self.transport.connected_peers().await {
            Ok(peers) => {
                let is_connected = peers.iter().any(|peer| peer.as_str() == peer_id_str);
                if is_connected {
                    self.peer_state.peer_connected(peer_id_str);
                }
                is_connected
            }
            Err(error) => {
                tracing::debug!(
                    peer_id = %peer_id_str,
                    error = %error,
                    "Failed to read transport-connected peers during access check"
                );
                false
            }
        }
    }
}

#[async_trait]
impl<T: P2PTransport> AccessAuthorizer for RuntimeAuthorizer<T> {
    async fn peer_authorized_for_any(&self, peer_id_str: &str) -> bool {
        if self.access_mode.is_open() {
            return true;
        }
        if self.replicators.is_any_replicator(peer_id_str) {
            return true;
        }
        if self.peer_state.is_connected(peer_id_str) {
            return true;
        }
        self.transport_reports_connected_peer(peer_id_str).await
    }

    async fn peer_authorized_for_collection(&self, peer_id_str: &str, collection_id: &str) -> bool {
        if self.access_mode.is_open() {
            return true;
        }
        if self.replicators.is_replicator(collection_id, peer_id_str) {
            return true;
        }

        // Transport's replicator state is source of truth on registry
        // cache miss. Most runtime entrypoints share one registry with
        // the transport; this preserves authorization if a caller still
        // wires separate state or during bootstrap.
        let peer_id = PeerId::new(peer_id_str.to_string());
        if let Ok(Some(info)) = self.transport.get_replicator(&peer_id).await {
            if info.collections.iter().any(|id| id == collection_id) {
                self.replicators.add_replicator(collection_id, peer_id_str);
                return true;
            }
        }

        // Accept messages from any connected (transport-authenticated)
        // peer. Matches Go DefraDB: replicator registration controls
        // what WE push, not what we ACCEPT.
        if self.peer_state.is_connected(peer_id_str) {
            return true;
        }

        self.transport_reports_connected_peer(peer_id_str).await
    }
}
