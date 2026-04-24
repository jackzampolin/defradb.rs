//! Peer authorization for sync ingress.
//!
//! Centralises the "may this peer ask us to sync?" decision so the
//! two-stream path (`SyncCoordinator::check_peer_is_replicator` /
//! `check_access_str`) and the `pubsub_rpc` path
//! (`HandlerContext::peer_may_*`) can't drift. Drift here is a Go
//! parity hazard: every request path must agree that Controlled mode
//! requires sync authorization, not just an authenticated connection.

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
    /// sync request (DocSync/CAR fetch).
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

    async fn transport_replicator_collections(&self, peer_id_str: &str) -> Option<Vec<String>> {
        let peer_id = PeerId::new(peer_id_str.to_string());
        match self.transport.get_replicator(&peer_id).await {
            Ok(Some(info)) if !info.collections.is_empty() => {
                self.replicators
                    .set_peer_collections(peer_id_str, &info.collections);
                Some(info.collections)
            }
            Ok(_) => None,
            Err(error) => {
                tracing::debug!(
                    peer_id = %peer_id_str,
                    error = %error,
                    "Failed to read transport replicator state during access check"
                );
                None
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
        if self.peer_state.peer_has_data_subscription(peer_id_str) {
            return true;
        }
        self.transport_replicator_collections(peer_id_str)
            .await
            .is_some()
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
        self.transport_replicator_collections(peer_id_str)
            .await
            .is_some_and(|collections| collections.iter().any(|id| id == collection_id))
    }
}
