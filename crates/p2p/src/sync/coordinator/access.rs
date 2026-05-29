//! Access control for the sync coordinator.
//!
//! Most decisions live in [`super::authorizer::RuntimeAuthorizer`] so the
//! pubsub_rpc handlers (which don't have easy access to the generic
//! `SyncCoordinator`) can make the same decision. These helpers adapt the
//! shared boolean authorizer into the `Result<()>` shape the event handlers
//! expect, and layer in local collection-subscription ingress where the sync
//! API explicitly opts this node into receiving collection updates.

use blockstore::Blockstore;

use super::authorizer::AccessAuthorizer;
use super::SyncCoordinator;
use crate::bitswap::AccessMode;
use crate::error::{Error, Result};
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Returns true when this node has joined the collection's sync topic.
    pub(super) async fn is_locally_subscribed_collection(&self, collection_id: &str) -> bool {
        self.subscriptions
            .subscribed_collections
            .read()
            .await
            .contains(collection_id)
    }

    /// Check if a peer (by string ID) has direct PushLog access to sync a collection.
    ///
    /// Direct PushLog is the delivery path used by one-way replicators. A local
    /// collection subscription means this node has explicitly opted into
    /// receiving updates for that collection; merge-time ACP still decides
    /// whether the document content is acceptable.
    pub(super) async fn check_pushlog_access_str(
        &self,
        peer_id_str: &str,
        collection_id: &str,
    ) -> Result<()> {
        if self
            .authorizer
            .peer_authorized_for_collection(peer_id_str, collection_id)
            .await
            || self.is_locally_subscribed_collection(collection_id).await
        {
            return Ok(());
        }

        tracing::warn!(
            peer_id = %peer_id_str,
            collection_id = %collection_id,
            "Access denied: peer is not authorized for direct PushLog on this collection"
        );
        Err(Error::AccessDenied {
            peer_id: peer_id_str.to_string(),
            collection_id: collection_id.to_string(),
        })
    }

    /// Check if a peer (by string ID) has access to sync a collection.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Error::AccessDenied)` if denied.
    ///
    /// Access rules:
    /// 1. If mode is Open → allow all
    /// 2. If peer is connected → allow pull-sync, matching Go's
    ///    `sync-branchable` handler
    /// 3. If peer is a replicator for the given collection → allow
    /// 4. If transport reports peer as a replicator for the collection
    ///    (registry cache miss) → allow
    /// 5. Otherwise → deny
    ///
    /// Document-level ACP still applies independently at merge time; this
    /// gate exists so that unauthorized peers never cause the receiver to
    /// spend resources validating their blocks in the first place.
    ///
    /// Uses string-based registry lookup, supporting both libp2p and iroh peer IDs.
    pub(super) async fn check_access_str(
        &self,
        peer_id_str: &str,
        collection_id: &str,
    ) -> Result<()> {
        if self.access.peer_state.is_connected(peer_id_str) {
            return Ok(());
        }

        if self
            .authorizer
            .peer_authorized_for_collection(peer_id_str, collection_id)
            .await
        {
            return Ok(());
        }

        tracing::warn!(
            peer_id = %peer_id_str,
            collection_id = %collection_id,
            "Access denied: peer is not a replicator for this collection"
        );
        Err(Error::AccessDenied {
            peer_id: peer_id_str.to_string(),
            collection_id: collection_id.to_string(),
        })
    }

    /// Returns true only when the peer is explicitly registered as a
    /// replicator for the collection.
    pub(super) fn is_registered_replicator(&self, peer_id_str: &str, collection_id: &str) -> bool {
        self.access
            .replicators
            .is_replicator(collection_id, peer_id_str)
    }

    /// Check if a peer may use unscoped pull-sync protocols.
    ///
    /// Used by handlers (DocSync, CAR fetch) whose wire protocol does not
    /// carry a collection id. Go's pull-sync RPCs serve connected peers and
    /// rely on block/merge-time ACP for document policy; Rust mirrors that
    /// here while keeping PushLog's collection-scoped replicator gate.
    ///
    /// Per-doc collection filtering (rejecting reads for specific collections
    /// the peer isn't authorized for) would be a stricter fix but requires a
    /// collection-aware header on DocSync/CAR requests; out of scope here.
    /// Delegates to
    /// [`super::authorizer::RuntimeAuthorizer::peer_authorized_for_any`].
    pub(super) async fn check_peer_is_replicator(&self, peer_id: &PeerId) -> Result<()> {
        if self
            .authorizer
            .peer_authorized_for_any(peer_id.as_str())
            .await
        {
            return Ok(());
        }

        tracing::warn!(
            peer_id = %peer_id,
            "Access denied: peer is not a replicator for any collection"
        );
        Err(Error::AccessDenied {
            peer_id: peer_id.to_string(),
            collection_id: "(any)".to_string(),
        })
    }

    /// Get the current access mode.
    pub fn access_mode(&self) -> AccessMode {
        self.access.access_mode
    }
}
