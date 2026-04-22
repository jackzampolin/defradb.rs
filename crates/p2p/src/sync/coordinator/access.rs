//! Access control for the sync coordinator.
//!
//! The actual decision lives in [`super::authorizer::RuntimeAuthorizer`]
//! so the pubsub_rpc handlers (which don't have easy access to the
//! generic `SyncCoordinator`) can make the same decision. These helpers
//! adapt the shared boolean authorizer into the `Result<()>` shape the
//! two-stream event handlers expect, including the log/`AccessDenied`
//! error details.

use blockstore::Blockstore;

use super::authorizer::AccessAuthorizer;
use super::SyncCoordinator;
use crate::bitswap::AccessMode;
use crate::error::{Error, Result};
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Check if a peer (by string ID) has access to sync a collection.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Error::AccessDenied)` if denied.
    ///
    /// Access rules (implemented in
    /// [`super::authorizer::RuntimeAuthorizer::peer_authorized_for_collection`]):
    /// 1. If mode is Open → allow all
    /// 2. If peer is a replicator for the collection → allow
    /// 3. If transport reports peer as a replicator for the collection (cache miss) → allow
    /// 4. If peer is connected → allow
    /// 5. If transport reports peer as connected (cache miss) → allow
    /// 6. Otherwise → deny
    ///
    /// Rule 4 matches Go DefraDB behavior: replicator registration is
    /// one-directional (source registers target), but the target accepts
    /// push-log requests from any connected peer. Connected peers are
    /// already authenticated via transport-level crypto. Document-level
    /// ACP still applies independently at merge time.
    ///
    /// Important: collection access is broader than explicit replicator trust.
    /// Callers that need to know whether a peer is an actual registered
    /// replicator must use [`Self::is_registered_replicator`] instead of treating a
    /// successful access check as equivalent.
    ///
    /// Uses string-based registry lookup, supporting both libp2p and iroh peer IDs.
    pub(super) async fn check_access_str(
        &self,
        peer_id_str: &str,
        collection_id: &str,
    ) -> Result<()> {
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

    /// Check if a peer is authorized as a replicator for any collection.
    ///
    /// Used by handlers (e.g. DocSync) that lack collection context.
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
