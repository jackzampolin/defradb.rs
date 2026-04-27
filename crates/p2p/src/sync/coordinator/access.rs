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
    /// 2. If peer is a replicator for the given collection → allow
    /// 3. If transport reports peer as a replicator for the collection
    ///    (registry cache miss) → allow
    /// 4. Otherwise → deny
    ///
    /// Transport-level authentication proves *who* a peer is, not *what* they
    /// are authorized to sync. Per-collection registry membership is the only
    /// thing that grants collection-scoped access in Controlled mode, matching
    /// Go DefraDB's `hasAccess` check (`go-p2p/peer.go`). A previous version
    /// of this module accepted any connected peer as a fallback — see #838 for
    /// the divergence that caused.
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

    /// Check if a peer is a replicator for *any* collection.
    ///
    /// Used by handlers (DocSync, CAR fetch) whose wire protocol does not
    /// carry a collection id. Strict membership is the best gate we can
    /// apply without protocol-level collection scoping; it still eliminates
    /// the "any connected peer" bypass that #838 flagged. In Open mode all
    /// peers are allowed. In Controlled mode the peer must either be
    /// registered as a replicator for at least one collection, be observed on
    /// a data subscription topic, or appear in the transport's replicator
    /// state on a registry cache miss.
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
