//! Access control for the sync coordinator.

use blockstore::Blockstore;

use super::SyncCoordinator;
use crate::bitswap::AccessMode;
use crate::error::{Error, Result};
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Check if a peer (by string ID) has access to sync a collection.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Error::AccessDenied)` if denied.
    ///
    /// Access rules:
    /// 1. If mode is Open → allow all
    /// 2. If peer is a replicator for the collection → allow
    /// 3. If peer is connected and the collection is subscribed → allow
    /// 4. Otherwise → deny
    ///
    /// Rule 3 matches Go DefraDB behavior: replicator registration is
    /// one-directional (source registers target), but both sides accept
    /// messages from connected peers on subscribed topics. Document-level
    /// ACP still applies independently at merge time.
    ///
    /// Important: collection access is broader than explicit replicator trust.
    /// Callers that need to know whether a peer is an actual registered
    /// replicator must use `is_registered_replicator()` instead of treating a
    /// successful access check as equivalent.
    ///
    /// Uses string-based registry lookup, supporting both libp2p and iroh peer IDs.
    pub(super) async fn check_access_str(
        &self,
        peer_id_str: &str,
        collection_id: &str,
    ) -> Result<()> {
        if self.access_mode.is_open() {
            return Ok(());
        }

        if self.replicators.is_replicator(collection_id, peer_id_str) {
            return Ok(());
        }

        // Accept messages from connected peers for collections we're subscribed to.
        // Connected peers are already authenticated via transport-level crypto. The
        // replicator registry controls what WE push; it shouldn't gate what we ACCEPT
        // from authenticated peers on topics we've subscribed to.
        if self.peer_state.is_connected(peer_id_str) {
            let subscribed = self.subscribed_collections.read().await;
            if subscribed.contains(collection_id) {
                return Ok(());
            }
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
        self.replicators.is_replicator(collection_id, peer_id_str)
    }

    /// Check if a peer is authorized as a replicator for any collection.
    ///
    /// Used by handlers (e.g. DocSync) that lack collection context.
    /// In Open mode, all peers are allowed. In Controlled mode, the peer
    /// must be a connected peer or a replicator for at least one collection.
    pub(super) fn check_peer_is_replicator(&self, peer_id: &PeerId) -> Result<()> {
        if self.access_mode.is_open() {
            return Ok(());
        }

        if self.replicators.is_any_replicator(peer_id.as_str()) {
            return Ok(());
        }

        if self.peer_state.is_connected(peer_id.as_str()) {
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
        self.access_mode
    }
}
