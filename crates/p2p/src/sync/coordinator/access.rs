//! Access control for the sync coordinator.

use blockstore::Blockstore;
use libp2p::PeerId;

use super::SyncCoordinator;
use crate::bitswap::AccessMode;
use crate::error::{Error, Result};

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Check if a peer has access to sync a collection.
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
    pub(super) async fn check_access(
        &self,
        peer_id: &PeerId,
        collection_id: &str,
    ) -> Result<()> {
        if self.access_mode.is_open() {
            return Ok(());
        }

        if self.replicators.is_replicator(collection_id, peer_id) {
            return Ok(());
        }

        // Accept messages from connected peers for collections we're subscribed to.
        // Connected peers are already authenticated via libp2p noise. The replicator
        // registry controls what WE push; it shouldn't gate what we ACCEPT from
        // authenticated peers on topics we've subscribed to.
        if self.peer_state.is_connected(peer_id) {
            let subscribed = self.subscribed_collections.read().await;
            if subscribed.contains(collection_id) {
                return Ok(());
            }
        }

        tracing::warn!(
            peer_id = %peer_id,
            collection_id = %collection_id,
            "Access denied: peer is not a replicator for this collection"
        );
        Err(Error::AccessDenied {
            peer_id: peer_id.to_string(),
            collection_id: collection_id.to_string(),
        })
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

        if self.replicators.is_any_replicator(peer_id) {
            return Ok(());
        }

        if self.peer_state.is_connected(peer_id) {
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
