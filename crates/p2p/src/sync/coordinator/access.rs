//! Access control for the sync coordinator.

use blockstore::Blockstore;

use super::SyncCoordinator;
use crate::bitswap::AccessMode;
use crate::error::{Error, Result};
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn transport_reports_connected_peer(&self, peer_id_str: &str) -> bool {
        match self.runtime.transport.connected_peers().await {
            Ok(peers) => {
                let is_connected = peers.iter().any(|peer| peer.as_str() == peer_id_str);
                if is_connected {
                    self.access.peer_state.peer_connected(peer_id_str);
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

    /// Check if a peer (by string ID) has access to sync a collection.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Error::AccessDenied)` if denied.
    ///
    /// Access rules:
    /// 1. If mode is Open → allow all
    /// 2. If peer is a replicator for the collection → allow
    /// 3. If peer is connected → allow (matches Go DefraDB behavior)
    /// 4. Otherwise → deny
    ///
    /// Rule 3 matches Go DefraDB behavior: replicator registration is
    /// one-directional (source registers target), but the target accepts
    /// push-log requests from any connected peer. Connected peers are
    /// already authenticated via transport-level crypto. Document-level
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
        if self.access.access_mode.is_open() {
            return Ok(());
        }

        if self
            .access
            .replicators
            .is_replicator(collection_id, peer_id_str)
        {
            return Ok(());
        }

        // Treat the transport's replicator state as the source of truth on a cache miss.
        // Most runtime entrypoints now share one registry instance between transport and
        // coordinator, but this fallback preserves authorization if a caller still wires
        // separate state or if a stale in-memory view survives during bootstrap.
        let peer_id = PeerId::new(peer_id_str.to_string());
        if let Ok(Some(info)) = self.runtime.transport.get_replicator(&peer_id).await {
            if info.collections.iter().any(|id| id == collection_id) {
                self.access
                    .replicators
                    .add_replicator(collection_id, peer_id_str);
                return Ok(());
            }
        }

        // Accept messages from any connected peer. Connected peers are already
        // authenticated via transport-level crypto. The replicator registry
        // controls what WE push; it should not gate what we ACCEPT from
        // authenticated peers. This matches Go DefraDB where the replicator
        // target accepts push-logs without explicit subscription.
        if self.access.peer_state.is_connected(peer_id_str) {
            return Ok(());
        }

        // The transport is the source of truth for active connections. PeerState is a
        // best-effort cache populated by transport events and can lag during bootstrap
        // or if a runtime wires a stale coordinator view. On a cache miss, consult the
        // transport directly and backfill PeerState so later checks stay hot.
        if self.transport_reports_connected_peer(peer_id_str).await {
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
    /// In Open mode, all peers are allowed. In Controlled mode, the peer
    /// must be a connected peer or a replicator for at least one collection.
    pub(super) async fn check_peer_is_replicator(&self, peer_id: &PeerId) -> Result<()> {
        if self.access.access_mode.is_open() {
            return Ok(());
        }

        if self.access.replicators.is_any_replicator(peer_id.as_str()) {
            return Ok(());
        }

        if self.access.peer_state.is_connected(peer_id.as_str()) {
            return Ok(());
        }

        if self.transport_reports_connected_peer(peer_id.as_str()).await {
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
