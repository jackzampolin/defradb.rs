//! Adapter to bridge P2PHostHandle to HTTP's P2POperations trait.

use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use defra_http::router::{P2POperations, P2pDocumentInfo, P2pDocumentRequest, ReplicatorInfo};
use p2p::sync::SyncCoordinator;
use p2p::P2PHostHandle;

/// Trait for looking up collection IDs by name.
///
/// This is used by the P2P adapter to resolve collection names to their
/// CollectionIDs for topic subscription, matching Go DefraDB behavior.
pub trait CollectionLookup: Send + Sync {
    /// Get a collection's ID by its name.
    ///
    /// Returns `Some(collection_id)` if found, `None` if not found.
    fn get_collection_id(&self, name: &str) -> Option<String>;
}

/// Adapter that implements P2POperations using P2PHostHandle.
///
/// Optionally uses a SyncCoordinator for replicator operations,
/// which enables auto-subscribe to collection topics.
pub struct P2PAdapter<
    B: Blockstore + 'static = blockstore::DefraBlockstore<storage::backends::MemoryStore>,
> {
    handle: P2PHostHandle,
    /// Optional sync coordinator for replicator operations with auto-subscribe
    sync_coordinator: Option<Arc<SyncCoordinator<B>>>,
    /// Optional collection lookup for resolving names to CollectionIDs
    collection_lookup: Option<Arc<dyn CollectionLookup>>,
}

impl<B: Blockstore + 'static> P2PAdapter<B> {
    /// Create a new adapter wrapping the given P2P handle.
    pub fn new(handle: P2PHostHandle) -> Self {
        Self {
            handle,
            sync_coordinator: None,
            collection_lookup: None,
        }
    }

    /// Create a new adapter with a sync coordinator for enhanced replicator support.
    pub fn with_sync_coordinator(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
    ) -> Self {
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            collection_lookup: None,
        }
    }

    /// Create a new adapter with sync coordinator and collection lookup.
    ///
    /// The collection lookup enables proper resolution of collection names to IDs
    /// for P2P topic subscription, matching Go DefraDB behavior.
    pub fn with_sync_coordinator_and_lookup(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
        lookup: Arc<dyn CollectionLookup>,
    ) -> Self {
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            collection_lookup: Some(lookup),
        }
    }
}

impl P2PAdapter<blockstore::DefraBlockstore<storage::backends::MemoryStore>> {
    /// Create an Arc-wrapped adapter.
    pub fn new_arc(handle: P2PHostHandle) -> Arc<dyn P2POperations> {
        Arc::new(Self::new(handle))
    }
}

impl<B: Blockstore + 'static> P2PAdapter<B> {
    /// Create an Arc-wrapped adapter with sync coordinator.
    pub fn with_sync_coordinator_arc(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_sync_coordinator(handle, coordinator))
    }

    /// Create an Arc-wrapped adapter with sync coordinator and collection lookup.
    pub fn with_sync_coordinator_and_lookup_arc(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
        lookup: Arc<dyn CollectionLookup>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_sync_coordinator_and_lookup(
            handle,
            coordinator,
            lookup,
        ))
    }
}

/// Parse a peer ID and multiaddr from a full multiaddr string.
fn parse_peer_id_from_multiaddr(addr: &str) -> Result<(libp2p::PeerId, libp2p::Multiaddr), String> {
    let multiaddr: libp2p::Multiaddr = addr
        .parse()
        .map_err(|e| format!("invalid multiaddr: {}", e))?;

    let peer_id = multiaddr
        .iter()
        .find_map(|proto| match proto {
            libp2p::multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
        .ok_or_else(|| "multiaddr must contain /p2p/<peer_id> component".to_string())?;

    Ok((peer_id, multiaddr))
}

#[async_trait]
impl<B: Blockstore + 'static> P2POperations for P2PAdapter<B> {
    async fn local_peer_id(&self) -> Result<String, String> {
        self.handle
            .local_peer_id()
            .await
            .map(|id| id.to_string())
            .map_err(|e| e.to_string())
    }

    async fn listen_addresses(&self) -> Result<Vec<String>, String> {
        self.handle
            .listen_addresses()
            .await
            .map(|addrs| addrs.into_iter().map(|a| a.to_string()).collect())
            .map_err(|e| e.to_string())
    }

    async fn connected_peers(&self) -> Result<Vec<String>, String> {
        self.handle
            .connected_peers()
            .await
            .map(|peers| peers.into_iter().map(|p| p.to_string()).collect())
            .map_err(|e| e.to_string())
    }

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        // Parse multiaddr and extract peer ID
        let multiaddr: libp2p::Multiaddr = addr
            .parse()
            .map_err(|e| format!("invalid multiaddr: {}", e))?;

        // Extract peer ID from multiaddr (should be in /p2p/<peer_id> component)
        let peer_id = multiaddr
            .iter()
            .find_map(|proto| match proto {
                libp2p::multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
                _ => None,
            })
            .ok_or_else(|| "multiaddr must contain /p2p/<peer_id> component".to_string())?;

        // Remove the p2p component from the address for dialing
        let dial_addr: libp2p::Multiaddr = multiaddr
            .iter()
            .filter(|proto| !matches!(proto, libp2p::multiaddr::Protocol::P2p(_)))
            .collect();

        self.handle
            .dial(peer_id, vec![dial_addr])
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String> {
        let p2p_infos = self
            .handle
            .get_all_replicators()
            .await
            .map_err(|e| e.to_string())?;

        let http_infos: Vec<ReplicatorInfo> = p2p_infos
            .into_iter()
            .map(|info| {
                let address = info.addresses_str().first().map(|s| s.to_string());
                ReplicatorInfo {
                    id: Some(info.peer_id_str().to_string()),
                    collections: info.collections,
                    address,
                }
            })
            .collect();

        Ok(http_infos)
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let (peer_id, _) = parse_peer_id_from_multiaddr(addr_str)?;

        // Use sync coordinator if available (enables auto-subscribe to topics)
        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .set_replicator(peer_id, collections, true) // auto_subscribe = true
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        } else {
            // Fall back to direct handle
            self.handle
                .set_replicator(peer_id, collections)
                .await
                .map_err(|e| e.to_string())
        }
    }

    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let (peer_id, _) = parse_peer_id_from_multiaddr(addr_str)?;

        // Go DefraDB behavior for replicator removal:
        // - If collections is empty: delete the entire replicator (all collections)
        // - If collections is non-empty: remove only those collections, keep replicator
        //   if other collections remain
        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .remove_replicator_collections(peer_id, collections)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            // Without coordinator, use direct handle (only supports full deletion)
            if !collections.is_empty() {
                tracing::warn!(
                    peer_id = %peer_id,
                    "Partial removal requested but sync coordinator not available - \
                     falling back to full deletion"
                );
            }
            self.handle
                .delete_replicator(peer_id)
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    async fn get_collections(&self) -> Result<Vec<String>, String> {
        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .get_subscribed_collections()
                .await
                .map_err(|e| e.to_string())
        } else {
            Ok(Vec::new())
        }
    }

    async fn add_collections(&self, collections: Vec<String>) -> Result<(), String> {
        // Subscribe to collection topics for P2P sync
        if let Some(ref coordinator) = self.sync_coordinator {
            for collection_name in collections {
                // Look up the collection to get its CollectionID (like Go does)
                // The CollectionID is used as the GossipSub topic
                let topic_id = if let Some(ref lookup) = self.collection_lookup {
                    if let Some(collection_id) = lookup.get_collection_id(&collection_name) {
                        tracing::debug!(
                            collection_name = %collection_name,
                            collection_id = %collection_id,
                            "Resolved collection name to CollectionID for P2P subscription"
                        );
                        collection_id
                    } else {
                        return Err(format!(
                            "collection '{}' not found - add schema before subscribing to P2P",
                            collection_name
                        ));
                    }
                } else {
                    // Fallback: use name directly (for backwards compatibility)
                    tracing::warn!(
                        collection_name = %collection_name,
                        "No collection lookup available, using name as topic (may not match Go)"
                    );
                    collection_name.clone()
                };

                coordinator
                    .subscribe_collection(&topic_id)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        } else {
            Err("p2p collections functionality requires sync coordinator".to_string())
        }
    }

    async fn remove_collections(&self, collections: Vec<String>) -> Result<(), String> {
        // Unsubscribe from collection topics
        if let Some(ref coordinator) = self.sync_coordinator {
            for collection_id in collections {
                coordinator
                    .unsubscribe_collection(&collection_id)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        } else {
            Err("p2p collections functionality requires sync coordinator".to_string())
        }
    }

    async fn get_documents(&self) -> Result<Vec<P2pDocumentInfo>, String> {
        // Document-level replication not yet implemented
        Ok(Vec::new())
    }

    async fn add_documents(&self, _docs: Vec<P2pDocumentRequest>) -> Result<(), String> {
        // Document-level replication not yet implemented
        Err("document-level replication not yet implemented".to_string())
    }

    async fn remove_documents(&self, _docs: Vec<P2pDocumentRequest>) -> Result<(), String> {
        // Document-level replication not yet implemented
        Err("document-level replication not yet implemented".to_string())
    }

    async fn sync_collections(&self) -> Result<(), String> {
        // Sync happens automatically via gossipsub; manual trigger not yet implemented
        Ok(())
    }

    async fn sync_documents(&self) -> Result<(), String> {
        // Document-level sync not yet implemented
        Err("document-level sync not yet implemented".to_string())
    }
}

/// Implementation of CollectionLookup for the database.
///
/// This allows the P2P adapter to look up collection IDs by name,
/// matching Go DefraDB's behavior for topic subscription.
pub struct DbCollectionLookup<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + 'static> DbCollectionLookup<S> {
    /// Create a new collection lookup wrapping the database.
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }

    /// Create an Arc-wrapped collection lookup.
    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn CollectionLookup> {
        Arc::new(Self::new(db))
    }
}

impl<S: storage::corekv::Store + 'static> CollectionLookup for DbCollectionLookup<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        match self.db.get_collection(name) {
            Ok(Some(collection)) => Some(collection.collection_id().to_string()),
            Ok(None) => {
                tracing::debug!(collection_name = %name, "Collection not found for P2P lookup");
                None
            }
            Err(e) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %e,
                    "Error looking up collection for P2P"
                );
                None
            }
        }
    }
}
