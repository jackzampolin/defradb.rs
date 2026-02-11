//! Adapter to bridge P2PHostHandle to HTTP's P2POperations trait.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use defra_http::router::{P2POperations, ReplicatorInfo};
use p2p::sync::SyncCoordinator;
use p2p::P2PHostHandle;

/// Trait for looking up collection IDs by name.
///
/// This is used by the P2P adapter to resolve collection names to their
/// CollectionIDs for topic subscription, matching Go DefraDB behavior.
pub trait CollectionLookup: Send + Sync {
    fn get_collection_id(&self, name: &str) -> Option<String>;
}

/// Type-erased interface for push operations and collection lookup.
///
/// Subsumes `CollectionLookup` — implementations provide both capabilities.
/// This avoids threading the store type parameter `S` through the HTTP layer.
#[async_trait]
pub trait DocPusher: Send + Sync {
    async fn push_existing_docs(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
    ) -> Result<(), String>;

    fn get_collection_id(&self, name: &str) -> Option<String>;

    fn list_collections(&self) -> Result<Vec<String>, String>;
}

/// Database-backed `DocPusher` implementation.
///
/// Wraps `db::DB<S>` and delegates to `db::push_existing_docs` for push
/// operations and `db::DB::get_collection` / `list_collections` for lookups.
pub struct DbDocPusher<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + 'static> DbDocPusher<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }

    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn DocPusher> {
        Arc::new(Self::new(db))
    }
}

#[async_trait]
impl<S: storage::corekv::Store + 'static> DocPusher for DbDocPusher<S> {
    async fn push_existing_docs(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
    ) -> Result<(), String> {
        db::push_existing_docs(handle, &self.db, peer_id, collections, se_key).await
    }

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

    fn list_collections(&self) -> Result<Vec<String>, String> {
        self.db
            .list_collections()
            .map_err(|e| format!("failed to list collections: {}", e))
    }
}

/// Also implement `CollectionLookup` so `DbDocPusher` can be used anywhere
/// the older trait is expected.
impl<S: storage::corekv::Store + 'static> CollectionLookup for DbDocPusher<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        DocPusher::get_collection_id(self, name)
    }
}

/// Adapter that implements P2POperations using P2PHostHandle.
///
/// Optionally uses a SyncCoordinator for replicator operations,
/// which enables auto-subscribe to collection topics.
pub struct P2PAdapter<
    B: Blockstore + 'static = blockstore::DefraBlockstore<storage::backends::MemoryStore>,
> {
    handle: P2PHostHandle,
    sync_coordinator: Option<Arc<SyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn DocPusher>>,
    // Used by Phases 2-6 (connect_peer, add_replicator, add_documents, etc.)
    #[allow(dead_code)]
    event_bus: Option<Arc<dyn events::Bus>>,
    #[allow(dead_code)]
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
    #[allow(dead_code)]
    tracked_documents: Arc<std::sync::RwLock<HashSet<String>>>,
}

impl<B: Blockstore + 'static> P2PAdapter<B> {
    /// Create a new adapter wrapping the given P2P handle.
    pub fn new(handle: P2PHostHandle) -> Self {
        Self {
            handle,
            sync_coordinator: None,
            doc_pusher: None,
            event_bus: None,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
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
            doc_pusher: None,
            event_bus: None,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    /// Create a new adapter with sync coordinator and collection lookup.
    pub fn with_sync_coordinator_and_lookup(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
        lookup: Arc<dyn CollectionLookup>,
    ) -> Self {
        let doc_pusher: Arc<dyn DocPusher> = Arc::new(LookupOnlyDocPusher(lookup));
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            doc_pusher: Some(doc_pusher),
            event_bus: None,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    /// Create a new adapter with full context for FFI-parity operations.
    pub fn with_full_context(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
        doc_pusher: Arc<dyn DocPusher>,
        event_bus: Arc<dyn events::Bus>,
    ) -> Self {
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            doc_pusher: Some(doc_pusher),
            event_bus: Some(event_bus),
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
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

    /// Create an Arc-wrapped adapter with full context.
    pub fn with_full_context_arc(
        handle: P2PHostHandle,
        coordinator: Arc<SyncCoordinator<B>>,
        doc_pusher: Arc<dyn DocPusher>,
        event_bus: Arc<dyn events::Bus>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_full_context(
            handle,
            coordinator,
            doc_pusher,
            event_bus,
        ))
    }
}

/// Adapter that wraps a `CollectionLookup` as a `DocPusher` for backward
/// compatibility. Push operations return an error since no DB is available.
struct LookupOnlyDocPusher(Arc<dyn CollectionLookup>);

#[async_trait]
impl DocPusher for LookupOnlyDocPusher {
    async fn push_existing_docs(
        &self,
        _handle: &P2PHostHandle,
        _peer_id: libp2p::PeerId,
        _collections: &[String],
        _se_key: Option<&[u8]>,
    ) -> Result<(), String> {
        Err("push_existing_docs not available (no database context)".to_string())
    }

    fn get_collection_id(&self, name: &str) -> Option<String> {
        self.0.get_collection_id(name)
    }

    fn list_collections(&self) -> Result<Vec<String>, String> {
        Err("list_collections not available (no database context)".to_string())
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

        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .set_replicator(peer_id, collections, true)
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        } else {
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

        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .remove_replicator_collections(peer_id, collections)
                .await
                .map_err(|e| e.to_string())?;
        } else {
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
        if let Some(ref coordinator) = self.sync_coordinator {
            for collection_name in collections {
                let topic_id = if let Some(ref pusher) = self.doc_pusher {
                    if let Some(collection_id) = pusher.get_collection_id(&collection_name) {
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

    async fn get_documents(&self) -> Result<Vec<defra_http::router::P2pDocumentInfo>, String> {
        Ok(Vec::new())
    }

    async fn add_documents(
        &self,
        _docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> Result<(), String> {
        Err("document-level P2P replication not yet implemented".to_string())
    }

    async fn remove_documents(
        &self,
        _docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> Result<(), String> {
        Err("document-level P2P replication not yet implemented".to_string())
    }

    async fn sync_collections(&self) -> Result<(), String> {
        Err("sync_collections not yet implemented".to_string())
    }

    async fn sync_documents(&self) -> Result<(), String> {
        Err("sync_documents not yet implemented".to_string())
    }
}

/// Implementation of CollectionLookup for the database.
///
/// Retained for backward compatibility. Prefer `DbDocPusher` for new code.
pub struct DbCollectionLookup<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + 'static> DbCollectionLookup<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }

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
