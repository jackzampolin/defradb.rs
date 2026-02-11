//! Adapter to bridge P2PHostHandle to HTTP's P2POperations trait.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use defra_http::router::{P2POperations, ReplicatorInfo};
use p2p::sync::SyncCoordinator;
use p2p::topics::DefraTopic;
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

    async fn persist_replicator(&self, peer_id: &str, collections: &[String])
        -> Result<(), String>;

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String>;

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String>;

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String>;

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String>;
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

    async fn persist_replicator(
        &self,
        peer_id: &str,
        collections: &[String],
    ) -> Result<(), String> {
        let pid: libp2p::PeerId = peer_id
            .parse()
            .map_err(|e| format!("invalid peer ID: {}", e))?;
        let info = p2p::ReplicatorInfo::new(pid, collections.to_vec());
        let bytes = info
            .to_bytes()
            .map_err(|e| format!("failed to serialize replicator info: {}", e))?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .set_replicator(peer_id, &bytes)
            .await
            .map_err(|e| format!("failed to persist replicator: {}", e))
    }

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .delete_replicator(peer_id)
            .await
            .map_err(|e| format!("failed to delete persisted replicator: {}", e))
    }

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String> {
        let data = serde_json::to_vec(doc_ids)
            .map_err(|e| format!("failed to serialize P2P documents: {}", e))?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .set_p2p_documents(&data)
            .await
            .map_err(|e| format!("failed to persist P2P documents: {}", e))
    }

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        match peerstore
            .get_p2p_documents()
            .await
            .map_err(|e| format!("failed to load P2P documents: {}", e))?
        {
            Some(data) => serde_json::from_slice(&data)
                .map_err(|e| format!("failed to deserialize P2P documents: {}", e)),
            None => Ok(Vec::new()),
        }
    }

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String> {
        let data = serde_json::to_vec(collections)
            .map_err(|e| format!("failed to serialize P2P collections: {}", e))?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .set_p2p_collections(&data)
            .await
            .map_err(|e| format!("failed to persist P2P collections: {}", e))
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
    event_bus: Option<Arc<dyn events::Bus>>,
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
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

    async fn persist_replicator(
        &self,
        _peer_id: &str,
        _collections: &[String],
    ) -> Result<(), String> {
        Err("persist_replicator not available (no database context)".to_string())
    }

    async fn delete_persisted_replicator(&self, _peer_id: &str) -> Result<(), String> {
        Err("delete_persisted_replicator not available (no database context)".to_string())
    }

    async fn persist_p2p_documents(&self, _doc_ids: &[String]) -> Result<(), String> {
        Err("persist_p2p_documents not available (no database context)".to_string())
    }

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String> {
        Err("load_p2p_documents not available (no database context)".to_string())
    }

    async fn persist_p2p_collections(&self, _collections: &[String]) -> Result<(), String> {
        Err("persist_p2p_collections not available (no database context)".to_string())
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
        let connected = self
            .handle
            .connected_peers()
            .await
            .map_err(|e| e.to_string())?;

        let mut host_addrs = Vec::new();
        let mut covered = HashSet::new();

        // Retry peer_addresses() up to 5 times (matching FFI behavior)
        for attempt in 0..5 {
            host_addrs = self
                .handle
                .peer_addresses()
                .await
                .map_err(|e| format!("failed to get peer addresses: {}", e))?;

            covered.clear();
            for addr_str in &host_addrs {
                if let Some(pid) = addr_str.rsplit("/p2p/").next() {
                    covered.insert(pid.to_string());
                }
            }

            let all_resolved = {
                let cached = self.peer_addresses.read().ok();
                connected.iter().all(|pid| {
                    let pid_str = pid.to_string();
                    covered.contains(&pid_str)
                        || cached.as_ref().is_some_and(|c| c.contains_key(&pid_str))
                })
            };

            if all_resolved || attempt == 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Append cached addresses for any unresolved peers
        let mut all_addrs = host_addrs;
        if let Ok(cached) = self.peer_addresses.read() {
            for pid in &connected {
                let pid_str = pid.to_string();
                if !covered.contains(&pid_str) {
                    if let Some(cached_addr) = cached.get(&pid_str) {
                        all_addrs.push(cached_addr.clone());
                    }
                }
            }
        }

        Ok(all_addrs)
    }

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        let (peer_id, full_multiaddr) = parse_peer_id_from_multiaddr(addr)?;

        let dial_addr: libp2p::Multiaddr = full_multiaddr
            .iter()
            .filter(|proto| !matches!(proto, libp2p::multiaddr::Protocol::P2p(_)))
            .collect();

        self.handle
            .dial(peer_id, vec![dial_addr])
            .await
            .map_err(|e| e.to_string())?;

        // Poll until connected (matching FFI: 50ms intervals, 10s timeout)
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(connected) = self.handle.connected_peers().await {
                if connected.contains(&peer_id) {
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("connection timed out waiting for peer".to_string());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Cache the full multiaddr for connected_peers resolution
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr.to_string());
        }

        Ok(())
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
        let (peer_id, full_multiaddr) = parse_peer_id_from_multiaddr(addr_str)?;

        // Strip /p2p/ component to get transport address for dialing
        let dial_addr: libp2p::Multiaddr = full_multiaddr
            .iter()
            .filter(|proto| !matches!(proto, libp2p::multiaddr::Protocol::P2p(_)))
            .collect();

        // If collections empty, replicate all (matching Go behavior)
        let effective_collections = if collections.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                pusher.list_collections()?
            } else {
                return Err("no database context to list collections".to_string());
            }
        } else {
            collections
        };

        // Resolve collection names → CIDs
        let mut collection_cids = Vec::new();
        if let Some(ref pusher) = self.doc_pusher {
            for name in &effective_collections {
                if let Some(cid) = pusher.get_collection_id(name) {
                    collection_cids.push(cid);
                } else {
                    return Err(format!("collection '{}' not found", name));
                }
            }
        } else {
            collection_cids.clone_from(&effective_collections);
        }

        // Dial peer
        self.handle
            .dial(peer_id, vec![dial_addr])
            .await
            .map_err(|e| format!("failed to connect to replicator peer: {}", e))?;

        // Cache peer address for connected_peers resolution
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        // Register replicator (coordinator handles topic auto-subscribe)
        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .set_replicator(peer_id, collection_cids.clone(), true)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            self.handle
                .set_replicator(peer_id, collection_cids.clone())
                .await
                .map_err(|e| e.to_string())?;
        }

        // Persist to peerstore (best-effort, log warning on failure)
        if let Some(ref pusher) = self.doc_pusher {
            if let Err(e) = pusher
                .persist_replicator(&peer_id.to_string(), &collection_cids)
                .await
            {
                tracing::warn!(peer_id = %peer_id, error = %e, "failed to persist replicator");
            }
        }

        // Spawn background task: push existing docs → emit ReplicatorCompleted
        if let Some(ref pusher) = self.doc_pusher {
            let push_handle = self.handle.clone();
            let push_pusher = Arc::clone(pusher);
            let push_event_bus = self.event_bus.clone();
            let push_collections = effective_collections;

            tokio::spawn(async move {
                if let Err(e) = push_pusher
                    .push_existing_docs(&push_handle, peer_id, &push_collections, None)
                    .await
                {
                    tracing::error!(error = %e, "Failed to push existing docs to replicator");
                }
                if let Some(bus) = push_event_bus {
                    tracing::debug!("publishing ReplicatorCompleted event");
                    bus.publish(events::Message::replicator_completed());
                    tracing::debug!("ReplicatorCompleted event published");
                }
            });
        } else if let Some(ref bus) = self.event_bus {
            bus.publish(events::Message::replicator_completed());
        }

        Ok(())
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

        // Delete from peerstore (best-effort, log warning on failure)
        if let Some(ref pusher) = self.doc_pusher {
            if let Err(e) = pusher
                .delete_persisted_replicator(&peer_id.to_string())
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %e,
                    "Failed to delete replicator from storage"
                );
            }
        }

        // Emit completion event
        if let Some(ref bus) = self.event_bus {
            bus.publish(events::Message::replicator_completed());
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

            // Persist all subscribed collections (best-effort)
            if let Some(ref pusher) = self.doc_pusher {
                let all_cols = coordinator
                    .get_subscribed_collections()
                    .await
                    .map_err(|e| e.to_string())?;
                if let Err(e) = pusher.persist_p2p_collections(&all_cols).await {
                    tracing::warn!(error = %e, "failed to persist P2P collections");
                }
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
        let docs = self
            .tracked_documents
            .read()
            .map_err(|e| format!("failed to read tracked documents: {}", e))?;
        let mut sorted: Vec<String> = docs.iter().cloned().collect();
        sorted.sort();
        Ok(sorted
            .into_iter()
            .map(|doc_id| defra_http::router::P2pDocumentInfo {
                collection: String::new(),
                doc_id,
            })
            .collect())
    }

    async fn add_documents(
        &self,
        docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> Result<(), String> {
        let doc_ids: Vec<String> = docs.into_iter().map(|d| d.doc_id).collect();

        // Validate all doc IDs atomically
        for doc_id in &doc_ids {
            if document::DocID::from_string(doc_id).is_err() {
                return Err("malformed document ID, missing either version or cid".to_string());
            }
        }

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(e) = self.handle.subscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %e, "Failed to subscribe to GossipSub topic for document");
            }
            if let Ok(mut tracked) = self.tracked_documents.write() {
                tracked.insert(doc_id.clone());
            }
        }

        // Persist all tracked documents (best-effort)
        if let Some(ref pusher) = self.doc_pusher {
            let all_docs: Vec<String> = self
                .tracked_documents
                .read()
                .map(|docs| docs.iter().cloned().collect())
                .unwrap_or_default();
            if let Err(e) = pusher.persist_p2p_documents(&all_docs).await {
                tracing::warn!(error = %e, "failed to persist P2P documents");
            }
        }

        Ok(())
    }

    async fn remove_documents(
        &self,
        docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> Result<(), String> {
        let doc_ids: Vec<String> = docs.into_iter().map(|d| d.doc_id).collect();

        // Validate all doc IDs atomically
        for doc_id in &doc_ids {
            if document::DocID::from_string(doc_id).is_err() {
                return Err("malformed document ID, missing either version or cid".to_string());
            }
        }

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(e) = self.handle.unsubscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %e, "Failed to unsubscribe from GossipSub topic for document");
            }
            if let Ok(mut tracked) = self.tracked_documents.write() {
                tracked.remove(doc_id);
            }
        }

        Ok(())
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
