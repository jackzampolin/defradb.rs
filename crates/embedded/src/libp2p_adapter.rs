use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::{
    P2POperations, P2pDocumentInfo, P2pDocumentRequest, ReplicatorInfo, ReplicatorPushOptions,
};

use p2p::sync::Libp2pSyncCoordinator;
use p2p::topics::DefraTopic;
use p2p::P2PHostHandle;

/// Trait for looking up collection IDs by name.
pub trait CollectionLookup: Send + Sync {
    fn get_collection_id(&self, name: &str) -> Option<String>;
}

/// Type-erased interface for libp2p-backed document push operations.
#[async_trait]
pub trait DocPusher: Send + Sync {
    async fn push_existing_docs(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
        se_identity_pubkey: Option<&[u8]>,
    ) -> Result<(), String>;

    fn get_collection_id(&self, name: &str) -> Option<String>;

    fn list_collections(&self) -> Result<Vec<String>, String>;

    async fn persist_replicator(&self, peer_id: &str, collections: &[String])
        -> Result<(), String>;

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String>;

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String>;

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String>;

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String>;

    fn validate_collection_exists(&self, name: &str) -> Result<(), String>;

    fn validate_branchable_collection(&self, collection_id: &str) -> Result<(), String>;

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String>;
}

/// Database-backed `DocPusher` implementation.
pub struct DbDocPusher<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
    document_acp: std::sync::OnceLock<Arc<dyn acp::DocumentACP>>,
}

impl<S: storage::corekv::Store + 'static> DbDocPusher<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self {
            db,
            document_acp: std::sync::OnceLock::new(),
        }
    }

    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn DocPusher> {
        Arc::new(Self::new(db))
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
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
        se_identity_pubkey: Option<&[u8]>,
    ) -> Result<(), String> {
        db::push_existing_docs(
            handle,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            collections,
            se_key,
            se_identity_pubkey,
        )
        .await
    }

    fn get_collection_id(&self, name: &str) -> Option<String> {
        match self.db.get_collection(name) {
            Ok(Some(collection)) => Some(collection.collection_id().to_string()),
            Ok(None) => {
                tracing::debug!(collection_name = %name, "collection not found for P2P lookup");
                None
            }
            Err(error) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %error,
                    "error looking up collection for P2P"
                );
                None
            }
        }
    }

    fn list_collections(&self) -> Result<Vec<String>, String> {
        self.db
            .list_collections()
            .map_err(|error| format!("failed to list collections: {error}"))
    }

    async fn persist_replicator(
        &self,
        peer_id: &str,
        collections: &[String],
    ) -> Result<(), String> {
        let parsed_peer_id: libp2p::PeerId = peer_id
            .parse()
            .map_err(|error| format!("invalid peer ID: {error}"))?;
        let info = p2p::ReplicatorInfo::new(parsed_peer_id, collections.to_vec());
        let bytes = info
            .to_bytes()
            .map_err(|error| format!("failed to serialize replicator info: {error}"))?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .create_replicator(peer_id, &bytes)
            .await
            .map_err(|error| format!("failed to persist replicator: {error}"))
    }

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .delete_replicator(peer_id)
            .await
            .map_err(|error| format!("failed to delete persisted replicator: {error}"))
    }

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_documents(doc_ids)
            .await
            .map_err(|error| format!("failed to persist P2P documents: {error}"))
    }

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .load_documents()
            .await
            .map_err(|error| format!("failed to load P2P documents: {error}"))
    }

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_collections(collections)
            .await
            .map_err(|error| format!("failed to persist P2P collections: {error}"))
    }

    fn validate_collection_exists(&self, name: &str) -> Result<(), String> {
        self.db
            .require_collection(name)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn validate_branchable_collection(&self, collection_id: &str) -> Result<(), String> {
        match self.db.find_collection_by_id(collection_id) {
            Ok(Some(collection)) => {
                if !collection.schema().is_branchable {
                    Err("collection is not branchable".to_string())
                } else {
                    Ok(())
                }
            }
            Ok(None) => Err(format!("collection with ID '{collection_id}' not found")),
            Err(error) => Err(format!("failed to find collection: {error}")),
        }
    }

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String> {
        db::retry_doc(
            handle,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            doc_id,
            collection_id,
        )
        .await
    }
}

impl<S: storage::corekv::Store + 'static> CollectionLookup for DbDocPusher<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        DocPusher::get_collection_id(self, name)
    }
}

/// Trait for syncing collection versions via Bitswap.
#[async_trait]
pub trait VersionSyncer: Send + Sync {
    async fn sync_versions(
        &self,
        handle: &P2PHostHandle,
        version_ids: Vec<String>,
        connected_peers: Vec<libp2p::PeerId>,
    ) -> Result<(), String>;
}

/// Adapter implementing embedded P2P operations on top of `P2PHostHandle`.
pub struct P2PAdapter<B: Blockstore + 'static> {
    handle: P2PHostHandle,
    sync_coordinator: Option<Arc<Libp2pSyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn DocPusher>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    version_syncer: Option<Arc<dyn VersionSyncer>>,
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
    tracked_documents: Arc<std::sync::RwLock<HashSet<String>>>,
}

impl<B: Blockstore + 'static> P2PAdapter<B> {
    pub fn with_full_context(
        handle: P2PHostHandle,
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
        doc_pusher: Arc<dyn DocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn VersionSyncer>>,
    ) -> Self {
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            doc_pusher: Some(doc_pusher),
            event_bus: Some(event_bus),
            version_syncer,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    pub fn with_full_context_arc(
        handle: P2PHostHandle,
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
        doc_pusher: Arc<dyn DocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn VersionSyncer>>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_full_context(
            handle,
            coordinator,
            doc_pusher,
            event_bus,
            version_syncer,
        ))
    }

    pub fn set_initial_tracked_documents(&self, docs: HashSet<String>) {
        if let Ok(mut tracked) = self.tracked_documents.write() {
            *tracked = docs;
        }
    }
}

#[async_trait]
impl<B: Blockstore + 'static> P2POperations for P2PAdapter<B> {
    async fn local_peer_id(&self) -> Result<String, String> {
        self.handle
            .local_peer_id()
            .await
            .map(|id| id.to_string())
            .map_err(|error| error.to_string())
    }

    async fn listen_addresses(&self) -> Result<Vec<String>, String> {
        self.handle
            .listen_addresses()
            .await
            .map(|addrs| addrs.into_iter().map(|addr| addr.to_string()).collect())
            .map_err(|error| error.to_string())
    }

    async fn connected_peers(&self) -> Result<Vec<String>, String> {
        let connected = self
            .handle
            .connected_peers()
            .await
            .map_err(|error| error.to_string())?;
        self.handle
            .resolve_peer_addresses(&connected, |peer_id| {
                self.peer_addresses.read().ok()?.get(peer_id).cloned()
            })
            .await
            .map_err(|error| error.to_string())
    }

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        let parsed = p2p::parse_multiaddr_with_peer_id(addr)?;
        self.handle
            .dial(parsed.peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|error| error.to_string())?;
        self.handle
            .poll_until_connected(parsed.peer_id, std::time::Duration::from_secs(10))
            .await
            .map_err(|error| error.to_string())?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(parsed.peer_id.to_string(), addr.to_string());
        }
        Ok(())
    }

    async fn notify_network_change(&self) -> Result<(), String> {
        Ok(())
    }

    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String> {
        let p2p_infos = self
            .handle
            .list_replicators()
            .await
            .map_err(|error| error.to_string())?;

        Ok(p2p_infos
            .into_iter()
            .map(|info| {
                let address = info.addresses_str().first().map(|addr| addr.to_string());
                ReplicatorInfo {
                    id: Some(info.peer_id_str().to_string()),
                    collections: info.collections,
                    address,
                }
            })
            .collect())
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        push_options: ReplicatorPushOptions,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let parsed = p2p::parse_multiaddr_with_peer_id(addr_str)?;

        let effective_collections = if collections.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                pusher.list_collections()?
            } else {
                return Err("no database context to list collections".to_string());
            }
        } else {
            collections
        };

        let mut collection_cids = Vec::new();
        if let Some(ref pusher) = self.doc_pusher {
            for name in &effective_collections {
                if let Some(cid) = pusher.get_collection_id(name) {
                    collection_cids.push(cid);
                } else {
                    return Err(format!("collection '{name}' not found"));
                }
            }
        } else {
            collection_cids.clone_from(&effective_collections);
        }

        let peer_id = parsed.peer_id;

        // Check existing replicator state before creating/updating so we can
        // skip the expensive initial replay when the replicator already exists
        // with the same collections (idempotent reconnect path).
        let existing_collection_ids: HashSet<String> = {
            let result = if let Some(ref coordinator) = self.sync_coordinator {
                let transport_peer_id = p2p::transport::PeerId::from(peer_id);
                coordinator
                    .get_replicator(&transport_peer_id)
                    .await
                    .map_err(|e| e.to_string())
            } else {
                self.handle
                    .get_replicator(peer_id)
                    .await
                    .map_err(|e| e.to_string())
            };
            match result {
                Ok(Some(info)) => info.collections.into_iter().collect(),
                Ok(None) => HashSet::new(),
                Err(e) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %e,
                        "Failed to check existing replicator state; falling back to full replay"
                    );
                    HashSet::new()
                }
            }
        };

        self.handle
            .dial(peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|error| format!("failed to connect to replicator peer: {error}"))?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        if let Some(ref coordinator) = self.sync_coordinator {
            let transport_peer_id = p2p::transport::PeerId::from(peer_id);
            coordinator
                .create_replicator(&transport_peer_id, collection_cids.clone(), true)
                .await
                .map_err(|error| error.to_string())?;
        } else {
            self.handle
                .create_replicator(peer_id, collection_cids.clone())
                .await
                .map_err(|error| error.to_string())?;
        }

        if let Some(ref pusher) = self.doc_pusher {
            if let Err(error) = pusher
                .persist_replicator(&peer_id.to_string(), &collection_cids)
                .await
            {
                tracing::warn!(peer_id = %peer_id, error = %error, "failed to persist replicator");
            }
        }

        // Only replay collections that weren't already replicated by this peer.
        let new_collection_names: Vec<String> = effective_collections
            .iter()
            .zip(collection_cids.iter())
            .filter(|(_, cid)| !existing_collection_ids.contains(*cid))
            .map(|(name, _)| name.clone())
            .collect();

        if !new_collection_names.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                let push_handle = self.handle.clone();
                let push_pusher = Arc::clone(pusher);
                let push_event_bus = self.event_bus.clone();
                let push_se_key = push_options.se_encryption_key;
                let push_identity = push_options.se_identity_pubkey;

                tracing::info!(
                    peer_id = %peer_id,
                    new_collections = ?new_collection_names,
                    "Replaying existing docs for new collections only"
                );

                tokio::spawn(async move {
                    if let Err(error) = push_pusher
                        .push_existing_docs(
                            &push_handle,
                            peer_id,
                            &new_collection_names,
                            push_se_key.as_deref(),
                            push_identity.as_deref(),
                        )
                        .await
                    {
                        tracing::error!(error = %error, "Failed to push existing docs to replicator");
                    }
                    if let Some(bus) = push_event_bus {
                        bus.publish(events::Message::replicator_completed());
                    }
                });
            } else if let Some(ref bus) = self.event_bus {
                bus.publish(events::Message::replicator_completed());
            }
        } else {
            tracing::debug!(
                peer_id = %peer_id,
                "Replicator already exists with same collections, skipping initial replay"
            );
            if let Some(ref bus) = self.event_bus {
                bus.publish(events::Message::replicator_completed());
            }
        }

        Ok(())
    }

    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let peer_id = match p2p::parse_multiaddr_with_peer_id(addr_str) {
            Ok(parsed) => parsed.peer_id,
            Err(_) => addr_str
                .parse::<libp2p::PeerId>()
                .map_err(|error| format!("invalid peer ID '{}': {}", addr_str, error))?,
        };

        let fully_deleted = if let Some(ref coordinator) = self.sync_coordinator {
            let transport_peer_id = p2p::transport::PeerId::from(peer_id);
            coordinator
                .remove_replicator_collections(&transport_peer_id, collections)
                .await
                .map_err(|error| error.to_string())?
        } else {
            self.handle
                .remove_replicator_collections(peer_id, collections)
                .await
                .map_err(|error| error.to_string())?
        };

        if let Some(ref pusher) = self.doc_pusher {
            if fully_deleted {
                if let Err(error) = pusher
                    .delete_persisted_replicator(&peer_id.to_string())
                    .await
                {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %error,
                        "failed to delete replicator from storage"
                    );
                }
            } else {
                let remaining = self
                    .handle
                    .get_replicator(peer_id)
                    .await
                    .map_err(|error| error.to_string())?;
                if let Some(info) = remaining {
                    if let Err(error) = pusher
                        .persist_replicator(&peer_id.to_string(), &info.collections)
                        .await
                    {
                        tracing::warn!(
                            peer_id = %peer_id,
                            error = %error,
                            "failed to update persisted replicator"
                        );
                    }
                }
            }
        }

        if let Some(ref bus) = self.event_bus {
            bus.publish(events::Message::replicator_completed());
        }

        Ok(())
    }

    async fn retry_replicators(&self, push_options: ReplicatorPushOptions) -> Result<(), String> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| "no database context to retry replicators".to_string())?;
        let collections = pusher.list_collections()?;
        let replicators = self
            .handle
            .list_replicators()
            .await
            .map_err(|error| format!("failed to get replicators: {error}"))?;

        let mut push_handles = Vec::new();
        for replicator in replicators {
            let Some(peer_id) = replicator.peer_id() else {
                continue;
            };

            let push_handle = self.handle.clone();
            let push_pusher = Arc::clone(pusher);
            let push_collections = collections.clone();
            let push_se_key = push_options.se_encryption_key.clone();
            let push_identity = push_options.se_identity_pubkey.clone();
            push_handles.push(tokio::spawn(async move {
                if let Err(error) = push_pusher
                    .push_existing_docs(
                        &push_handle,
                        peer_id,
                        &push_collections,
                        push_se_key.as_deref(),
                        push_identity.as_deref(),
                    )
                    .await
                {
                    tracing::error!(
                        peer_id = %peer_id,
                        error = %error,
                        "Failed to retry push existing docs to replicator"
                    );
                }
            }));
        }

        for handle in push_handles {
            let _ = handle.await;
        }

        Ok(())
    }

    async fn get_collections(&self) -> Result<Vec<String>, String> {
        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .get_subscribed_collections()
                .await
                .map_err(|error| error.to_string())
        } else {
            Ok(Vec::new())
        }
    }

    async fn add_collections(&self, collections: Vec<String>) -> Result<(), String> {
        if let Some(ref coordinator) = self.sync_coordinator {
            for collection_name in collections {
                let topic_id = if let Some(ref pusher) = self.doc_pusher {
                    if let Some(collection_id) = pusher.get_collection_id(&collection_name) {
                        collection_id
                    } else {
                        return Err(format!(
                            "collection '{}' not found - add schema before subscribing to P2P",
                            collection_name
                        ));
                    }
                } else {
                    collection_name.clone()
                };

                coordinator
                    .subscribe_collection(&topic_id)
                    .await
                    .map_err(|error| error.to_string())?;
            }

            if let Some(ref pusher) = self.doc_pusher {
                let all_cols = coordinator
                    .get_subscribed_collections()
                    .await
                    .map_err(|error| error.to_string())?;
                if let Err(error) = pusher.persist_p2p_collections(&all_cols).await {
                    tracing::warn!(error = %error, "failed to persist P2P collections");
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
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        } else {
            Err("p2p collections functionality requires sync coordinator".to_string())
        }
    }

    async fn get_documents(&self) -> Result<Vec<P2pDocumentInfo>, String> {
        let docs = self
            .tracked_documents
            .read()
            .map_err(|error| format!("failed to read tracked documents: {error}"))?;
        let mut sorted: Vec<String> = docs.iter().cloned().collect();
        sorted.sort();
        Ok(sorted
            .into_iter()
            .map(|doc_id| P2pDocumentInfo {
                collection: String::new(),
                doc_id,
            })
            .collect())
    }

    async fn add_documents(&self, docs: Vec<P2pDocumentRequest>) -> Result<(), String> {
        let doc_ids: Vec<String> = docs.into_iter().map(|doc| doc.doc_id).collect();
        document::validate_doc_ids(&doc_ids)
            .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(error) = self.handle.subscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %error, "Failed to subscribe to topic for document");
            }
            if let Ok(mut tracked) = self.tracked_documents.write() {
                tracked.insert(doc_id.clone());
            }
        }

        if let Some(ref pusher) = self.doc_pusher {
            let all_docs: Vec<String> = self
                .tracked_documents
                .read()
                .map(|docs| docs.iter().cloned().collect())
                .unwrap_or_default();
            if let Err(error) = pusher.persist_p2p_documents(&all_docs).await {
                tracing::warn!(error = %error, "failed to persist P2P documents");
            }
        }

        Ok(())
    }

    async fn remove_documents(&self, docs: Vec<P2pDocumentRequest>) -> Result<(), String> {
        let doc_ids: Vec<String> = docs.into_iter().map(|doc| doc.doc_id).collect();
        document::validate_doc_ids(&doc_ids)
            .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(error) = self.handle.unsubscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %error, "Failed to unsubscribe from topic for document");
            }
            if let Ok(mut tracked) = self.tracked_documents.write() {
                tracked.remove(doc_id);
            }
        }

        Ok(())
    }

    async fn sync_documents(
        &self,
        collection_name: &str,
        doc_ids: Vec<String>,
    ) -> Result<(), String> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| "no database context for sync".to_string())?;
        pusher.validate_collection_exists(collection_name)?;

        let event_bus = self
            .event_bus
            .as_ref()
            .ok_or_else(|| "no event bus for sync".to_string())?;

        let connected_peers = self
            .handle
            .connected_peers()
            .await
            .map_err(|error| format!("failed to get connected peers: {error}"))?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut sub = event_bus.subscribe(&[events::EventName::MergeComplete]);
        let total_expected = connected_peers.len() * doc_ids.len();
        let mut total_received = 0;
        let overall_timeout = std::time::Duration::from_secs(30);
        let idle_timeout = std::time::Duration::from_secs(3);
        let start = std::time::Instant::now();
        let doc_set: HashSet<String> = doc_ids.iter().cloned().collect();

        for _attempt in 0..3 {
            if total_received >= total_expected || start.elapsed() >= overall_timeout {
                break;
            }

            let mut request = p2p::message::DocSyncRequest::new(doc_ids.clone());
            if let Err(error) = p2p::signing::sign_message(self.handle.keypair(), &mut request) {
                event_bus.unsubscribe(sub.id());
                return Err(format!("failed to sign DocSync request: {error}"));
            }

            for peer_id in &connected_peers {
                if let Err(error) = self
                    .handle
                    .send_doc_sync_request(*peer_id, request.clone())
                    .await
                {
                    tracing::warn!(peer_id = %peer_id, error = %error, "failed to send DocSync request");
                }
            }

            let mut last_merge = std::time::Instant::now();
            while total_received < total_expected && start.elapsed() < overall_timeout {
                if total_received >= doc_ids.len() && last_merge.elapsed() > idle_timeout {
                    break;
                }

                match tokio::time::timeout(std::time::Duration::from_millis(100), sub.recv()).await
                {
                    Ok(Some(msg)) => {
                        if let Some(data) = msg.as_merge_complete() {
                            if doc_set.contains(&data.doc_id) {
                                total_received += 1;
                                last_merge = std::time::Instant::now();
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
        }

        event_bus.unsubscribe(sub.id());
        Ok(())
    }

    async fn sync_branchable_collection(&self, collection_id: &str) -> Result<(), String> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| "no database context for sync".to_string())?;
        pusher.validate_branchable_collection(collection_id)?;

        let connected_peers = self
            .handle
            .connected_peers()
            .await
            .map_err(|error| format!("failed to get connected peers: {error}"))?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_message(self.handle.keypair(), &mut request)
            .map_err(|error| format!("failed to sign BranchableSync request: {error}"))?;

        for peer_id in &connected_peers {
            let request_clone = request.clone();
            let handle = self.handle.clone();
            let peer_id = *peer_id;
            tokio::spawn(async move {
                if let Err(error) = handle
                    .send_branchable_sync_request(peer_id, request_clone)
                    .await
                {
                    tracing::warn!(peer_id = %peer_id, error = %error, "failed to send BranchableSyncRequest");
                }
            });
        }

        Ok(())
    }

    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> Result<(), String> {
        if version_ids.is_empty() {
            return Ok(());
        }
        for version_id in &version_ids {
            cid::Cid::try_from(version_id.as_str())
                .map_err(|error| format!("invalid cid: {error}"))?;
        }

        let connected_peers = self
            .handle
            .connected_peers()
            .await
            .map_err(|error| format!("failed to get connected peers: {error}"))?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let syncer = self
            .version_syncer
            .as_ref()
            .ok_or_else(|| "version syncer required".to_string())?;
        syncer
            .sync_versions(&self.handle, version_ids, connected_peers)
            .await
    }
}
