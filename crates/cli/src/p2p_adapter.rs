//! Adapter to bridge P2PHostHandle to HTTP's P2POperations trait.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use defra_http::router::{ExplicitReplayCapabilityInput, P2POperations, ReplicatorInfo};
use p2p::sync::Libp2pSyncCoordinator;
use p2p::topics::DefraTopic;
use p2p::P2PHostHandle;

// Re-export extracted types so existing `crate::p2p_adapter::Foo` paths still resolve.
pub(crate) use crate::p2p_adapter_helpers::collections_requiring_replay;
pub(crate) use crate::p2p_collection_lookup::LookupOnlyDocPusher;
pub use crate::p2p_doc_pusher::DbDocPusher;

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

    /// Validate that a collection with the given name exists.
    fn validate_collection_exists(&self, name: &str) -> Result<(), String>;

    /// Validate that a collection with the given ID exists and is branchable.
    fn validate_branchable_collection(&self, collection_id: &str) -> Result<(), String>;

    /// Retry pushing a single document to a specific peer.
    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String>;
}

/// Trait for syncing collection versions (schema definitions) via Bitswap.
#[async_trait]
pub trait VersionSyncer: Send + Sync {
    async fn sync_versions(
        &self,
        handle: &P2PHostHandle,
        version_ids: Vec<String>,
        connected_peers: Vec<libp2p::PeerId>,
    ) -> Result<(), String>;
}

/// Adapter that implements P2POperations using P2PHostHandle.
///
/// Optionally uses a SyncCoordinator for replicator operations,
/// which enables auto-subscribe to collection topics.
pub struct P2PAdapter<
    B: Blockstore + 'static = blockstore::DefraBlockstore<storage::backends::MemoryStore>,
> {
    handle: P2PHostHandle,
    sync_coordinator: Option<Arc<Libp2pSyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn DocPusher>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    version_syncer: Option<Arc<dyn VersionSyncer>>,
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
            version_syncer: None,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    /// Create a new adapter with a sync coordinator for enhanced replicator support.
    pub fn with_sync_coordinator(
        handle: P2PHostHandle,
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
    ) -> Self {
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            doc_pusher: None,
            event_bus: None,
            version_syncer: None,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    /// Create a new adapter with sync coordinator and collection lookup.
    pub fn with_sync_coordinator_and_lookup(
        handle: P2PHostHandle,
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
        lookup: Arc<dyn CollectionLookup>,
    ) -> Self {
        let doc_pusher: Arc<dyn DocPusher> = Arc::new(LookupOnlyDocPusher(lookup));
        Self {
            handle,
            sync_coordinator: Some(coordinator),
            doc_pusher: Some(doc_pusher),
            event_bus: None,
            version_syncer: None,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    /// Pre-populate tracked documents from persisted state without re-subscribing.
    pub fn set_initial_tracked_documents(&self, docs: HashSet<String>) {
        if let Ok(mut tracked) = self.tracked_documents.write() {
            *tracked = docs;
        }
    }

    /// Create a new adapter with full context for FFI-parity operations.
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
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_sync_coordinator(handle, coordinator))
    }

    /// Create an Arc-wrapped adapter with sync coordinator and collection lookup.
    pub fn with_sync_coordinator_and_lookup_arc(
        handle: P2PHostHandle,
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
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

        let all_addrs = self
            .handle
            .resolve_peer_addresses(&connected, |pid| {
                self.peer_addresses.read().ok()?.get(pid).cloned()
            })
            .await
            .map_err(|e| e.to_string())?;

        Ok(all_addrs)
    }

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        let parsed = p2p::parse_multiaddr_with_peer_id(addr).map_err(|e| e.to_string())?;

        self.handle
            .dial(parsed.peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|e| e.to_string())?;

        self.handle
            .poll_until_connected(parsed.peer_id, std::time::Duration::from_secs(10))
            .await
            .map_err(|e| e.to_string())?;

        // Cache the full multiaddr for connected_peers resolution
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(parsed.peer_id.to_string(), addr.to_string());
        }

        Ok(())
    }

    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String> {
        let p2p_infos = self
            .handle
            .list_replicators()
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
        explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
        expected_authorizer_did: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let parsed = p2p::parse_multiaddr_with_peer_id(addr_str).map_err(|e| e.to_string())?;

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

        // Resolve collection names -> CIDs
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

        let peer_id = parsed.peer_id;
        let requested_collections: HashSet<String> = collection_cids.iter().cloned().collect();
        let local_peer_id = self.handle.local_peer_id_cached().to_string();
        let target_peer_id = peer_id.to_string();
        let mut validated_capabilities = Vec::new();

        if !explicit_replay_capabilities.is_empty() {
            let expected_authorizer_did = expected_authorizer_did.ok_or_else(|| {
                "explicit replay capabilities require an authenticated identity".to_string()
            })?;

            for capability in explicit_replay_capabilities {
                if !requested_collections.contains(&capability.collection_id) {
                    return Err(format!(
                        "explicit replay capability collection '{}' was not requested",
                        capability.collection_id
                    ));
                }

                let authorization = p2p::verify_explicit_replay_capability(
                    &capability.capability,
                    &local_peer_id,
                    &target_peer_id,
                    &capability.collection_id,
                )
                .map_err(|error| {
                    format!(
                        "invalid explicit replay capability for collection '{}': {}",
                        capability.collection_id, error
                    )
                })?;

                if authorization.authorizer_did != expected_authorizer_did {
                    return Err(format!(
                        "explicit replay capability authorizer '{}' did not match authenticated identity '{}'",
                        authorization.authorizer_did, expected_authorizer_did
                    ));
                }

                validated_capabilities.push((capability.collection_id, capability.capability));
            }
        }

        let collections_with_changed_capabilities: HashSet<String> = validated_capabilities
            .iter()
            .filter_map(|(collection_id, capability)| {
                let matches_existing = self.handle.explicit_replay_capability_matches(
                    peer_id,
                    collection_id.as_str(),
                    capability,
                );
                if matches_existing {
                    None
                } else {
                    Some(collection_id.clone())
                }
            })
            .collect();

        self.handle
            .clear_explicit_replay_capability(peer_id, &collection_cids);
        for (collection_id, capability) in validated_capabilities {
            self.handle.set_explicit_replay_capability(
                peer_id,
                std::slice::from_ref(&collection_id),
                &capability,
            );
        }

        // Check existing replicator state before creating/updating so we can
        // skip the expensive initial replay when the replicator already exists
        // with the same collections and replay capability.
        let existing_collection_ids: HashSet<String> = {
            let result = if let Some(ref coordinator) = self.sync_coordinator {
                let transport_pid = p2p::transport::PeerId::from(peer_id);
                coordinator
                    .get_replicator(&transport_pid)
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

        // Dial peer
        self.handle
            .dial(peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|e| format!("failed to connect to replicator peer: {}", e))?;

        // Cache peer address for connected_peers resolution
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        // Register replicator (coordinator handles topic auto-subscribe)
        if let Some(ref coordinator) = self.sync_coordinator {
            let transport_pid = p2p::transport::PeerId::from(peer_id);
            coordinator
                .create_replicator(&transport_pid, collection_cids.clone(), true)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            self.handle
                .create_replicator(peer_id, collection_cids.clone())
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

        // Replay new collections, plus collections whose explicit replay
        // capability changed. The latter case matters for encrypted ACP
        // replay where a previous configuration may have carried an invalid
        // authorizer capability and therefore skipped storing the document.
        let collection_names_requiring_replay = collections_requiring_replay(
            &effective_collections,
            &collection_cids,
            &existing_collection_ids,
            &collections_with_changed_capabilities,
        );

        if !collection_names_requiring_replay.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                let push_handle = self.handle.clone();
                let push_pusher = Arc::clone(pusher);
                let push_event_bus = self.event_bus.clone();

                tracing::info!(
                    peer_id = %peer_id,
                    replay_collections = ?collection_names_requiring_replay,
                    "Replaying existing docs for collections requiring replay"
                );

                tokio::spawn(async move {
                    if let Err(e) = push_pusher
                        .push_existing_docs(
                            &push_handle,
                            peer_id,
                            &collection_names_requiring_replay,
                            None,
                            None,
                        )
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
        } else {
            tracing::debug!(
                peer_id = %peer_id,
                "Replicator already exists with same collections and replay capability, skipping initial replay"
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
        let parsed = p2p::parse_multiaddr_with_peer_id(addr_str).map_err(|e| e.to_string())?;
        let peer_id = parsed.peer_id;

        if let Some(ref coordinator) = self.sync_coordinator {
            let transport_pid = p2p::transport::PeerId::from(peer_id);
            coordinator
                .remove_replicator_collections(&transport_pid, collections)
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

        // Validate all document IDs have valid format (atomic: all or nothing)
        document::validate_doc_ids(&doc_ids)
            .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

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

        // Validate all document IDs have valid format (atomic: all or nothing)
        document::validate_doc_ids(&doc_ids)
            .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

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
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

        if connected_peers.is_empty() {
            return Ok(());
        }

        // Subscribe to MergeComplete events BEFORE sending requests
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
            if let Err(e) = p2p::signing::sign_message(self.handle.keypair(), &mut request) {
                event_bus.unsubscribe(sub.id());
                return Err(format!("failed to sign DocSync request: {}", e));
            }

            let mut any_sent = false;
            for peer_id in &connected_peers {
                match self
                    .handle
                    .send_doc_sync_request(*peer_id, request.clone())
                    .await
                {
                    Ok(()) => any_sent = true,
                    Err(e) => {
                        tracing::warn!(peer_id = %peer_id, error = %e, "failed to send DocSync request")
                    }
                }
            }

            if !any_sent {
                break;
            }

            let mut last_merge = std::time::Instant::now();
            while total_received < total_expected && start.elapsed() < overall_timeout {
                if last_merge.elapsed() > idle_timeout {
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
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_message(self.handle.keypair(), &mut request)
            .map_err(|e| format!("failed to sign BranchableSync request: {}", e))?;

        for peer_id in &connected_peers {
            let request_clone = request.clone();
            let handle = self.handle.clone();
            let peer_id = *peer_id;

            tokio::spawn(async move {
                if let Err(e) = handle
                    .send_branchable_sync_request(peer_id, request_clone)
                    .await
                {
                    tracing::warn!(peer_id = %peer_id, error = %e, "failed to send BranchableSyncRequest");
                }
            });
        }

        Ok(())
    }

    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> Result<(), String> {
        if version_ids.is_empty() {
            return Ok(());
        }

        // Validate all CIDs upfront before attempting sync (matches Go behavior).
        for vid in &version_ids {
            cid::Cid::try_from(vid.as_str()).map_err(|e| format!("invalid cid: {}", e))?;
        }

        let connected_peers = self
            .handle
            .connected_peers()
            .await
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

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
