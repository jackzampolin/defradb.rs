use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::libp2p_doc_pusher::DocPusher;
use crate::{
    P2PError, P2POperations, P2PResult, P2pDocumentInfo, P2pDocumentRequest, ReplicatorInfo,
    ReplicatorPushOptions,
};

use p2p::sync::Libp2pSyncCoordinator;
use p2p::topics::DefraTopic;
use p2p::P2PHostHandle;

/// Trait for looking up collection IDs by name.
pub trait CollectionLookup: Send + Sync {
    fn get_collection_id(&self, name: &str) -> Option<String>;
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
    async fn resubscribe_tracked_document_topics(&self) {
        let doc_ids: Vec<String> = match self.tracked_documents.read() {
            Ok(docs) => docs.iter().cloned().collect(),
            Err(error) => {
                tracing::warn!(error = %error, "failed to read tracked documents");
                return;
            }
        };
        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(error) = self.handle.unsubscribe(topic.clone()).await {
                tracing::debug!(doc_id = %doc_id, error = %error, "failed to drop tracked document topic before reconnect resubscribe");
            }
            if let Err(error) = self.handle.subscribe(topic).await {
                tracing::debug!(doc_id = %doc_id, error = %error, "failed to resubscribe tracked document topic after reconnect");
            }
        }
    }

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
    async fn local_peer_id(&self) -> P2PResult<String> {
        self.handle
            .local_peer_id()
            .await
            .map(|id| id.to_string())
            .map_err(|error| P2PError::transport(error.to_string()))
    }

    async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
        self.handle
            .listen_addresses()
            .await
            .map(|addrs| addrs.into_iter().map(|addr| addr.to_string()).collect())
            .map_err(|error| P2PError::transport(error.to_string()))
    }

    async fn connected_peers(&self) -> P2PResult<Vec<String>> {
        let connected = self
            .handle
            .connected_peers()
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;
        self.handle
            .resolve_peer_addresses(&connected, |peer_id| {
                self.peer_addresses.read().ok()?.get(peer_id).cloned()
            })
            .await
            .map_err(|error| P2PError::transport(error.to_string()))
    }

    async fn connect_peer(&self, addr: &str) -> P2PResult<()> {
        let parsed = p2p::parse_multiaddr_with_peer_id(addr)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;
        let already_connected = self
            .handle
            .connected_peers()
            .await
            .map(|peers| peers.contains(&parsed.peer_id))
            .unwrap_or(false);
        if already_connected {
            if let Ok(mut addrs) = self.peer_addresses.write() {
                addrs.insert(parsed.peer_id.to_string(), addr.to_string());
            }
            return Ok(());
        }
        self.handle
            .dial(parsed.peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;
        self.handle
            .poll_until_connected(parsed.peer_id, std::time::Duration::from_secs(10))
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(parsed.peer_id.to_string(), addr.to_string());
        }
        self.resubscribe_tracked_document_topics().await;
        Ok(())
    }

    async fn notify_network_change(&self) -> P2PResult<()> {
        Ok(())
    }

    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
        let p2p_infos = self
            .handle
            .list_replicators()
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;

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
    ) -> P2PResult<()> {
        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let parsed = p2p::parse_multiaddr_with_peer_id(addr_str)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;

        let effective_collections = if collections.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                pusher.list_collections().map_err(P2PError::from)?
            } else {
                return Err(P2PError::unsupported(
                    "no database context to list collections",
                ));
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
                    return Err(P2PError::not_found(format!(
                        "collection '{name}' not found"
                    )));
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
                    .map_err(|error| P2PError::transport(error.to_string()))
            } else {
                self.handle
                    .get_replicator(peer_id)
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))
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
            .map_err(|error| {
                P2PError::transport(format!("failed to connect to replicator peer: {error}"))
            })?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        if let Some(ref coordinator) = self.sync_coordinator {
            let transport_peer_id = p2p::transport::PeerId::from(peer_id);
            coordinator
                .create_replicator(&transport_peer_id, collection_cids.clone(), true)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
        } else {
            self.handle
                .create_replicator(peer_id, collection_cids.clone())
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
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
    ) -> P2PResult<()> {
        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let peer_id = match p2p::parse_multiaddr_with_peer_id(addr_str) {
            Ok(parsed) => parsed.peer_id,
            Err(_) => addr_str.parse::<libp2p::PeerId>().map_err(|error| {
                P2PError::invalid_input(format!("invalid peer ID '{}': {}", addr_str, error))
            })?,
        };

        let fully_deleted = if let Some(ref coordinator) = self.sync_coordinator {
            let transport_peer_id = p2p::transport::PeerId::from(peer_id);
            coordinator
                .remove_replicator_collections(&transport_peer_id, collections)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        } else {
            self.handle
                .remove_replicator_collections(peer_id, collections)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
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
                    .map_err(|error| P2PError::transport(error.to_string()))?;
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

    async fn retry_replicators(&self, push_options: ReplicatorPushOptions) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context to retry replicators"))?;
        let collections = pusher.list_collections().map_err(P2PError::from)?;
        let replicators =
            self.handle.list_replicators().await.map_err(|error| {
                P2PError::transport(format!("failed to get replicators: {error}"))
            })?;

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

    async fn get_collections(&self) -> P2PResult<Vec<String>> {
        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .get_subscribed_collections()
                .await
                .map_err(|error| P2PError::transport(error.to_string()))
        } else {
            Ok(Vec::new())
        }
    }

    async fn add_collections(&self, collections: Vec<String>) -> P2PResult<()> {
        if let Some(ref coordinator) = self.sync_coordinator {
            for collection_name in collections {
                let topic_id = if let Some(ref pusher) = self.doc_pusher {
                    if let Some(collection_id) = pusher.get_collection_id(&collection_name) {
                        collection_id
                    } else {
                        return Err(P2PError::not_found(format!(
                            "collection '{}' not found - add schema before subscribing to P2P",
                            collection_name
                        )));
                    }
                } else {
                    collection_name.clone()
                };

                coordinator
                    .subscribe_collection(&topic_id)
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))?;
            }

            if let Some(ref pusher) = self.doc_pusher {
                let all_cols = coordinator
                    .get_subscribed_collections()
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))?;
                if let Err(error) = pusher.persist_p2p_collections(&all_cols).await {
                    tracing::warn!(error = %error, "failed to persist P2P collections");
                }
            }

            Ok(())
        } else {
            Err(P2PError::unsupported(
                "p2p collections functionality requires sync coordinator",
            ))
        }
    }

    async fn remove_collections(&self, collections: Vec<String>) -> P2PResult<()> {
        if let Some(ref coordinator) = self.sync_coordinator {
            let topic_ids = collections
                .into_iter()
                .map(|collection_name| {
                    if let Some(ref pusher) = self.doc_pusher {
                        pusher.get_collection_id(&collection_name).ok_or_else(|| {
                            P2PError::not_found(format!(
                                "collection '{}' not found",
                                collection_name
                            ))
                        })
                    } else {
                        Ok(collection_name)
                    }
                })
                .collect::<P2PResult<Vec<_>>>()?;

            for topic_id in topic_ids {
                coordinator
                    .unsubscribe_collection(&topic_id)
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))?;
            }

            if let Some(ref pusher) = self.doc_pusher {
                let all_cols = coordinator
                    .get_subscribed_collections()
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))?;
                if let Err(error) = pusher.persist_p2p_collections(&all_cols).await {
                    tracing::warn!(error = %error, "failed to persist P2P collections after removal");
                }
            }

            Ok(())
        } else {
            Err(P2PError::unsupported(
                "p2p collections functionality requires sync coordinator",
            ))
        }
    }

    async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>> {
        let docs = self.tracked_documents.read().map_err(|error| {
            P2PError::internal(format!("failed to read tracked documents: {error}"))
        })?;
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

    async fn add_documents(&self, docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
        let doc_ids: Vec<String> = docs.into_iter().map(|doc| doc.doc_id).collect();
        document::validate_doc_ids(&doc_ids).map_err(|_| {
            P2PError::invalid_input("malformed document ID, missing either version or cid")
        })?;

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

    async fn remove_documents(&self, docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
        let doc_ids: Vec<String> = docs.into_iter().map(|doc| doc.doc_id).collect();
        document::validate_doc_ids(&doc_ids).map_err(|_| {
            P2PError::invalid_input("malformed document ID, missing either version or cid")
        })?;

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

    async fn republish_document(&self, collection_name: &str, doc_id: &str) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for republish"))?;
        let coordinator = self
            .sync_coordinator
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no sync coordinator for republish"))?;
        pusher
            .validate_collection_exists(collection_name)
            .map_err(P2PError::from)?;
        let collection_id = pusher.get_collection_id(collection_name).ok_or_else(|| {
            P2PError::not_found(format!("collection '{collection_name}' not found"))
        })?;
        let head_blocks = pusher
            .load_document_head_blocks(doc_id)
            .await
            .map_err(P2PError::from)?;

        for (cid, block) in head_blocks {
            coordinator
                .broadcast_local_update(&cid, &block, doc_id, &collection_id)
                .await
                .map_err(|error| {
                    P2PError::transport(format!("failed to republish document head {cid}: {error}"))
                })?;
        }

        Ok(())
    }

    async fn sync_documents(&self, collection_name: &str, doc_ids: Vec<String>) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for sync"))?;
        pusher
            .validate_collection_exists(collection_name)
            .map_err(P2PError::from)?;

        let event_bus = self
            .event_bus
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no event bus for sync"))?;

        let connected_peers = self.handle.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
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
                return Err(P2PError::internal(format!(
                    "failed to sign DocSync request: {error}"
                )));
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

    async fn sync_branchable_collection(&self, collection_id: &str) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for sync"))?;
        pusher
            .validate_branchable_collection(collection_id)
            .map_err(P2PError::from)?;

        let connected_peers = self.handle.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_message(self.handle.keypair(), &mut request).map_err(|error| {
            P2PError::internal(format!("failed to sign BranchableSync request: {error}"))
        })?;

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

    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> P2PResult<()> {
        if version_ids.is_empty() {
            return Ok(());
        }
        for version_id in &version_ids {
            cid::Cid::try_from(version_id.as_str())
                .map_err(|error| P2PError::invalid_input(format!("invalid cid: {error}")))?;
        }

        let connected_peers = self.handle.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let syncer = self
            .version_syncer
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("version syncer required"))?;
        syncer
            .sync_versions(&self.handle, version_ids, connected_peers)
            .await
            .map_err(P2PError::from)
    }
}
