use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::transport_doc_pusher::TransportDocPusher;
use crate::transport_version_syncer::TransportVersionSyncer;
use crate::{
    ExplicitReplayCapabilityInput, P2PError, P2PErrorExt as _, P2POperations, P2PResult,
    P2pDocumentInfo, P2pDocumentRequest, ReplicatorInfo, ReplicatorPushOptions,
    ReplicatorPushOptionsState,
};

use p2p::iroh::{
    best_shareable_public_addr, format_public_listen_addrs, parse_public_peer_addr, IrohTransport,
};
use p2p::sync::IrohSyncCoordinator;
use p2p::topics::DefraTopic;
use p2p::P2PTransport;

const DOC_SYNC_DISPATCH_PARALLELISM: usize = 16;

/// P2P operations implementation for the iroh transport.
pub struct IrohP2PAdapter<B: Blockstore + 'static> {
    transport: IrohTransport,
    sync_coordinator: Option<Arc<IrohSyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn TransportDocPusher>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
    replicator_push_options: ReplicatorPushOptionsState,
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
    tracked_documents: Arc<std::sync::RwLock<HashSet<String>>>,
    nac_checker: Option<Arc<dyn db::NodeAccessChecker>>,
}

impl<B: Blockstore + 'static> IrohP2PAdapter<B> {
    async fn check_nac(&self, permission: acp::nac::NodePermission) -> P2PResult<()> {
        if let Some(ref checker) = self.nac_checker {
            checker
                .check_node_access(permission)
                .await
                .map_err(|error| P2PError::internal(error.to_string()))?;
        }
        Ok(())
    }

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
            if let Err(error) = self.transport.unsubscribe(topic.clone()).await {
                tracing::debug!(doc_id = %doc_id, error = %error, "failed to drop tracked document topic before reconnect resubscribe");
            }
            if let Err(error) = self.transport.subscribe(topic).await {
                tracing::debug!(doc_id = %doc_id, error = %error, "failed to resubscribe tracked document topic after reconnect");
            }
        }
    }

    pub fn with_full_context(
        transport: IrohTransport,
        coordinator: Arc<IrohSyncCoordinator<B>>,
        doc_pusher: Arc<dyn TransportDocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Self {
        Self {
            transport,
            sync_coordinator: Some(coordinator),
            doc_pusher: Some(doc_pusher),
            event_bus: Some(event_bus),
            version_syncer,
            replicator_push_options: ReplicatorPushOptionsState::default(),
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
            nac_checker: Some(nac_checker),
        }
    }

    pub fn with_replicator_push_options(mut self, options: ReplicatorPushOptions) -> Self {
        self.replicator_push_options = ReplicatorPushOptionsState::new(options);
        self
    }

    pub fn with_replicator_push_options_state(
        mut self,
        options: ReplicatorPushOptionsState,
    ) -> Self {
        self.replicator_push_options = options;
        self
    }

    pub fn with_full_context_arc(
        transport: IrohTransport,
        coordinator: Arc<IrohSyncCoordinator<B>>,
        doc_pusher: Arc<dyn TransportDocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_full_context(
            transport,
            coordinator,
            doc_pusher,
            event_bus,
            version_syncer,
            nac_checker,
        ))
    }

    pub fn set_initial_tracked_documents(&self, docs: HashSet<String>) {
        if let Ok(mut tracked) = self.tracked_documents.write() {
            *tracked = docs;
        }
    }

    async fn send_doc_sync_requests_concurrently(
        &self,
        peers: &[p2p::transport::PeerId],
        request: p2p::message::DocSyncRequest,
    ) -> bool {
        let mut peer_iter = peers.iter().cloned();
        let mut tasks = tokio::task::JoinSet::new();
        let mut any_sent = false;

        loop {
            while tasks.len() < DOC_SYNC_DISPATCH_PARALLELISM {
                let Some(peer) = peer_iter.next() else {
                    break;
                };
                let transport = self.transport.clone();
                let request = request.clone();
                tasks.spawn(async move {
                    let result = transport.send_doc_sync_request(&peer, request).await;
                    (peer, result)
                });
            }

            if tasks.is_empty() {
                break;
            }

            match tasks.join_next().await {
                Some(Ok((peer, Ok(())))) => {
                    any_sent = true;
                    tracing::debug!(peer_id = %peer, "sent DocSync request");
                }
                Some(Ok((peer, Err(error)))) => {
                    tracing::warn!(peer_id = %peer, error = %error, "failed to send DocSync request");
                }
                Some(Err(error)) => {
                    tracing::warn!(error = %error, "DocSync dispatch task failed");
                }
                None => break,
            }
        }

        any_sent
    }
}

#[async_trait]
impl<B: Blockstore + 'static> P2POperations for IrohP2PAdapter<B> {
    async fn local_peer_id(&self) -> P2PResult<String> {
        Ok(self.transport.local_peer_id().to_string())
    }

    async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
        self.transport
            .listen_addresses()
            .await
            .map(|addrs| format_public_listen_addrs(self.transport.local_peer_id(), &addrs))
            .map_err(|error| P2PError::transport(error.to_string()))
    }

    async fn shareable_address(&self) -> P2PResult<Option<String>> {
        self.transport
            .listen_addresses()
            .await
            .map(|addrs| best_shareable_public_addr(self.transport.local_peer_id(), &addrs))
            .map_err(|error| P2PError::transport(error.to_string()))
    }

    async fn connected_peers(&self) -> P2PResult<Vec<String>> {
        self.check_nac(acp::nac::NodePermission::P2pPeerActive)
            .await?;

        let connected = self
            .transport
            .connected_peers()
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;

        let mut result = Vec::new();
        for peer in &connected {
            let peer_str = peer.to_string();
            if let Ok(addrs) = self.peer_addresses.read() {
                if let Some(addr) = addrs.get(&peer_str) {
                    result.push(addr.clone());
                    continue;
                }
            }
            result.push(peer_str);
        }
        Ok(result)
    }

    async fn connect_peer(&self, addr: &str) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pPeerConnect)
            .await?;

        let (peer_id, direct_addrs) = parse_public_peer_addr(addr)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;
        let dial_timeout = if direct_addrs.is_empty() {
            std::time::Duration::from_secs(10)
        } else {
            std::time::Duration::from_secs(5)
        };

        tokio::time::timeout(dial_timeout, self.transport.dial(&peer_id, direct_addrs))
            .await
            .map_err(|_| {
                P2PError::transport(format!("failed to connect: timeout dialing {peer_id}"))
            })?
            .map_err(|error| P2PError::transport(format!("failed to connect: {error}")))?;
        self.transport
            .poll_until_connected(&peer_id, std::time::Duration::from_secs(10))
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr.to_string());
        }
        self.resubscribe_tracked_document_topics().await;

        Ok(())
    }

    async fn notify_network_change(&self) -> P2PResult<()> {
        self.transport
            .network_change()
            .await
            .map_err(|error| P2PError::transport(error.to_string()))
    }

    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorList)
            .await?;

        let p2p_infos = if let Some(ref pusher) = self.doc_pusher {
            match pusher.load_persisted_replicators().await? {
                Some(infos) => infos,
                None => self
                    .transport
                    .list_replicators()
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))?,
            }
        } else {
            self.transport
                .list_replicators()
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        };

        Ok(p2p_infos
            .into_iter()
            .map(crate::to_http_replicator_info)
            .collect())
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        _explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
        _expected_authorizer_did: Option<&str>,
    ) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorAdd)
            .await?;

        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let (peer_id, direct_addrs) = parse_public_peer_addr(addr_str)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;

        let effective_collections = if collections.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                pusher.list_collections()?
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

        // Check existing replicator state before creating/updating so we can
        // skip the expensive initial replay when the replicator already exists
        // with the same collections (idempotent reconnect path).
        let existing_collection_ids: HashSet<String> = {
            let result = if let Some(ref coordinator) = self.sync_coordinator {
                coordinator
                    .get_replicator(&peer_id)
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))
            } else {
                self.transport
                    .get_replicator(&peer_id)
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

        self.transport
            .dial(&peer_id, direct_addrs)
            .await
            .map_err(|error| {
                P2PError::transport(format!("failed to connect to replicator peer: {error}"))
            })?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .create_replicator(&peer_id, collection_cids.clone(), true)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
        } else {
            self.transport
                .create_replicator(&peer_id, collection_cids.clone())
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
                let push_pusher = Arc::clone(pusher);
                let push_event_bus = self.event_bus.clone();
                let push_peer = peer_id;
                let push_options = self.replicator_push_options.load();
                let push_se_key = push_options.se_encryption_key;

                tracing::info!(
                    peer_id = %push_peer,
                    new_collections = ?new_collection_names,
                    "Replaying existing docs for new collections only"
                );

                tokio::spawn(async move {
                    if let Err(error) = push_pusher
                        .push_existing_docs(
                            &push_peer,
                            &new_collection_names,
                            push_se_key.as_ref().map(|key| key.as_slice()),
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
        self.check_nac(acp::nac::NodePermission::P2pReplicatorDelete)
            .await?;

        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let (peer_id, _direct_addrs) = parse_public_peer_addr(addr_str)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;

        let fully_deleted = if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .remove_replicator_collections(&peer_id, collections)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        } else {
            self.transport
                .remove_replicator_collections(&peer_id, collections)
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
                        "Failed to delete replicator from storage"
                    );
                }
            } else {
                let remaining = self
                    .transport
                    .get_replicator(&peer_id)
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
                            "Failed to update persisted replicator"
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

    async fn get_collections(&self) -> P2PResult<Vec<String>> {
        self.check_nac(acp::nac::NodePermission::P2pCollectionList)
            .await?;

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
        self.check_nac(acp::nac::NodePermission::P2pCollectionAdd)
            .await?;

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
        self.check_nac(acp::nac::NodePermission::P2pCollectionDelete)
            .await?;

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
        self.check_nac(acp::nac::NodePermission::P2pDocumentList)
            .await?;

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
        self.check_nac(acp::nac::NodePermission::P2pDocumentAdd)
            .await?;

        let doc_ids: Vec<String> = docs.into_iter().map(|doc| doc.doc_id).collect();
        document::validate_doc_ids(&doc_ids).map_err(|_| {
            P2PError::invalid_input("malformed document ID, missing either version or cid")
        })?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(error) = self.transport.subscribe(topic).await {
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
        self.check_nac(acp::nac::NodePermission::P2pDocumentDelete)
            .await?;

        let doc_ids: Vec<String> = docs.into_iter().map(|doc| doc.doc_id).collect();
        document::validate_doc_ids(&doc_ids).map_err(|_| {
            P2PError::invalid_input("malformed document ID, missing either version or cid")
        })?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(error) = self.transport.unsubscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %error, "Failed to unsubscribe from topic for document");
            }
            if let Ok(mut tracked) = self.tracked_documents.write() {
                tracked.remove(doc_id);
            }
        }

        if let Some(ref pusher) = self.doc_pusher {
            let all_docs: Vec<String> = self
                .tracked_documents
                .read()
                .map(|docs| docs.iter().cloned().collect())
                .unwrap_or_default();
            if let Err(error) = pusher.persist_p2p_documents(&all_docs).await {
                tracing::warn!(error = %error, "failed to persist P2P documents after removal");
            }
        }

        Ok(())
    }

    async fn republish_document(&self, collection_name: &str, doc_id: &str) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pDocumentList)
            .await?;

        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for republish"))?;
        let coordinator = self
            .sync_coordinator
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no sync coordinator for republish"))?;
        pusher.validate_collection_exists(collection_name)?;
        let collection_id = pusher.get_collection_id(collection_name).ok_or_else(|| {
            P2PError::not_found(format!("collection '{collection_name}' not found"))
        })?;
        let head_blocks = pusher.load_document_head_blocks(doc_id).await?;
        let creator_did = pusher.load_doc_creator_did(collection_name, doc_id).await?;
        let acp_actor_relationships = pusher
            .load_doc_actor_relationships(collection_name, doc_id)
            .await?;

        for (cid, block) in head_blocks {
            coordinator
                .broadcast_local_update_with_creator_and_relationships(
                    &cid,
                    &block,
                    doc_id,
                    &collection_id,
                    creator_did.as_deref(),
                    acp_actor_relationships.clone(),
                )
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
        pusher.validate_collection_exists(collection_name)?;

        let event_bus = self
            .event_bus
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no event bus for sync"))?;

        let connected_peers = self.transport.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut sub = event_bus.subscribe(&[events::EventName::MergeComplete]);
        let total_expected = connected_peers.len() * doc_ids.len();
        let mut total_received = 0;
        let overall_timeout = std::time::Duration::from_secs(10);
        let idle_timeout = std::time::Duration::from_secs(3);
        let start = std::time::Instant::now();
        let doc_set: HashSet<String> = doc_ids.iter().cloned().collect();

        for _attempt in 0..3 {
            if total_received >= total_expected || start.elapsed() >= overall_timeout {
                break;
            }

            let mut request = p2p::message::DocSyncRequest::new(doc_ids.clone());
            if let Err(error) = p2p::signing::sign_with_transport(&self.transport, &mut request) {
                event_bus.unsubscribe(sub.id());
                return Err(P2PError::internal(format!(
                    "failed to sign DocSync request: {error}"
                )));
            }

            let any_sent = self
                .send_doc_sync_requests_concurrently(&connected_peers, request)
                .await;
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

    async fn sync_branchable_collection(&self, collection_id: &str) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for sync"))?;
        pusher.validate_branchable_collection(collection_id)?;

        let connected_peers = self.transport.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_with_transport(&self.transport, &mut request).map_err(|error| {
            P2PError::internal(format!("failed to sign BranchableSync request: {error}"))
        })?;

        for peer in &connected_peers {
            let request_clone = request.clone();
            let transport = self.transport.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                if let Err(error) = transport
                    .send_branchable_sync_request(&peer, request_clone)
                    .await
                {
                    tracing::warn!(peer_id = %peer, error = %error, "failed to send BranchableSyncRequest");
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

        let connected_peers = self.transport.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        let syncer = self
            .version_syncer
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("version syncer required"))?
            .clone();
        tokio::spawn(async move {
            if let Err(error) = syncer.sync_versions(version_ids, connected_peers).await {
                tracing::warn!(error = %error, "version sync failed");
            }
        });

        Ok(())
    }
}
