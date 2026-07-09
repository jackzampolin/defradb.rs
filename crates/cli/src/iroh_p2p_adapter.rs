//! P2POperations adapter for the iroh transport.
//!
//! Mirrors `P2PAdapter` (libp2p) but uses `IrohTransport` and the
//! transport-generic `TransportDocPusher`/`TransportVersionSyncer` traits.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use defra_http::router::{P2PError, P2POperations, P2PResult, ReplicationFilters, ReplicatorInfo};
use p2p::iroh::{
    best_shareable_public_addr, format_public_listen_addrs, parse_public_peer_addr, IrohTransport,
};
use p2p::sync::IrohSyncCoordinator;
use p2p::topics::DefraTopic;
use p2p::P2PTransport;

use crate::transport_doc_pusher::TransportDocPusher;
use crate::transport_version_syncer::TransportVersionSyncer;

const DOC_SYNC_DISPATCH_PARALLELISM: usize = 16;

fn p2p_filter_to_http(
    filter: &p2p::ReplicationFilter,
) -> Option<defra_http::router::ReplicationFilter> {
    match filter {
        p2p::ReplicationFilter::Predicate(conds) => {
            if conds.len() == 1 {
                let (field, condition) = conds.iter().next()?;
                if let Some(value) = condition.as_object().and_then(|op| op.get("_eq")).cloned() {
                    return Some(defra_http::router::ReplicationFilter::eq(
                        field.clone(),
                        value,
                    ));
                }
            }
            Some(defra_http::router::ReplicationFilter::predicate(
                conds.clone(),
            ))
        }
        p2p::ReplicationFilter::Acp { .. } | p2p::ReplicationFilter::All(_) => {
            tracing::warn!(
                "replication filter variant not representable over HTTP yet; omitted from replicator listing"
            );
            None
        }
    }
}

/// P2POperations implementation for iroh transport.
pub struct IrohP2PAdapter<B: Blockstore + 'static> {
    transport: IrohTransport,
    sync_coordinator: Option<Arc<IrohSyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn TransportDocPusher>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
    tracked_documents: Arc<std::sync::RwLock<HashSet<String>>>,
    nac_checker: Arc<dyn db::NodeAccessChecker>,
}

impl<B: Blockstore + 'static> IrohP2PAdapter<B> {
    async fn check_nac(&self, permission: acp::nac::NodePermission) -> P2PResult<()> {
        self.nac_checker
            .check_node_access(permission)
            .await
            .map_err(|e| P2PError::Internal(e.to_string()))
    }

    fn to_http_replicator_info(info: p2p::ReplicatorInfo) -> ReplicatorInfo {
        let address = info.addresses_str().first().map(|s| s.to_string());
        let status = Some(info.status.into());
        let last_status_change = Some(info.last_status_change_go_string());
        ReplicatorInfo {
            id: Some(info.peer_id_str().to_string()),
            collections: info.collections,
            address,
            status,
            last_status_change,
            filters: info
                .filters
                .into_iter()
                .filter_map(|(collection, filter)| Some((collection, p2p_filter_to_http(&filter)?)))
                .collect(),
        }
    }

    fn resolve_replication_filters(
        filters: ReplicationFilters,
        effective_collections: &[String],
        collection_cids: &[String],
    ) -> P2PResult<p2p::ReplicationFilters> {
        let mut resolved = p2p::ReplicationFilters::new();
        for (key, filter) in filters {
            let p2p_filter = if let Some(conds) = filter.conditions {
                p2p::ReplicationFilter::predicate(conds)
            } else {
                if filter.field.trim().is_empty() {
                    return Err(P2PError::InvalidInput(
                        "replication filter field cannot be empty".into(),
                    ));
                }
                if filter.value.is_null() || filter.value.is_array() || filter.value.is_object() {
                    return Err(P2PError::InvalidInput(format!(
                        "replication filter for collection '{key}' must use a scalar value"
                    )));
                }
                p2p::ReplicationFilter::new(filter.field, filter.value)
            };

            let collection_id = collection_cids
                .iter()
                .position(|collection_id| collection_id == &key)
                .or_else(|| {
                    effective_collections
                        .iter()
                        .position(|collection_name| collection_name == &key)
                })
                .and_then(|index| collection_cids.get(index))
                .cloned()
                .ok_or_else(|| {
                    P2PError::InvalidInput(format!(
                        "replication filter collection '{key}' was not requested"
                    ))
                })?;

            resolved.insert(collection_id, p2p_filter);
        }
        Ok(resolved)
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
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
            nac_checker,
        }
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

    /// Resolve collection names to CIDs for removal, mirroring `add_replicator`.
    fn resolve_collections_for_remove(&self, collections: Vec<String>) -> Vec<String> {
        match self.doc_pusher {
            Some(ref pusher) => {
                defra_p2p_adapter::resolve_remove_collections(collections, |name| {
                    pusher.get_collection_id(name)
                })
            }
            None => collections,
        }
    }
}

#[async_trait]
impl<B: Blockstore + 'static> P2POperations for IrohP2PAdapter<B> {
    async fn sync_status(&self) -> P2PResult<serde_json::Value> {
        match self.sync_coordinator.as_ref() {
            Some(coordinator) => serde_json::to_value(coordinator.sync_status())
                .map_err(|error| P2PError::Internal(error.to_string())),
            None => Ok(serde_json::Value::Null),
        }
    }

    async fn local_peer_id(&self) -> P2PResult<String> {
        Ok(self.transport.local_peer_id().to_string())
    }

    async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
        self.transport
            .listen_addresses()
            .await
            .map(|addrs| format_public_listen_addrs(self.transport.local_peer_id(), &addrs))
            .map_err(|e| P2PError::Transport(e.to_string()))
    }

    async fn shareable_address(&self) -> P2PResult<Option<String>> {
        self.transport
            .listen_addresses()
            .await
            .map(|addrs| best_shareable_public_addr(self.transport.local_peer_id(), &addrs))
            .map_err(|e| P2PError::Transport(e.to_string()))
    }

    async fn connected_peers(&self) -> P2PResult<Vec<String>> {
        self.check_nac(acp::nac::NodePermission::P2pPeerActive)
            .await?;

        let connected = self
            .transport
            .connected_peers()
            .await
            .map_err(|e| P2PError::Transport(e.to_string()))?;

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

        let (peer_id, direct_addrs) =
            parse_public_peer_addr(addr).map_err(|e| P2PError::InvalidInput(e.to_string()))?;

        self.transport
            .dial(&peer_id, direct_addrs)
            .await
            .map_err(|e| P2PError::Transport(format!("failed to connect: {}", e)))?;

        self.transport
            .poll_until_connected(&peer_id, std::time::Duration::from_secs(10))
            .await
            .map_err(|e| P2PError::Transport(e.to_string()))?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr.to_string());
        }

        Ok(())
    }

    async fn disconnect_peer(&self, addr: &str) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pPeerConnect)
            .await?;

        let (peer_id, _direct_addrs) =
            parse_public_peer_addr(addr).map_err(|e| P2PError::InvalidInput(e.to_string()))?;

        self.transport
            .disconnect(&peer_id)
            .await
            .map_err(|e| P2PError::Transport(e.to_string()))?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.remove(&peer_id.to_string());
        }

        Ok(())
    }

    async fn notify_network_change(&self) -> P2PResult<()> {
        self.transport
            .network_change()
            .await
            .map_err(|e| P2PError::Transport(e.to_string()))
    }

    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorList)
            .await?;

        // #1074: report the LIVE replicator registry, not the persisted peerstore,
        // so a reconciler can observe authorization drift. The coordinator and the
        // raw transport read the same shared registry (the coordinator delegates to
        // the same transport), so both branches are equally live-authoritative;
        // persisted rows only overlay address/status metadata below.
        let p2p_infos = if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .list_replicators()
                .await
                .map_err(|e| P2PError::Transport(e.to_string()))?
        } else {
            self.transport
                .list_replicators()
                .await
                .map_err(|e| P2PError::Transport(e.to_string()))?
        };
        let p2p_infos = if let Some(ref pusher) = self.doc_pusher {
            let persisted = pusher
                .load_persisted_replicators()
                .await
                .map_err(P2PError::Internal)?;
            defra_p2p_adapter::merge_live_replicators_with_persisted_metadata(p2p_infos, persisted)
        } else {
            p2p_infos
        };

        let http_infos: Vec<ReplicatorInfo> = p2p_infos
            .into_iter()
            .map(Self::to_http_replicator_info)
            .collect();

        Ok(http_infos)
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        filters: ReplicationFilters,
        _explicit_replay_capabilities: Vec<defra_http::router::ExplicitReplayCapabilityInput>,
        _expected_authorizer_did: Option<&str>,
    ) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorAdd)
            .await?;

        let addr_str = addr.ok_or_else(|| P2PError::InvalidInput("address is required".into()))?;
        let (peer_id, direct_addrs) = parse_public_peer_addr(addr_str)
            .map_err(|error| P2PError::InvalidInput(error.to_string()))?;

        let effective_collections = if collections.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
                pusher.list_collections()?
            } else {
                return Err(P2PError::Unsupported(
                    "no database context to list collections".into(),
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
                    return Err(P2PError::NotFound(format!(
                        "collection '{}' not found",
                        name
                    )));
                }
            }
        } else {
            collection_cids.clone_from(&effective_collections);
        }
        let replication_filters =
            Self::resolve_replication_filters(filters, &effective_collections, &collection_cids)?;
        if !replication_filters.is_empty() {
            self.doc_pusher
                .as_ref()
                .ok_or_else(|| {
                    P2PError::Unsupported("no database context to validate filters".into())
                })?
                .validate_replication_filters(&replication_filters)
                .map_err(P2PError::InvalidInput)?;
        }

        // Check existing replicator state before creating/updating so we can
        // skip the expensive initial replay when the replicator already exists
        // with the same collections (idempotent reconnect path).
        let (existing_collection_ids, existing_filters): (
            HashSet<String>,
            p2p::ReplicationFilters,
        ) = {
            let result = if let Some(ref coordinator) = self.sync_coordinator {
                coordinator
                    .get_replicator(&peer_id)
                    .await
                    .map_err(|e| P2PError::Transport(e.to_string()))
            } else {
                self.transport
                    .get_replicator(&peer_id)
                    .await
                    .map_err(|e| P2PError::Transport(e.to_string()))
            };
            match result {
                Ok(Some(info)) => (info.collections.into_iter().collect(), info.filters),
                Ok(None) => (HashSet::new(), p2p::ReplicationFilters::new()),
                Err(e) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %e,
                        "Failed to check existing replicator state; falling back to full replay"
                    );
                    (HashSet::new(), p2p::ReplicationFilters::new())
                }
            }
        };
        let existing_collection_ids = if existing_filters == replication_filters {
            existing_collection_ids
        } else {
            HashSet::new()
        };

        self.transport
            .dial(&peer_id, direct_addrs)
            .await
            .map_err(|e| {
                P2PError::Transport(format!("failed to connect to replicator peer: {}", e))
            })?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        if let Some(ref coordinator) = self.sync_coordinator {
            let info = p2p::ReplicatorInfo::from_raw_with_filters(
                peer_id.to_string(),
                collection_cids.clone(),
                vec![addr_str.to_string()],
                replication_filters.clone(),
            );
            coordinator
                .create_replicator_info(&peer_id, info, true)
                .await
                .map_err(|e| P2PError::Transport(e.to_string()))?;
        } else {
            let info = p2p::ReplicatorInfo::from_raw_with_filters(
                peer_id.to_string(),
                collection_cids.clone(),
                vec![addr_str.to_string()],
                replication_filters.clone(),
            );
            self.transport
                .create_replicator_info(&peer_id, info)
                .await
                .map_err(|e| P2PError::Transport(e.to_string()))?;
        }

        if let Some(ref pusher) = self.doc_pusher {
            let info = p2p::ReplicatorInfo::from_raw_with_filters(
                peer_id.to_string(),
                collection_cids.clone(),
                vec![addr_str.to_string()],
                replication_filters.clone(),
            );
            if let Err(e) = pusher.persist_replicator_info(&info).await {
                tracing::warn!(peer_id = %peer_id, error = %e, "failed to persist replicator");
            }
        }

        // Only replay collections that weren't already replicated by this peer.
        // This makes add_replicator idempotent for reconnect paths — calling it
        // again with the same collections is a no-op instead of a full replay storm.
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
                let push_filters = replication_filters.clone();

                tracing::info!(
                    peer_id = %push_peer,
                    new_collections = ?new_collection_names,
                    "Replaying existing docs for new collections only"
                );

                tokio::spawn(async move {
                    if let Err(e) = push_pusher
                        .push_existing_docs(&push_peer, &new_collection_names, &push_filters, None)
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

        let addr_str = addr.ok_or_else(|| P2PError::InvalidInput("address is required".into()))?;
        let (peer_id, _) =
            parse_public_peer_addr(addr_str).map_err(|e| P2PError::InvalidInput(e.to_string()))?;

        // Push registry is CID-keyed; resolve names symmetric with add_replicator.
        let collections = self.resolve_collections_for_remove(collections);

        let fully_deleted = if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .remove_replicator_collections(&peer_id, collections)
                .await
                .map_err(|e| P2PError::Transport(e.to_string()))?
        } else {
            self.transport
                .delete_replicator(&peer_id)
                .await
                .map_err(|e| P2PError::Transport(e.to_string()))?;
            true
        };

        // Reconcile the peerstore with the registry: wipe the peer entry only on a
        // full removal, else re-persist the remaining collections (preserving their
        // filters) so `replicator list` and post-restart restore stay consistent.
        if let Some(ref pusher) = self.doc_pusher {
            if fully_deleted {
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
            } else {
                let remaining = if let Some(ref coordinator) = self.sync_coordinator {
                    coordinator
                        .get_replicator(&peer_id)
                        .await
                        .map_err(|e| P2PError::Transport(e.to_string()))?
                } else {
                    self.transport
                        .get_replicator(&peer_id)
                        .await
                        .map_err(|e| P2PError::Transport(e.to_string()))?
                };
                if let Some(info) = remaining {
                    if let Err(e) = pusher
                        .persist_replicator(&peer_id.to_string(), &info.collections)
                        .await
                    {
                        tracing::warn!(
                            peer_id = %peer_id,
                            error = %e,
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
                .map_err(|e| P2PError::Transport(e.to_string()))
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
                        return Err(P2PError::NotFound(format!(
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
                    .map_err(|e| P2PError::Transport(e.to_string()))?;
            }

            if let Some(ref pusher) = self.doc_pusher {
                let all_cols = coordinator
                    .get_subscribed_collections()
                    .await
                    .map_err(|e| P2PError::Transport(e.to_string()))?;
                if let Err(e) = pusher.persist_p2p_collections(&all_cols).await {
                    tracing::warn!(error = %e, "failed to persist P2P collections");
                }
            }

            Ok(())
        } else {
            Err(P2PError::Unsupported(
                "p2p collections functionality requires sync coordinator".into(),
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
                        pusher
                            .get_collection_id(&collection_name)
                            .ok_or_else(|| format!("collection '{}' not found", collection_name))
                    } else {
                        Ok(collection_name)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            for topic_id in topic_ids {
                coordinator
                    .unsubscribe_collection(&topic_id)
                    .await
                    .map_err(|e| P2PError::Transport(e.to_string()))?;
            }

            if let Some(ref pusher) = self.doc_pusher {
                let all_cols = coordinator
                    .get_subscribed_collections()
                    .await
                    .map_err(|e| P2PError::Transport(e.to_string()))?;
                if let Err(e) = pusher.persist_p2p_collections(&all_cols).await {
                    tracing::warn!(error = %e, "failed to persist P2P collections after removal");
                }
            }

            Ok(())
        } else {
            Err(P2PError::Unsupported(
                "p2p collections functionality requires sync coordinator".into(),
            ))
        }
    }

    async fn get_documents(&self) -> P2PResult<Vec<defra_http::router::P2pDocumentInfo>> {
        self.check_nac(acp::nac::NodePermission::P2pDocumentList)
            .await?;

        let docs = self
            .tracked_documents
            .read()
            .map_err(|e| P2PError::Internal(format!("failed to read tracked documents: {}", e)))?;
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
    ) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pDocumentAdd)
            .await?;

        let doc_ids: Vec<String> = docs.into_iter().map(|d| d.doc_id).collect();

        document::validate_doc_ids(&doc_ids).map_err(|_| {
            P2PError::InvalidInput("malformed document ID, missing either version or cid".into())
        })?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(e) = self.transport.subscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %e, "Failed to subscribe to topic for document");
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
            if let Err(e) = pusher.persist_p2p_documents(&all_docs).await {
                tracing::warn!(error = %e, "failed to persist P2P documents");
            }
        }

        Ok(())
    }

    async fn remove_documents(
        &self,
        docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pDocumentDelete)
            .await?;

        let doc_ids: Vec<String> = docs.into_iter().map(|d| d.doc_id).collect();

        document::validate_doc_ids(&doc_ids).map_err(|_| {
            P2PError::InvalidInput("malformed document ID, missing either version or cid".into())
        })?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(e) = self.transport.unsubscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %e, "Failed to unsubscribe from topic for document");
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
            if let Err(e) = pusher.persist_p2p_documents(&all_docs).await {
                tracing::warn!(error = %e, "failed to persist P2P documents after removal");
            }
        }

        Ok(())
    }

    async fn sync_documents(&self, collection_name: &str, doc_ids: Vec<String>) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| "no database context for sync".to_string())?;
        pusher.validate_collection_exists(collection_name)?;

        let event_bus = self
            .event_bus
            .as_ref()
            .ok_or_else(|| "no event bus for sync".to_string())?;

        let connected_peers =
            self.transport.connected_peers().await.map_err(|e| {
                P2PError::Transport(format!("failed to get connected peers: {}", e))
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
            if let Err(e) = p2p::signing::sign_with_transport(&self.transport, &mut request) {
                event_bus.unsubscribe(sub.id());
                return Err(P2PError::Internal(format!(
                    "failed to sign DocSync request: {}",
                    e
                )));
            }

            let any_sent = self
                .send_doc_sync_requests_concurrently(&connected_peers, request)
                .await;

            // No peers accepted the request; no MergeComplete events will arrive.
            if !any_sent {
                break;
            }

            let mut last_merge = std::time::Instant::now();
            while total_received < total_expected && start.elapsed() < overall_timeout {
                // Exit early if no merge events have arrived recently — sync either completed
                // or the remote doesn't have the requested documents.
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
            .ok_or_else(|| "no database context for sync".to_string())?;
        pusher.validate_branchable_collection(collection_id)?;

        let connected_peers =
            self.transport.connected_peers().await.map_err(|e| {
                P2PError::Transport(format!("failed to get connected peers: {}", e))
            })?;

        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_with_transport(&self.transport, &mut request).map_err(|e| {
            P2PError::Internal(format!("failed to sign BranchableSync request: {}", e))
        })?;

        for peer in &connected_peers {
            let request_clone = request.clone();
            let t = self.transport.clone();
            let p = peer.clone();
            tokio::spawn(async move {
                if let Err(e) = t.send_branchable_sync_request(&p, request_clone).await {
                    tracing::warn!(peer_id = %p, error = %e, "failed to send BranchableSyncRequest");
                }
            });
        }

        Ok(())
    }

    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> P2PResult<()> {
        if version_ids.is_empty() {
            return Ok(());
        }

        for vid in &version_ids {
            cid::Cid::try_from(vid.as_str())
                .map_err(|e| P2PError::InvalidInput(format!("invalid cid: {}", e)))?;
        }

        let connected_peers =
            self.transport.connected_peers().await.map_err(|e| {
                P2PError::Transport(format!("failed to get connected peers: {}", e))
            })?;

        if connected_peers.is_empty() {
            return Ok(());
        }

        let syncer = self
            .version_syncer
            .as_ref()
            .ok_or_else(|| "version syncer required".to_string())?
            .clone();

        tokio::spawn(async move {
            match syncer.sync_versions(version_ids, connected_peers).await {
                Ok(()) => tracing::info!("version_sync_complete"),
                Err(e) => tracing::warn!(error = %e, "version sync failed"),
            }
        });

        Ok(())
    }
}
