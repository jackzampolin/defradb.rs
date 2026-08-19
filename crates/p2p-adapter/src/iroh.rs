use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::transport_doc_pusher::TransportDocPusher;
use crate::transport_version_syncer::TransportVersionSyncer;
use crate::{
    ExplicitReplayCapabilityInput, P2PError, P2PErrorExt as _, P2POperations, P2PResult,
    P2pDocumentInfo, P2pDocumentRequest, ReplicationFilters, ReplicatorInfo, ReplicatorPushOptions,
    ReplicatorPushOptionsState,
};

use p2p::iroh::{
    best_shareable_public_addr, canonical_peer_id, format_public_listen_addrs,
    parse_public_peer_addr, IrohTransport,
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

    /// True when the transport already holds a live connection to `peer_id`.
    ///
    /// Comparison is in canonical id form (`canonical_peer_id`), so a base32
    /// spelling of the peer id still matches the transport's canonical-hex
    /// entries. A failed read degrades to `false`, so callers fall back to
    /// dialing exactly as they did before this check existed.
    async fn is_transport_connected(&self, peer_id: &p2p::transport::PeerId) -> bool {
        let canonical_id = canonical_peer_id(peer_id);
        self.transport
            .connected_peers()
            .await
            .map(|peers| {
                peers
                    .iter()
                    .any(|peer| canonical_peer_id(peer) == canonical_id)
            })
            .unwrap_or(false)
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
                    return Err(P2PError::invalid_input(
                        "replication filter field cannot be empty",
                    ));
                }
                if filter.value.is_null() || filter.value.is_array() || filter.value.is_object() {
                    return Err(P2PError::invalid_input(format!(
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
                    P2PError::invalid_input(format!(
                        "replication filter collection '{key}' was not requested"
                    ))
                })?;

            resolved.insert(collection_id, p2p_filter);
        }
        Ok(resolved)
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

    /// Transport-only adapter for tests: no coordinator, pusher, event bus, or
    /// NAC. Enough surface to exercise the connection-management paths
    /// (`connect_peer`, `add_replicator` registration) against real endpoints.
    #[cfg(test)]
    fn for_tests(transport: IrohTransport) -> Self {
        Self {
            transport,
            sync_coordinator: None,
            doc_pusher: None,
            event_bus: None,
            version_syncer: None,
            replicator_push_options: ReplicatorPushOptionsState::default(),
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
            nac_checker: None,
        }
    }

    /// Resolve collection names to CIDs for removal, mirroring `add_replicator`.
    fn resolve_collections_for_remove(&self, collections: Vec<String>) -> Vec<String> {
        match self.doc_pusher {
            Some(ref pusher) => crate::resolve_remove_collections(collections, |name| {
                pusher.get_collection_id(name)
            }),
            None => collections,
        }
    }
}

#[async_trait]
impl<B: Blockstore + 'static> P2POperations for IrohP2PAdapter<B> {
    async fn sync_status(&self) -> P2PResult<serde_json::Value> {
        let Some(coordinator) = self.sync_coordinator.as_ref() else {
            return Ok(serde_json::Value::Null);
        };
        let mut status = serde_json::to_value(coordinator.sync_status())
            .map_err(|error| P2PError::transport(error.to_string()))?;
        if let (Some(pusher), Some(object)) = (self.doc_pusher.as_ref(), status.as_object_mut()) {
            object.insert(
                "push_retry_markers".to_string(),
                serde_json::to_value(pusher.push_retry_marker_stats().await?)
                    .map_err(|error| P2PError::transport(error.to_string()))?,
            );
        }
        Ok(status)
    }

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
        // Connecting to an already-connected peer is a no-op (matching Go
        // DefraDB, where libp2p `Connect` returns immediately when connected):
        // a redundant dial is not just wasted work — on Linux it can time out
        // against a healthy connection and fail the caller. Refresh the
        // address book and return.
        if self.is_transport_connected(&peer_id).await {
            if let Ok(mut addrs) = self.peer_addresses.write() {
                addrs.insert(peer_id.to_string(), addr.to_string());
            }
            return Ok(());
        }

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

    async fn disconnect_peer(&self, addr: &str) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pPeerDisconnect)
            .await?;

        let (peer_id, _direct_addrs) = parse_public_peer_addr(addr)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;
        self.transport
            .disconnect(&peer_id)
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.remove(&peer_id.to_string());
        }
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

        // #1074: report the LIVE replicator registry, not the persisted peerstore,
        // so a reconciler can observe authorization drift. The coordinator and the
        // raw transport read the same shared registry (the coordinator delegates to
        // the same transport), so both branches are equally live-authoritative;
        // persisted rows only overlay address/status metadata below.
        let p2p_infos = if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .list_replicators()
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        } else {
            self.transport
                .list_replicators()
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        };
        let p2p_infos = if let Some(ref pusher) = self.doc_pusher {
            crate::merge_live_replicators_with_persisted_metadata(
                p2p_infos,
                pusher.load_persisted_replicators().await?,
            )
        } else {
            p2p_infos
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
        filters: ReplicationFilters,
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
        let replication_filters =
            Self::resolve_replication_filters(filters, &effective_collections, &collection_cids)?;
        if !replication_filters.is_empty() {
            self.doc_pusher
                .as_ref()
                .ok_or_else(|| P2PError::unsupported("no database context to validate filters"))?
                .validate_replication_filters(&replication_filters)?;
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
                    .map_err(|error| P2PError::transport(error.to_string()))
            } else {
                self.transport
                    .get_replicator(&peer_id)
                    .await
                    .map_err(|error| P2PError::transport(error.to_string()))
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

        // Same rationale as `connect_peer`: in the common pairing flow the
        // replicator is installed over an already-live connection, and a
        // redundant dial can spuriously time out (Linux). The registration and
        // initial replay below ride the existing connection; the address book
        // entry is still refreshed either way.
        if !self.is_transport_connected(&peer_id).await {
            self.transport
                .dial(&peer_id, direct_addrs)
                .await
                .map_err(|error| {
                    P2PError::transport(format!("failed to connect to replicator peer: {error}"))
                })?;
        }

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
                .create_replicator_info(&peer_id, info, false)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
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
                .map_err(|error| P2PError::transport(error.to_string()))?;
        }

        if let Some(ref pusher) = self.doc_pusher {
            let info = p2p::ReplicatorInfo::from_raw_with_filters(
                peer_id.to_string(),
                collection_cids.clone(),
                vec![addr_str.to_string()],
                replication_filters.clone(),
            );
            if let Err(error) = pusher.persist_replicator_info(&info).await {
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
                let push_identity = push_options.se_identity_pubkey;
                let push_filters = replication_filters.clone();

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
                            &push_filters,
                            push_se_key.as_ref().map(|key| key.as_slice()),
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
        self.check_nac(acp::nac::NodePermission::P2pReplicatorDelete)
            .await?;

        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let (peer_id, _direct_addrs) = parse_public_peer_addr(addr_str)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;

        // Push registry is CID-keyed; resolve names symmetric with add_replicator.
        let collections = self.resolve_collections_for_remove(collections);

        let fully_deleted = if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .remove_replicator_collections(&peer_id, collections)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        } else if collections.is_empty() {
            self.transport
                .delete_replicator(&peer_id)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
            true
        } else {
            self.transport
                .remove_replicator_collections(&peer_id, collections)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        };

        if let Some(ref pusher) = self.doc_pusher {
            if fully_deleted {
                pusher
                    .delete_persisted_replicator(&peer_id.to_string())
                    .await?;
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

    async fn sync_documents(
        &self,
        collection_name: &str,
        doc_ids: Vec<String>,
        timeout: Option<std::time::Duration>,
    ) -> P2PResult<()> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for sync"))?;
        pusher.validate_collection_exists(collection_name)?;

        let event_bus = self
            .event_bus
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no event bus for sync"))?;

        crate::doc_sync::sync::sync_documents(
            Arc::new(self.transport.clone()),
            event_bus.as_ref(),
            doc_ids,
            timeout.unwrap_or(crate::doc_sync::DEFAULT_DOC_SYNC_TIMEOUT),
            DOC_SYNC_DISPATCH_PARALLELISM,
        )
        .await
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use bytes::Bytes;
    use cid::Cid;
    use p2p::iroh::{
        is_ticket_string, load_or_generate_secret_key, spawn_endpoint, IrohDiscoveryConfig,
        IrohEndpointConfig, IrohRelayModeConfig,
    };
    use p2p::P2PTransport;

    use super::*;

    /// The adapters under test carry no sync coordinator, so the blockstore
    /// generic is never exercised; a do-nothing implementation satisfies it.
    struct NoopBlockstore;

    #[async_trait]
    impl Blockstore for NoopBlockstore {
        async fn get(&self, _cid: &Cid) -> blockstore::Result<Option<Bytes>> {
            Ok(None)
        }
        async fn put(&self, _cid: &Cid, _data: &[u8]) -> blockstore::Result<()> {
            Ok(())
        }
        async fn put_many(&self, _blocks: &[(&Cid, &[u8])]) -> blockstore::Result<()> {
            Ok(())
        }
        async fn has(&self, _cid: &Cid) -> blockstore::Result<bool> {
            Ok(false)
        }
        async fn delete(&self, _cid: &Cid) -> blockstore::Result<()> {
            Ok(())
        }
        async fn get_size(&self, _cid: &Cid) -> blockstore::Result<Option<usize>> {
            Ok(None)
        }
        async fn all_cids(&self) -> blockstore::Result<Vec<Cid>> {
            Ok(Vec::new())
        }
        fn hash_on_read(&self, _enabled: bool) {}
        async fn is_merged(&self, _cid: &Cid) -> blockstore::Result<bool> {
            Ok(false)
        }
        async fn mark_as_merged(&self, _cid: &Cid) -> blockstore::Result<()> {
            Ok(())
        }
        async fn get_unmerged(&self) -> blockstore::Result<Vec<Cid>> {
            Ok(Vec::new())
        }
    }

    fn test_endpoint_config(secret_key: iroh::SecretKey) -> IrohEndpointConfig {
        IrohEndpointConfig {
            secret_key,
            node_identity: None,
            relay_mode: IrohRelayModeConfig::Disabled,
            discovery: IrohDiscoveryConfig::Disabled,
            bind_port: None,
            bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            max_concurrent_multipath_paths: None,
            gossip_heal: Default::default(),
        }
    }

    /// The endpoint ticket among a transport's listen addresses — the same
    /// self-contained dialable form production peers exchange.
    async fn dialable_ticket(transport: &IrohTransport) -> String {
        transport
            .listen_addresses()
            .await
            .expect("listen addrs")
            .iter()
            .map(|addr| addr.as_str())
            .find(|addr| is_ticket_string(addr))
            .expect("endpoint ticket in listen addresses")
            .to_string()
    }

    /// Base32 spelling of a canonical-hex endpoint id (iroh accepts both).
    fn base32_spelling(hex_id: &str) -> String {
        let bytes = data_encoding::HEXLOWER
            .decode(hex_id.as_bytes())
            .expect("valid hex endpoint id");
        data_encoding::BASE32_NOPAD.encode(&bytes).to_lowercase()
    }

    /// Regression for the Linux demo `pair` hang: connecting to an
    /// already-connected peer must be a no-op, not a redial. Both reconnect
    /// attempts below carry an undialable direct address (a port nothing
    /// listens on) for the connected peer — pre-fix, `connect_peer` dialed it
    /// and failed; post-fix, the live connection short-circuits the dial. The
    /// base32 spelling exercises the canonical-form comparison
    /// (`canonical_peer_id`) rather than raw string equality.
    #[tokio::test]
    async fn connect_peer_is_noop_when_already_connected() {
        let key_a = load_or_generate_secret_key(None).await.expect("key a");
        let key_b = load_or_generate_secret_key(None).await.expect("key b");
        let (command_tx_a, _events_a, _replicators_a, _task_a) =
            spawn_endpoint(test_endpoint_config(key_a.clone()))
                .await
                .expect("endpoint a");
        let (command_tx_b, _events_b, _replicators_b, _task_b) =
            spawn_endpoint(test_endpoint_config(key_b.clone()))
                .await
                .expect("endpoint b");
        let transport_a = IrohTransport::new(command_tx_a, key_a);
        let transport_b = IrohTransport::new(command_tx_b, key_b);
        let adapter = IrohP2PAdapter::<NoopBlockstore>::for_tests(transport_a);

        // Establish the connection through the adapter's normal dial path.
        let dial_addr = dialable_ticket(&transport_b).await;
        adapter
            .connect_peer(&dial_addr)
            .await
            .expect("initial dial");

        let hex_id = transport_b.local_peer_id().to_string();
        adapter
            .connect_peer(&format!("{hex_id}@127.0.0.1:1"))
            .await
            .expect("reconnect to a connected peer (hex id) must be a no-op");
        adapter
            .connect_peer(&format!("{}@127.0.0.1:1", base32_spelling(&hex_id)))
            .await
            .expect("reconnect to a connected peer (base32 id) must be a no-op");
    }

    /// Negative control: the already-connected check must not blanket-accept.
    /// With no connection to the target, an undialable address still fails.
    #[tokio::test]
    async fn connect_peer_still_dials_when_not_connected() {
        let key_a = load_or_generate_secret_key(None).await.expect("key a");
        let (command_tx_a, _events_a, _replicators_a, _task_a) =
            spawn_endpoint(test_endpoint_config(key_a.clone()))
                .await
                .expect("endpoint a");
        let transport_a = IrohTransport::new(command_tx_a, key_a);
        let adapter = IrohP2PAdapter::<NoopBlockstore>::for_tests(transport_a);

        let phantom = load_or_generate_secret_key(None)
            .await
            .expect("phantom key")
            .public()
            .to_string();
        adapter
            .connect_peer(&format!("{phantom}@127.0.0.1:1"))
            .await
            .expect_err("dial to an unconnected, undialable peer must fail");
    }

    /// `add_replicator` shares the rationale: installing a replicator over an
    /// already-live connection must not redial. The undialable address proves
    /// the internal `transport.dial` is skipped; registration still succeeds
    /// and the replicator is listed.
    #[tokio::test]
    async fn add_replicator_skips_dial_when_already_connected() {
        let key_a = load_or_generate_secret_key(None).await.expect("key a");
        let key_b = load_or_generate_secret_key(None).await.expect("key b");
        let (command_tx_a, _events_a, _replicators_a, _task_a) =
            spawn_endpoint(test_endpoint_config(key_a.clone()))
                .await
                .expect("endpoint a");
        let (command_tx_b, _events_b, _replicators_b, _task_b) =
            spawn_endpoint(test_endpoint_config(key_b.clone()))
                .await
                .expect("endpoint b");
        let transport_a = IrohTransport::new(command_tx_a, key_a);
        let transport_b = IrohTransport::new(command_tx_b, key_b);
        let adapter = IrohP2PAdapter::<NoopBlockstore>::for_tests(transport_a);

        let dial_addr = dialable_ticket(&transport_b).await;
        adapter
            .connect_peer(&dial_addr)
            .await
            .expect("initial dial");

        // Undialable address for the connected peer: with no doc pusher the
        // provided collection tokens are used as-is, and with no coordinator
        // the replicator registers at the transport.
        let undialable = format!("{}@127.0.0.1:1", transport_b.local_peer_id());
        adapter
            .add_replicator(
                vec!["Collection1".to_string()],
                Some(&undialable),
                ReplicationFilters::new(),
                Vec::new(),
                None,
            )
            .await
            .expect("add_replicator over a live connection must not redial");

        let replicators = adapter.get_replicators().await.expect("list replicators");
        assert!(
            replicators
                .iter()
                .any(|r| r.id.as_deref() == Some(transport_b.local_peer_id().as_str())),
            "replicator registered without a redial: {replicators:?}"
        );
    }

    /// #1299, against a real transport: with a peer that never replies, iroh's
    /// send awaits a reply that never comes and fails, so nothing reached any
    /// peer and the sync must report an error.
    ///
    /// The peer is made unresponsive by dropping its `DocSyncRequest` event —
    /// and with it the reply token, the peer's half of the bidirectional
    /// stream. That resets the stream, so the sender's await fails in
    /// milliseconds rather than waiting out iroh's 30s request-response
    /// timeout, which keeps this runnable by default.
    ///
    /// This fails if iroh's send ever becomes fire-and-forget like libp2p's: a
    /// one-way send ignores what the peer does with the stream and returns
    /// `Ok`, which would turn this sync into a silent success.
    #[tokio::test]
    async fn doc_sync_with_unresponsive_peer_errors() {
        let key_a = load_or_generate_secret_key(None).await.expect("key a");
        let key_b = load_or_generate_secret_key(None).await.expect("key b");
        let (command_tx_a, _events_a, _replicators_a, _task_a) =
            spawn_endpoint(test_endpoint_config(key_a.clone()))
                .await
                .expect("endpoint a");
        let (command_tx_b, mut events_b, _replicators_b, _task_b) =
            spawn_endpoint(test_endpoint_config(key_b.clone()))
                .await
                .expect("endpoint b");
        let transport_a = IrohTransport::new(command_tx_a, key_a);
        let transport_b = IrohTransport::new(command_tx_b, key_b);

        let adapter = IrohP2PAdapter::<NoopBlockstore> {
            transport: transport_a,
            sync_coordinator: None,
            doc_pusher: Some(crate::doc_sync::test_support::StubPusher::arc()),
            event_bus: Some(Arc::new(events::ChannelBus::default())),
            version_syncer: None,
            replicator_push_options: ReplicatorPushOptionsState::default(),
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
            nac_checker: None,
        };

        // Endpoint B has no coordinator behind it, so it accepts the doc-sync
        // request at the transport layer and never produces a reply or a merge.
        // Dropping each event drops the reply token with it.
        let drain_b = tokio::spawn(async move { while events_b.recv().await.is_some() {} });

        let dial_addr = dialable_ticket(&transport_b).await;
        adapter.connect_peer(&dial_addr).await.expect("dial b");

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            adapter.sync_documents("Users", vec!["bae-does-not-matter".to_string()], None),
        )
        .await
        .expect("sync must fail on the reset stream, not wait out the 30s response timeout");
        drain_b.abort();

        let error = result.expect_err("no request reached a peer, so sync must fail");
        assert!(
            error
                .to_string()
                .contains("no doc-sync request could be sent"),
            "expected a no-send error, got: {error}"
        );
    }
}
