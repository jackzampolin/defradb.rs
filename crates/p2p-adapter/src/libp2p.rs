use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::libp2p_doc_pusher::DocPusher;
use crate::{
    ExplicitReplayCapabilityInput, P2PError, P2PErrorExt as _, P2POperations, P2PResult,
    P2pDocumentInfo, P2pDocumentRequest, ReplicationFilters, ReplicatorInfo, ReplicatorPushOptions,
    ReplicatorPushOptionsState,
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
    ) -> P2PResult<()>;
}

/// Adapter implementing embedded P2P operations on top of `P2PHostHandle`.
pub struct P2PAdapter<B: Blockstore + 'static> {
    handle: P2PHostHandle,
    sync_coordinator: Option<Arc<Libp2pSyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn DocPusher>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    version_syncer: Option<Arc<dyn VersionSyncer>>,
    replicator_push_options: ReplicatorPushOptionsState,
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
    tracked_documents: Arc<std::sync::RwLock<HashSet<String>>>,
    nac_checker: Option<Arc<dyn db::NodeAccessChecker>>,
}

async fn wait_for_branchable_merges(
    sub: &mut events::Subscription,
    collection_id: &str,
    start: std::time::Instant,
    overall_timeout: std::time::Duration,
    idle_timeout: std::time::Duration,
) {
    let mut saw_merge = false;
    let mut last_activity = std::time::Instant::now();

    while start.elapsed() < overall_timeout {
        if last_activity.elapsed() > idle_timeout {
            break;
        }

        match tokio::time::timeout(std::time::Duration::from_millis(100), sub.recv()).await {
            Ok(Some(msg)) => {
                if let Some(data) = msg.as_merge_complete() {
                    if data.collection_id == collection_id {
                        saw_merge = true;
                        last_activity = std::time::Instant::now();
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {
                if !saw_merge && start.elapsed() > idle_timeout {
                    break;
                }
            }
        }
    }
}

impl<B: Blockstore + 'static> P2PAdapter<B> {
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
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Self {
        Self {
            handle,
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
        handle: P2PHostHandle,
        coordinator: Arc<Libp2pSyncCoordinator<B>>,
        doc_pusher: Arc<dyn DocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn VersionSyncer>>,
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_full_context(
            handle,
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
impl<B: Blockstore + 'static> P2POperations for P2PAdapter<B> {
    async fn sync_status(&self) -> P2PResult<serde_json::Value> {
        match self.sync_coordinator.as_ref() {
            Some(coordinator) => serde_json::to_value(coordinator.sync_status())
                .map_err(|error| P2PError::transport(error.to_string())),
            None => Ok(serde_json::Value::Null),
        }
    }

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
        self.check_nac(acp::nac::NodePermission::P2pPeerActive)
            .await?;

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
        self.check_nac(acp::nac::NodePermission::P2pPeerConnect)
            .await?;

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

    async fn disconnect_peer(&self, addr: &str) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pPeerDisconnect)
            .await?;

        let parsed = p2p::parse_multiaddr_with_peer_id(addr)
            .map_err(|error| P2PError::invalid_input(error.to_string()))?;
        self.handle
            .disconnect(parsed.peer_id)
            .await
            .map_err(|error| P2PError::transport(error.to_string()))?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.remove(&parsed.peer_id.to_string());
        }
        Ok(())
    }

    async fn notify_network_change(&self) -> P2PResult<()> {
        Ok(())
    }

    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorList)
            .await?;

        // #1074: report the LIVE replicator registry, not the persisted peerstore,
        // so a reconciler can observe authorization drift. The coordinator and the
        // raw handle read the same shared registry (the coordinator delegates to the
        // same transport), so both branches are equally live-authoritative; persisted
        // rows only overlay address/status metadata below.
        let p2p_infos = if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .list_replicators()
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?
        } else {
            self.handle
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
        explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
        expected_authorizer_did: Option<&str>,
    ) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorAdd)
            .await?;

        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let parsed = p2p::parse_multiaddr_with_peer_id(addr_str)
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

        let peer_id = parsed.peer_id;
        let requested_collections: HashSet<String> = collection_cids.iter().cloned().collect();
        let local_peer_id = self.handle.local_peer_id_cached().to_string();
        let target_peer_id = peer_id.to_string();
        let mut validated_capabilities = Vec::new();

        if !explicit_replay_capabilities.is_empty() {
            let expected_authorizer_did = expected_authorizer_did.ok_or_else(|| {
                P2PError::invalid_input(
                    "explicit replay capabilities require an authenticated identity",
                )
            })?;

            for capability in explicit_replay_capabilities {
                if !requested_collections.contains(&capability.collection_id) {
                    return Err(P2PError::invalid_input(format!(
                        "explicit replay capability collection '{}' was not requested",
                        capability.collection_id
                    )));
                }

                let authorization = p2p::verify_explicit_replay_capability(
                    &capability.capability,
                    &local_peer_id,
                    &target_peer_id,
                    &capability.collection_id,
                )
                .map_err(|error| {
                    P2PError::invalid_input(format!(
                        "invalid explicit replay capability for collection '{}': {}",
                        capability.collection_id, error
                    ))
                })?;

                if authorization.authorizer_did != expected_authorizer_did {
                    return Err(P2PError::invalid_input(format!(
                        "explicit replay capability authorizer '{}' did not match authenticated identity '{}'",
                        authorization.authorizer_did, expected_authorizer_did
                    )));
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
        let (existing_collection_ids, existing_filters): (
            HashSet<String>,
            p2p::ReplicationFilters,
        ) = {
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
            let info = p2p::ReplicatorInfo::from_raw_with_filters(
                peer_id.to_string(),
                collection_cids.clone(),
                vec![addr_str.to_string()],
                replication_filters.clone(),
            );
            coordinator
                .create_replicator_info(&transport_peer_id, info, true)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
        } else {
            let info = p2p::ReplicatorInfo::from_raw_with_filters(
                peer_id.to_string(),
                collection_cids.clone(),
                vec![addr_str.to_string()],
                replication_filters.clone(),
            );
            self.handle
                .create_replicator_info(peer_id, info)
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

        // Replay new collections, plus collections whose explicit replay
        // capability changed. The latter case matters for encrypted ACP
        // replay where a previous configuration may have carried an invalid
        // authorizer capability and therefore skipped storing the document.
        let collection_names_requiring_replay = crate::collections_requiring_replay(
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
                let push_options = self.replicator_push_options.load();
                let push_se_key = push_options.se_encryption_key;
                let push_identity = push_options.se_identity_pubkey;
                let push_filters = replication_filters.clone();

                tracing::info!(
                    peer_id = %peer_id,
                    replay_collections = ?collection_names_requiring_replay,
                    "Replaying existing docs for collections requiring replay"
                );

                tokio::spawn(async move {
                    if let Err(error) = push_pusher
                        .push_existing_docs(
                            &push_handle,
                            peer_id,
                            &collection_names_requiring_replay,
                            &push_filters,
                            push_se_key.as_ref().map(|key| key.as_slice()),
                            push_identity.as_deref(),
                        )
                        .await
                    {
                        tracing::error!(error = %error, "Failed to push existing docs to replicator");
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
    ) -> P2PResult<()> {
        self.check_nac(acp::nac::NodePermission::P2pReplicatorDelete)
            .await?;

        let addr_str = addr.ok_or_else(|| P2PError::invalid_input("address is required"))?;
        let peer_id = match p2p::parse_multiaddr_with_peer_id(addr_str) {
            Ok(parsed) => parsed.peer_id,
            Err(_) => addr_str.parse::<libp2p::PeerId>().map_err(|error| {
                P2PError::invalid_input(format!("invalid peer ID '{}': {}", addr_str, error))
            })?,
        };

        // Push registry is CID-keyed; resolve names symmetric with add_replicator.
        let collections = self.resolve_collections_for_remove(collections);

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
        self.check_nac(acp::nac::NodePermission::P2pDocumentDelete)
            .await?;

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

        let connected_peers = self.handle.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Ok(());
        }

        // Wire-compatible with Go (#828): use pubsub_rpc doc-sync when the
        // coordinator has it. `pubsub_sync_documents` also feeds each
        // received reply through the coordinator's DAG-fetch scheduler,
        // so merges flow to the event bus the same way as the two-stream
        // path. Falls back to two-stream per-peer requests when no
        // coordinator is wired (e.g. light tests).
        let use_pubsub = self
            .sync_coordinator
            .as_ref()
            .is_some_and(|coord| coord.pubsub_services_ready());
        let sync_peer_count = if use_pubsub {
            match self.handle.topic_peers(DefraTopic::DocSync).await {
                Ok(peers) if !peers.is_empty() => peers.len(),
                Ok(_) => connected_peers.len(),
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        "failed to get doc-sync topic peers; using connected peer count"
                    );
                    connected_peers.len()
                }
            }
        } else {
            connected_peers.len()
        };

        let mut sub = event_bus.subscribe(&[events::EventName::MergeComplete]);
        let total_expected = sync_peer_count * doc_ids.len();
        let mut total_received = 0;
        let overall_timeout = std::time::Duration::from_secs(30);
        let idle_timeout = std::time::Duration::from_secs(3);
        let start = std::time::Instant::now();
        let doc_set: HashSet<String> = doc_ids.iter().cloned().collect();

        if use_pubsub {
            let coord = self
                .sync_coordinator
                .as_ref()
                .expect("pubsub readiness requires a coordinator");
            let replies = coord
                .pubsub_sync_documents(
                    doc_ids,
                    Some(std::time::Duration::from_secs(8)),
                    Some(sync_peer_count),
                )
                .await
                .map_err(|error| {
                    event_bus.unsubscribe(sub.id());
                    P2PError::transport(format!("pubsub_rpc doc-sync failed: {error}"))
                })?;
            let mut pending_heads: HashSet<cid::Cid> = replies
                .into_iter()
                .flat_map(|(_, reply)| reply.results)
                .flat_map(|item| item.heads)
                .filter_map(|head| cid::Cid::try_from(head.as_slice()).ok())
                .collect();

            while !pending_heads.is_empty() && start.elapsed() < overall_timeout {
                match tokio::time::timeout(std::time::Duration::from_millis(100), sub.recv()).await
                {
                    Ok(Some(msg)) => {
                        if let Some(data) = msg.as_merge_complete() {
                            if doc_set.contains(&data.doc_id) {
                                pending_heads.remove(&data.cid);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
            }

            event_bus.unsubscribe(sub.id());
            return Ok(());
        }

        for _attempt in 0..3 {
            if total_received >= total_expected || start.elapsed() >= overall_timeout {
                break;
            }

            // Track whether any request was dispatched. If none were, further
            // attempts cannot produce merges — exit like the historical CLI
            // and iroh paths instead of burning the full overall timeout.
            let mut any_sent = false;
            let mut request = p2p::message::DocSyncRequest::new(doc_ids.clone());
            if let Err(error) = p2p::signing::sign_message(self.handle.keypair(), &mut request) {
                event_bus.unsubscribe(sub.id());
                return Err(P2PError::internal(format!(
                    "failed to sign DocSync request: {error}"
                )));
            }

            for peer_id in &connected_peers {
                match self
                    .handle
                    .send_doc_sync_request(*peer_id, request.clone())
                    .await
                {
                    Ok(_) => any_sent = true,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id, error = %error, "failed to send DocSync request");
                    }
                }
            }

            if !any_sent {
                break;
            }

            // Idle completion matches iroh/historical CLI: exit after
            // `idle_timeout` with no MergeComplete events, even when zero
            // merges arrived. Requiring a minimum merge count (the previous
            // shared-adapter gate) held HTTP handlers for the full 30s when
            // peers had nothing to contribute — e.g. source-side explicit
            // sync while collection replication delivers the doc out of band.
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
        let overall_timeout = std::time::Duration::from_secs(30);
        let idle_timeout = std::time::Duration::from_secs(3);
        let start = std::time::Instant::now();
        let collection_id_string = collection_id.to_string();

        // Go-compatible pubsub_rpc path when coordinator is wired (#828).
        // Falls back to two-stream per-peer requests otherwise.
        if let Some(coord) = self.sync_coordinator.as_ref() {
            let expected_responses = match self.handle.topic_peers(DefraTopic::SyncBranchable).await
            {
                Ok(peers) if !peers.is_empty() => peers.len(),
                Ok(_) => connected_peers.len(),
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        "failed to get sync-branchable topic peers; using connected peer count"
                    );
                    connected_peers.len()
                }
            };
            coord
                .pubsub_sync_branchable_collection(
                    collection_id.to_string(),
                    Some(std::time::Duration::from_secs(8)),
                    Some(expected_responses),
                )
                .await
                .map_err(|error| {
                    event_bus.unsubscribe(sub.id());
                    P2PError::transport(format!("pubsub_rpc branchable-sync failed: {error}"))
                })?;
            wait_for_branchable_merges(
                &mut sub,
                &collection_id_string,
                start,
                overall_timeout,
                idle_timeout,
            )
            .await;
            event_bus.unsubscribe(sub.id());
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_message(self.handle.keypair(), &mut request).map_err(|error| {
            event_bus.unsubscribe(sub.id());
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

        wait_for_branchable_merges(
            &mut sub,
            &collection_id_string,
            start,
            overall_timeout,
            idle_timeout,
        )
        .await;
        event_bus.unsubscribe(sub.id());
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
    }
}
