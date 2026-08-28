use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use crate::transport_doc_pusher::TransportDocPusher;
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
    doc_pusher: Option<Arc<dyn TransportDocPusher>>,
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

async fn unmerged_doc_sync_heads<B, I>(blockstore: &B, heads: I) -> HashSet<cid::Cid>
where
    B: Blockstore,
    I: IntoIterator<Item = cid::Cid>,
{
    let mut pending = HashSet::new();
    for cid in heads {
        if !matches!(blockstore.is_merged(&cid).await, Ok(true)) {
            pending.insert(cid);
        }
    }
    pending
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
        doc_pusher: Arc<dyn TransportDocPusher>,
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
        doc_pusher: Arc<dyn TransportDocPusher>,
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
        let validated_capabilities = crate::validate_explicit_replay_capabilities(
            explicit_replay_capabilities,
            expected_authorizer_did,
            &requested_collections,
            &local_peer_id,
            &target_peer_id,
        )?;

        let collections_with_changed_capabilities = crate::collections_with_changed_capabilities(
            &collection_cids,
            &validated_capabilities,
            |collection_id, capability| {
                self.handle
                    .explicit_replay_capability_matches(peer_id, collection_id, capability)
            },
        );

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
        self.handle
            .dial(peer_id, vec![parsed.transport_addr])
            .await
            .map_err(|error| {
                P2PError::transport(format!("failed to connect to replicator peer: {error}"))
            })?;
        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        let replicator_info = p2p::ReplicatorInfo::from_raw_with_filters(
            peer_id.to_string(),
            collection_cids.clone(),
            vec![addr_str.to_string()],
            replication_filters.clone(),
        );
        if let Some(ref pusher) = self.doc_pusher {
            pusher
                .persist_replicator_info(&replicator_info)
                .await
                .map_err(|error| {
                    P2PError::persistence(format!(
                        "failed to durably register replicator {peer_id}: {error}"
                    ))
                })?;
        }

        if let Some(ref coordinator) = self.sync_coordinator {
            let transport_peer_id = p2p::transport::PeerId::from(peer_id);
            coordinator
                .create_replicator_info(&transport_peer_id, replicator_info.clone(), false)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
        } else {
            self.handle
                .create_replicator_info(peer_id, replicator_info)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
        }

        // Replay new collections, plus collections whose explicit replay
        // capability changed. The latter case matters for encrypted ACP
        // replay where a previous configuration may have carried an invalid
        // authorizer capability and therefore skipped storing the document.
        let collection_names_requiring_replay = crate::collections_requiring_replay(
            &effective_collections,
            &collection_cids,
            &existing_collection_ids,
            &existing_filters,
            &replication_filters,
            &collections_with_changed_capabilities,
        );

        if !collection_names_requiring_replay.is_empty() {
            if let Some(ref pusher) = self.doc_pusher {
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
                            &p2p::transport::PeerId::from(peer_id),
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
        } else if collections.is_empty() {
            self.handle
                .delete_replicator(peer_id)
                .await
                .map_err(|error| P2PError::transport(error.to_string()))?;
            true
        } else {
            self.handle
                .remove_replicator_collections(peer_id, collections)
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
        if !use_pubsub {
            // Parallelism 1: libp2p's send is fire-and-forget, dispatched one
            // peer at a time.
            return crate::doc_sync::sync::sync_documents(
                Arc::new(self.handle.clone()),
                event_bus.as_ref(),
                doc_ids,
                timeout.unwrap_or(crate::doc_sync::DEFAULT_DOC_SYNC_TIMEOUT),
                1,
            )
            .await;
        }

        let connected_peers = self.handle.connected_peers().await.map_err(|error| {
            P2PError::transport(format!("failed to get connected peers: {error}"))
        })?;
        if connected_peers.is_empty() {
            return Err(P2PError::transport("no connected peers to sync with"));
        }

        let mut sub = event_bus.subscribe(&[events::EventName::MergeComplete]);
        // A caller deadline covers the whole operation, as Go's single wait
        // context does. `start` is taken before the publish, so the merge loop
        // below gets whatever the reply wait leaves rather than a second full
        // budget.
        let overall_timeout = timeout.unwrap_or(crate::doc_sync::DEFAULT_DOC_SYNC_TIMEOUT);
        let start = std::time::Instant::now();
        let doc_set: HashSet<String> = doc_ids.iter().cloned().collect();

        let coord = self
            .sync_coordinator
            .as_ref()
            .expect("pubsub readiness requires a coordinator");
        // Go's `activePeers` (`sync_doc.go:97-109`) is the connected set, so
        // that is what a reply is expected from — not the doc-sync topic's
        // subscriber list.
        let expected_peers = connected_peers.len();
        let replies = coord
            .pubsub_sync_documents(doc_ids, Some(overall_timeout), Some(expected_peers))
            .await
            .map_err(|error| {
                event_bus.unsubscribe(sub.id());
                P2PError::transport(format!("pubsub_rpc doc-sync failed: {error}"))
            })?;
        // Evaluated on the raw advertisement, before the merged-head filter
        // below: a peer offering a head we already hold is a reply Go counts,
        // and filtering first would turn it into a spurious timeout.
        let heads = match crate::doc_sync::pubsub_replies::advertised_heads(
            expected_peers,
            &doc_set,
            &replies,
        ) {
            Ok(heads) => heads,
            Err(error) => {
                event_bus.unsubscribe(sub.id());
                return Err(error);
            }
        };
        let mut pending_heads = unmerged_doc_sync_heads(coord.blockstore().as_ref(), heads).await;

        while !pending_heads.is_empty() && start.elapsed() < overall_timeout {
            match tokio::time::timeout(std::time::Duration::from_millis(100), sub.recv()).await {
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
            let expected_responses = self
                .handle
                .topic_peers(DefraTopic::SyncBranchable)
                .await
                .map_err(|error| {
                    event_bus.unsubscribe(sub.id());
                    P2PError::transport(format!(
                        "failed to get sync-branchable topic peers: {error}"
                    ))
                })?
                .len();
            if expected_responses == 0 {
                event_bus.unsubscribe(sub.id());
                return Ok(());
            }
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

    async fn push_documents_to_peer(
        &self,
        peer_id: &str,
        docs: Vec<P2pDocumentRequest>,
    ) -> P2PResult<()> {
        let peer_id = match p2p::parse_multiaddr_with_peer_id(peer_id) {
            Ok(parsed) => parsed.peer_id,
            Err(_) => peer_id.parse::<libp2p::PeerId>().map_err(|error| {
                P2PError::invalid_input(format!("invalid peer ID '{peer_id}': {error}"))
            })?,
        };
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| P2PError::unsupported("no database context for document push"))?;
        let pairs: Vec<(String, String)> = docs
            .into_iter()
            .map(|doc| (doc.collection, doc.doc_id))
            .collect();
        pusher
            .push_existing_docs_by_id(&p2p::transport::PeerId::from(peer_id), &pairs)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::DefraBlockstore;
    use p2p::BitswapStoreAdapter;
    use storage::RegolithStore;

    #[tokio::test]
    async fn merged_doc_sync_heads_are_not_pending() {
        let blockstore = DefraBlockstore::new(Arc::new(RegolithStore::in_memory().unwrap()), true);
        let merged =
            cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
                .unwrap();
        let unmerged =
            cid::Cid::try_from("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
                .unwrap();
        blockstore.put(&merged, b"merged").await.unwrap();
        blockstore.put(&unmerged, b"unmerged").await.unwrap();
        blockstore.mark_as_merged(&merged).await.unwrap();

        let pending = unmerged_doc_sync_heads(&blockstore, [merged, unmerged, unmerged]).await;

        assert_eq!(pending, HashSet::from([unmerged]));
    }

    /// #1299 regression guard for the ordering of the two steps the pubsub
    /// branch runs: a silent peer plus a head we have already merged is a
    /// success in Go (its `result` map holds the head), so the timeout check
    /// must read the raw advertisement and only then drop merged heads.
    #[tokio::test]
    async fn already_merged_head_from_one_peer_is_not_a_timeout() {
        let blockstore = DefraBlockstore::new(Arc::new(RegolithStore::in_memory().unwrap()), true);
        let merged =
            cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
                .unwrap();
        blockstore.put(&merged, b"merged").await.unwrap();
        blockstore.mark_as_merged(&merged).await.unwrap();

        let doc_set = HashSet::from(["bae-doc-1".to_string()]);
        let replies = [(
            "peer-a".to_string(),
            p2p::message::pubsub::DocSyncReply {
                results: vec![p2p::message::pubsub::DocSyncItem {
                    doc_id: "bae-doc-1".to_string(),
                    heads: vec![merged.to_bytes()],
                }],
                sender: "peer-a".to_string(),
            },
        )];

        let heads = crate::doc_sync::pubsub_replies::advertised_heads(2, &doc_set, &replies)
            .expect("an advertised head is a result even when a peer stays silent");
        let pending = unmerged_doc_sync_heads(&blockstore, heads).await;

        assert!(
            pending.is_empty(),
            "the advertised head is already merged, so nothing is left to wait for"
        );
    }

    /// The adapter under test carries no sync coordinator, so the blockstore
    /// generic is never exercised; a do-nothing implementation satisfies it.
    #[derive(Debug)]
    struct NoopBlockstore;

    #[async_trait]
    impl Blockstore for NoopBlockstore {
        async fn get(&self, _cid: &cid::Cid) -> blockstore::Result<Option<bytes::Bytes>> {
            Ok(None)
        }
        async fn put(&self, _cid: &cid::Cid, _data: &[u8]) -> blockstore::Result<()> {
            Ok(())
        }
        async fn put_many(&self, _blocks: &[(&cid::Cid, &[u8])]) -> blockstore::Result<()> {
            Ok(())
        }
        async fn has(&self, _cid: &cid::Cid) -> blockstore::Result<bool> {
            Ok(false)
        }
        async fn delete(&self, _cid: &cid::Cid) -> blockstore::Result<()> {
            Ok(())
        }
        async fn get_size(&self, _cid: &cid::Cid) -> blockstore::Result<Option<usize>> {
            Ok(None)
        }
        async fn all_cids(&self) -> blockstore::Result<Vec<cid::Cid>> {
            Ok(Vec::new())
        }
        fn hash_on_read(&self, _enabled: bool) {}
        async fn is_merged(&self, _cid: &cid::Cid) -> blockstore::Result<bool> {
            Ok(false)
        }
        async fn mark_as_merged(&self, _cid: &cid::Cid) -> blockstore::Result<()> {
            Ok(())
        }
        async fn get_unmerged(&self) -> blockstore::Result<Vec<cid::Cid>> {
            Ok(Vec::new())
        }
    }

    async fn wait_until_connected(handle: &P2PHostHandle, peer_id: libp2p::PeerId) {
        let start = std::time::Instant::now();
        loop {
            if handle
                .connected_peers()
                .await
                .unwrap_or_default()
                .contains(&peer_id)
            {
                return;
            }
            assert!(
                start.elapsed() < std::time::Duration::from_secs(5),
                "timed out waiting for connection to {peer_id}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Dials `handle_a` to `handle_b` and waits until each side observes the
    /// other as connected. Mirrors `assert_hosts_connect_over` in
    /// `crates/p2p/tests/host_tests.rs`.
    async fn connect_hosts(handle_a: &P2PHostHandle, handle_b: &P2PHostHandle) {
        handle_b
            .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .await
            .unwrap();
        let addr_b = handle_b.listen_addresses().await.unwrap().remove(0);
        let peer_b = handle_b.local_peer_id_cached();
        let peer_a = handle_a.local_peer_id_cached();

        handle_a.dial(peer_b, vec![addr_b]).await.unwrap();
        wait_until_connected(handle_a, peer_b).await;
        wait_until_connected(handle_b, peer_a).await;
    }

    /// Characterization, libp2p counterpart of the iroh test: a peer that
    /// accepts the request but never replies leaves `sync_documents` exiting
    /// Ok. libp2p's send is fire-and-forget, unlike iroh's, which awaits a
    /// reply and so fails against an unresponsive peer and exits via the
    /// `!any_sent` branch instead.
    #[tokio::test]
    async fn doc_sync_with_unresponsive_peer_returns_ok() {
        let (host_a, handle_a, _events_a, _replicators_a) =
            p2p::host::P2PHost::new(BitswapStoreAdapter::new(Arc::new(NoopBlockstore)))
                .await
                .expect("host a");
        let (host_b, handle_b, _events_b, _replicators_b) =
            p2p::host::P2PHost::new(BitswapStoreAdapter::new(Arc::new(NoopBlockstore)))
                .await
                .expect("host b");

        tokio::spawn(host_a.run());
        tokio::spawn(host_b.run());

        let adapter = P2PAdapter::<NoopBlockstore> {
            handle: handle_a.clone(),
            sync_coordinator: None,
            doc_pusher: Some(crate::doc_sync::test_support::StubPusher::arc()),
            event_bus: Some(Arc::new(events::ChannelBus::default())),
            version_syncer: None,
            replicator_push_options: ReplicatorPushOptionsState::default(),
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
            nac_checker: None,
        };

        connect_hosts(&handle_a, &handle_b).await;

        let result = adapter
            .sync_documents("Users", vec!["bae-does-not-matter".to_string()], None)
            .await;

        assert!(
            result.is_ok(),
            "unresponsive peer should currently exit via idle timeout with Ok, got: {result:?}"
        );
    }
}
