//! P2POperations adapter for the iroh transport.
//!
//! Mirrors `P2PAdapter` (libp2p) but uses `IrohTransport` and the
//! transport-generic `TransportDocPusher`/`TransportVersionSyncer` traits.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;

use defra_http::router::{P2POperations, ReplicatorInfo};
use p2p::iroh::IrohTransport;
use p2p::sync::IrohSyncCoordinator;
use p2p::topics::DefraTopic;
use p2p::transport::{PeerAddr, PeerId};
use p2p::P2PTransport;

use crate::transport_doc_pusher::TransportDocPusher;
use crate::transport_version_syncer::TransportVersionSyncer;

/// P2POperations implementation for iroh transport.
pub struct IrohP2PAdapter<B: Blockstore + 'static> {
    transport: IrohTransport,
    sync_coordinator: Option<Arc<IrohSyncCoordinator<B>>>,
    doc_pusher: Option<Arc<dyn TransportDocPusher>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
    peer_addresses: Arc<std::sync::RwLock<HashMap<String, String>>>,
    tracked_documents: Arc<std::sync::RwLock<HashSet<String>>>,
}

impl<B: Blockstore + 'static> IrohP2PAdapter<B> {
    pub fn with_full_context(
        transport: IrohTransport,
        coordinator: Arc<IrohSyncCoordinator<B>>,
        doc_pusher: Arc<dyn TransportDocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
    ) -> Self {
        Self {
            transport,
            sync_coordinator: Some(coordinator),
            doc_pusher: Some(doc_pusher),
            event_bus: Some(event_bus),
            version_syncer,
            peer_addresses: Arc::new(std::sync::RwLock::new(HashMap::new())),
            tracked_documents: Arc::new(std::sync::RwLock::new(HashSet::new())),
        }
    }

    pub fn with_full_context_arc(
        transport: IrohTransport,
        coordinator: Arc<IrohSyncCoordinator<B>>,
        doc_pusher: Arc<dyn TransportDocPusher>,
        event_bus: Arc<dyn events::Bus>,
        version_syncer: Option<Arc<dyn TransportVersionSyncer>>,
    ) -> Arc<dyn P2POperations> {
        Arc::new(Self::with_full_context(
            transport,
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
impl<B: Blockstore + 'static> P2POperations for IrohP2PAdapter<B> {
    async fn local_peer_id(&self) -> Result<String, String> {
        Ok(self.transport.local_peer_id().to_string())
    }

    async fn listen_addresses(&self) -> Result<Vec<String>, String> {
        self.transport
            .listen_addresses()
            .await
            .map(|addrs| addrs.into_iter().map(|a| a.to_string()).collect())
            .map_err(|e| e.to_string())
    }

    async fn connected_peers(&self) -> Result<Vec<String>, String> {
        let connected = self
            .transport
            .connected_peers()
            .await
            .map_err(|e| e.to_string())?;

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

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        // Parse compound address formats to extract endpoint ID and direct addresses:
        // - "{socket_addr}/p2p/{endpoint_id}" → dial with direct address hint
        // - "iroh://{endpoint_id}" → dial via relay/discovery
        // - "{endpoint_id}" (raw hex) → dial via relay/discovery
        let (endpoint_id, direct_addrs) = parse_iroh_address(addr);
        let peer_id = PeerId::new(endpoint_id);

        self.transport
            .dial(&peer_id, direct_addrs)
            .await
            .map_err(|e| format!("failed to connect: {}", e))?;

        self.transport
            .poll_until_connected(&peer_id, std::time::Duration::from_secs(10))
            .await
            .map_err(|e| e.to_string())?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr.to_string());
        }

        Ok(())
    }

    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String> {
        let p2p_infos = self
            .transport
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
        _explicit_replay_authorizer: Option<&str>,
    ) -> Result<(), String> {
        let addr_str = addr.ok_or_else(|| "address is required".to_string())?;
        let (endpoint_id, direct_addrs) = parse_iroh_address(addr_str);
        let peer_id = PeerId::new(endpoint_id);

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
                    return Err(format!("collection '{}' not found", name));
                }
            }
        } else {
            collection_cids.clone_from(&effective_collections);
        }

        self.transport
            .dial(&peer_id, direct_addrs)
            .await
            .map_err(|e| format!("failed to connect to replicator peer: {}", e))?;

        if let Ok(mut addrs) = self.peer_addresses.write() {
            addrs.insert(peer_id.to_string(), addr_str.to_string());
        }

        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .create_replicator(&peer_id, collection_cids.clone(), true)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            self.transport
                .create_replicator(&peer_id, collection_cids.clone())
                .await
                .map_err(|e| e.to_string())?;
        }

        if let Some(ref pusher) = self.doc_pusher {
            if let Err(e) = pusher
                .persist_replicator(&peer_id.to_string(), &collection_cids)
                .await
            {
                tracing::warn!(peer_id = %peer_id, error = %e, "failed to persist replicator");
            }
        }

        if let Some(ref pusher) = self.doc_pusher {
            let push_pusher = Arc::clone(pusher);
            let push_event_bus = self.event_bus.clone();
            let push_collections = effective_collections;
            let push_peer = peer_id;

            tokio::spawn(async move {
                if let Err(e) = push_pusher
                    .push_existing_docs(&push_peer, &push_collections, None)
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
        let (endpoint_id, _direct_addrs) = parse_iroh_address(addr_str);
        let peer_id = PeerId::new(endpoint_id);

        if let Some(ref coordinator) = self.sync_coordinator {
            coordinator
                .remove_replicator_collections(&peer_id, collections)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            self.transport
                .delete_replicator(&peer_id)
                .await
                .map_err(|e| e.to_string())?;
        }

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
                    .map_err(|e| e.to_string())?;
            }

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
            for collection_name in collections {
                let topic_id = if let Some(ref pusher) = self.doc_pusher {
                    pusher
                        .get_collection_id(&collection_name)
                        .unwrap_or(collection_name)
                } else {
                    collection_name
                };
                coordinator
                    .unsubscribe_collection(&topic_id)
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

        document::validate_doc_ids(&doc_ids)
            .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

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
    ) -> Result<(), String> {
        let doc_ids: Vec<String> = docs.into_iter().map(|d| d.doc_id).collect();

        document::validate_doc_ids(&doc_ids)
            .map_err(|_| "malformed document ID, missing either version or cid".to_string())?;

        for doc_id in &doc_ids {
            let topic = DefraTopic::document(doc_id);
            if let Err(e) = self.transport.unsubscribe(topic).await {
                tracing::warn!(doc_id = %doc_id, error = %e, "Failed to unsubscribe from topic for document");
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
            .transport
            .connected_peers()
            .await
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

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
                return Err(format!("failed to sign DocSync request: {}", e));
            }

            let mut any_sent = false;
            for peer in &connected_peers {
                match self
                    .transport
                    .send_doc_sync_request(peer, request.clone())
                    .await
                {
                    Ok(()) => any_sent = true,
                    Err(e) => {
                        tracing::warn!(peer_id = %peer, error = %e, "failed to send DocSync request")
                    }
                }
            }

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

    async fn sync_branchable_collection(&self, collection_id: &str) -> Result<(), String> {
        let pusher = self
            .doc_pusher
            .as_ref()
            .ok_or_else(|| "no database context for sync".to_string())?;
        pusher.validate_branchable_collection(collection_id)?;

        let connected_peers = self
            .transport
            .connected_peers()
            .await
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

        if connected_peers.is_empty() {
            return Ok(());
        }

        let mut request = p2p::message::BranchableSyncRequest::new(collection_id.to_string());
        p2p::signing::sign_with_transport(&self.transport, &mut request)
            .map_err(|e| format!("failed to sign BranchableSync request: {}", e))?;

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

    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> Result<(), String> {
        if version_ids.is_empty() {
            return Ok(());
        }

        for vid in &version_ids {
            cid::Cid::try_from(vid.as_str()).map_err(|e| format!("invalid cid: {}", e))?;
        }

        let connected_peers = self
            .transport
            .connected_peers()
            .await
            .map_err(|e| format!("failed to get connected peers: {}", e))?;

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

/// Parse an iroh address into (endpoint_id, direct_addresses).
///
/// Handles formats:
/// - `"{socket_addr}/p2p/{endpoint_id}"` → endpoint_id + socket_addr as direct address
/// - `"iroh://{endpoint_id}"` → endpoint_id only (relay/discovery)
/// - `"{endpoint_id}"` → raw hex endpoint_id (relay/discovery)
fn parse_iroh_address(addr: &str) -> (String, Vec<PeerAddr>) {
    if let Some(pos) = addr.rfind("/p2p/") {
        let addr_part = &addr[..pos];
        let id_part = &addr[pos + 5..];
        (
            id_part.to_string(),
            vec![PeerAddr::new(addr_part.to_string())],
        )
    } else {
        (addr.to_string(), vec![])
    }
}
