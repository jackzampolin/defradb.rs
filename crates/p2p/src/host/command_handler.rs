//! Command handler for P2P host commands.

use std::collections::HashSet;

use cid::Cid;
use iroh_bitswap::Store;
use libp2p::PeerId;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};
use crate::QueryId;

use super::command::HostCommand;
use super::event::HostEvent;
use super::p2p_host::P2PHost;

impl<S: Store> P2PHost<S> {
    /// Handle a command from the handle.
    ///
    /// Returns false if the host should shutdown.
    pub(super) async fn handle_command(&mut self, command: HostCommand) -> bool {
        match command {
            HostCommand::Listen { addr, response } => {
                let result = self
                    .swarm
                    .listen_on(addr.clone())
                    .map(|_| ())
                    .map_err(|e| Error::Transport(e.to_string()));
                if response.send(result).is_err() {
                    debug!(addr = %addr, "Listen command response dropped - caller cancelled");
                }
            }

            HostCommand::Dial {
                peer_id,
                addrs,
                response,
            } => {
                let result = self.dial_peer(peer_id, addrs);
                if response.send(result).is_err() {
                    debug!(peer_id = %peer_id, "Dial command response dropped - caller cancelled");
                }
            }

            HostCommand::SendPushLog {
                peer_id,
                request,
                response,
            } => {
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .send_pushlog_request(&peer_id, request);
                self.pending_requests.insert(request_id, response);
            }

            HostCommand::SendPushLogResponse {
                channel,
                reply,
                response,
            } => {
                let result = self
                    .swarm
                    .behaviour_mut()
                    .send_pushlog_response(channel.into_inner(), reply)
                    .map(|_| ())
                    .map_err(|resp| Error::ResponseSend(format!("message_id={}", resp.message_id)));
                if response.send(result).is_err() {
                    debug!("SendPushLogResponse command response dropped - caller cancelled");
                }
            }

            HostCommand::LocalPeerId { response } => {
                if response.send(*self.swarm.local_peer_id()).is_err() {
                    debug!("LocalPeerId command response dropped - caller cancelled");
                }
            }

            HostCommand::ListenAddresses { response } => {
                let addrs: Vec<_> = self.swarm.listeners().cloned().collect();
                if response.send(addrs).is_err() {
                    debug!("ListenAddresses command response dropped - caller cancelled");
                }
            }

            HostCommand::ConnectedPeers { response } => {
                let peers: Vec<_> = self.swarm.connected_peers().cloned().collect();
                if response.send(peers).is_err() {
                    debug!("ConnectedPeers command response dropped - caller cancelled");
                }
            }

            HostCommand::Subscribe { topic, response } => {
                let ident_topic = topic.to_ident_topic();
                let result = self
                    .swarm
                    .behaviour_mut()
                    .subscribe(&ident_topic)
                    .map_err(|e| Error::GossipSubSubscription(e.to_string()));
                if response.send(result).is_err() {
                    debug!(topic = ?topic, "Subscribe command response dropped - caller cancelled");
                }
            }

            HostCommand::Unsubscribe { topic, response } => {
                let ident_topic = topic.to_ident_topic();
                let result = self
                    .swarm
                    .behaviour_mut()
                    .unsubscribe(&ident_topic)
                    .map_err(|e| Error::GossipSubUnsubscribe(e.to_string()));
                if response.send(result).is_err() {
                    debug!(topic = ?topic, "Unsubscribe command response dropped - caller cancelled");
                }
            }

            HostCommand::Publish {
                topic,
                message,
                response,
            } => {
                let ident_topic = topic.to_ident_topic();
                let result = serde_cbor::to_vec(&message)
                    .map_err(|e| Error::CborSerialization(e.to_string()))
                    .and_then(|data| {
                        self.swarm
                            .behaviour_mut()
                            .publish(ident_topic, data)
                            .map_err(|e| Error::GossipSubPublish(e.to_string()))
                    });
                if response.send(result).is_err() {
                    debug!(topic = ?topic, "Publish command response dropped - caller cancelled");
                }
            }

            HostCommand::SubscribedTopics { response } => {
                let topics: Vec<String> = self
                    .swarm
                    .behaviour()
                    .subscribed_topics()
                    .map(|t| t.to_string())
                    .collect();
                if response.send(topics).is_err() {
                    debug!("SubscribedTopics command response dropped - caller cancelled");
                }
            }

            HostCommand::Shutdown => {
                info!(
                    "Shutdown requested, waiting for {} spawned tasks to complete",
                    self.spawned_tasks.len()
                );
                // Wait for all spawned tasks to complete with a timeout
                let timeout_duration = std::time::Duration::from_secs(5);
                let shutdown_start = std::time::Instant::now();
                while !self.spawned_tasks.is_empty() {
                    if shutdown_start.elapsed() > timeout_duration {
                        warn!(
                            "Shutdown timeout exceeded, aborting {} remaining tasks",
                            self.spawned_tasks.len()
                        );
                        self.spawned_tasks.abort_all();
                        break;
                    }
                    // Try to join tasks with a short timeout
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        self.spawned_tasks.join_next(),
                    )
                    .await
                    {
                        Ok(Some(Ok(()))) => {
                            debug!("Spawned task completed during shutdown");
                        }
                        Ok(Some(Err(e))) => {
                            warn!("Spawned task failed during shutdown: {}", e);
                        }
                        Ok(None) => break,  // No more tasks
                        Err(_) => continue, // Timeout, check again
                    }
                }
                info!("All spawned tasks completed or aborted");
                return false;
            }

            HostCommand::BitswapSync {
                cid,
                providers,
                missing,
                response,
            } => {
                self.handle_bitswap_sync(cid, providers, missing, response)
                    .await;
            }

            HostCommand::BitswapCancel { query_id, response } => {
                let cancelled = if let Some(abort_handle) = self.bitswap_queries.remove(&query_id) {
                    debug!(query_id = ?query_id, "Cancelling Bitswap query");
                    abort_handle.abort();
                    true
                } else {
                    debug!(query_id = ?query_id, "Bitswap query not found for cancellation");
                    false
                };
                if response.send(cancelled).is_err() {
                    debug!(query_id = ?query_id, "BitswapCancel command response dropped - caller cancelled");
                }
            }

            HostCommand::SetReplicator {
                peer_id,
                collections,
                response,
            } => {
                debug!(peer_id = %peer_id, collections = ?collections, "Setting replicator");
                // First remove peer from all existing collections
                self.replicators.remove_peer(&peer_id);
                // Then add to the new collections
                for collection_id in &collections {
                    self.replicators.add_replicator(collection_id, peer_id);
                }
                if response.send(Ok(())).is_err() {
                    debug!(peer_id = %peer_id, "SetReplicator command response dropped - caller cancelled");
                }
            }

            HostCommand::DeleteReplicator { peer_id, response } => {
                debug!(peer_id = %peer_id, "Deleting replicator");
                self.replicators.remove_peer(&peer_id);
                if response.send(Ok(())).is_err() {
                    debug!(peer_id = %peer_id, "DeleteReplicator command response dropped - caller cancelled");
                }
            }

            HostCommand::RemoveReplicatorCollections {
                peer_id,
                collections,
                response,
            } => {
                debug!(
                    peer_id = %peer_id,
                    collections = ?collections,
                    "Removing collections from replicator"
                );

                // Remove specific collections from the replicator
                for collection_id in &collections {
                    self.replicators.remove_replicator(collection_id, &peer_id);
                }

                // Check if the replicator still has any collections
                let fully_deleted = !self.replicators.is_any_replicator(&peer_id);

                if fully_deleted {
                    debug!(peer_id = %peer_id, "Replicator fully deleted (no collections remain)");
                }

                if response.send(Ok(fully_deleted)).is_err() {
                    debug!(
                        peer_id = %peer_id,
                        "RemoveReplicatorCollections command response dropped - caller cancelled"
                    );
                }
            }

            HostCommand::GetAllReplicators { response } => {
                let infos = self.replicators.get_all_replicator_info();
                if response.send(infos).is_err() {
                    debug!("GetAllReplicators command response dropped - caller cancelled");
                }
            }

            HostCommand::GetReplicator { peer_id, response } => {
                let info = self.replicators.get_replicator_info(&peer_id);
                if response.send(info).is_err() {
                    debug!(peer_id = %peer_id, "GetReplicator command response dropped - caller cancelled");
                }
            }

            HostCommand::SendTwoStreamResponse {
                peer_id,
                reply,
                response,
            } => {
                let handler = self.two_stream_handler.clone();
                self.spawned_tasks.spawn(async move {
                    let mut h = handler.lock().await;
                    let result = h.send_response(peer_id, reply).await;
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendTwoStreamResponse command response dropped - caller cancelled");
                    }
                });
            }

            HostCommand::SendTwoStreamRequest {
                peer_id,
                request,
                response,
            } => {
                let handler = self.two_stream_handler.clone();
                self.spawned_tasks.spawn(async move {
                    let mut h = handler.lock().await;
                    let result = h.send_request(peer_id, request).await;
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendTwoStreamRequest command response dropped - caller cancelled");
                    }
                });
            }

            HostCommand::SendDocSyncResponse {
                peer_id,
                reply,
                response,
            } => {
                let handler = self.two_stream_handler.clone();
                self.spawned_tasks.spawn(async move {
                    let mut h = handler.lock().await;
                    let result = h.send_doc_sync_response(peer_id, reply).await;
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendDocSyncResponse command response dropped - caller cancelled");
                    }
                });
            }

            HostCommand::SendDocSyncRequest {
                peer_id,
                request,
                response,
            } => {
                let handler = self.two_stream_handler.clone();
                self.spawned_tasks.spawn(async move {
                    let mut h = handler.lock().await;
                    // Send the request - response will arrive asynchronously via TwoStreamEvent::DocSyncReply
                    let result = h.send_doc_sync_request_fire_and_forget(peer_id, request).await;
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendDocSyncRequest command response dropped - caller cancelled");
                    }
                });
            }

            HostCommand::SendBranchableSyncResponse {
                peer_id,
                reply,
                response,
            } => {
                let handler = self.two_stream_handler.clone();
                self.spawned_tasks.spawn(async move {
                    let mut h = handler.lock().await;
                    let result = h.send_branchable_sync_response(peer_id, reply).await;
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendBranchableSyncResponse command response dropped - caller cancelled");
                    }
                });
            }

            HostCommand::SendBranchableSyncRequest {
                peer_id,
                request,
                response,
            } => {
                let handler = self.two_stream_handler.clone();
                self.spawned_tasks.spawn(async move {
                    let mut h = handler.lock().await;
                    let result = h
                        .send_branchable_sync_request_fire_and_forget(peer_id, request)
                        .await;
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendBranchableSyncRequest command response dropped - caller cancelled");
                    }
                });
            }

            HostCommand::PeerAddresses { response } => {
                // Build full multiaddrs for connected peers (matches Go's ActivePeers).
                let connected: HashSet<PeerId> = self.swarm.connected_peers().cloned().collect();
                let addrs: Vec<String> = connected
                    .iter()
                    .filter_map(|pid| {
                        self.peer_addrs
                            .get(pid)
                            .map(|addr| format!("{}/p2p/{}", addr, pid))
                    })
                    .collect();
                if response.send(addrs).is_err() {
                    debug!("PeerAddresses command response dropped - caller cancelled");
                }
            }
        }
        true
    }

    /// Handle a Bitswap sync command.
    async fn handle_bitswap_sync(
        &mut self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
        response: tokio::sync::oneshot::Sender<Result<QueryId>>,
    ) {
        // Generate a query ID for tracking
        static QUERY_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let query_id = QueryId(QUERY_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

        info!(
            cid = %cid,
            providers = ?providers,
            missing_count = missing.len(),
            query_id = query_id.0,
            "Starting Bitswap block fetch via Client API"
        );

        // Clone the client for use in the spawned task
        let client = self.swarm.behaviour().bitswap.client().clone();
        let event_tx = self.event_tx.clone();
        let missing_cids: Vec<Cid> = missing;
        let providers_list = providers;

        // Spawn async task to fetch blocks (with cancellation support)
        let task_handle = tokio::spawn(async move {
            info!(
                query_id = query_id.0,
                missing_count = missing_cids.len(),
                providers = ?providers_list,
                "Bitswap fetch task started"
            );

            // Create a session and add providers for each CID
            let session = client.new_session().await;

            // Add each provider for each missing CID
            for cid in &missing_cids {
                for provider in &providers_list {
                    info!(
                        query_id = query_id.0,
                        cid = %cid,
                        provider = %provider,
                        "Adding Bitswap provider for CID"
                    );
                    session.add_provider(cid, *provider).await;
                }
            }

            match session.get_blocks(&missing_cids).await {
                Ok(receiver) => {
                    // Use into_parts() to get the underlying channel
                    // BlockReceiver only implements Deref (not DerefMut), so we can't call recv() through it
                    let (chan, _guard) = receiver.into_parts();
                    let mut fetched = 0;

                    while let Ok(block) = chan.recv().await {
                        fetched += 1;
                        let block_cid = *block.cid();
                        let block_data = block.data().to_vec();

                        info!(
                            query_id = query_id.0,
                            cid = %block_cid,
                            fetched = fetched,
                            total = missing_cids.len(),
                            data_len = block_data.len(),
                            "Bitswap fetched block"
                        );

                        // Send block to coordinator for storage
                        if let Err(e) = event_tx
                            .send(HostEvent::BitswapBlockReceived {
                                query_id,
                                cid: block_cid,
                                data: block_data,
                            })
                            .await
                        {
                            warn!(
                                query_id = query_id.0,
                                cid = %block_cid,
                                error = %e,
                                "Failed to send BitswapBlockReceived event"
                            );
                        }
                    }

                    let success = fetched == missing_cids.len();
                    info!(
                        query_id = query_id.0,
                        fetched = fetched,
                        total = missing_cids.len(),
                        success = success,
                        "Bitswap fetch complete"
                    );

                    // Notify completion
                    let _ = event_tx
                        .send(HostEvent::BitswapComplete {
                            query_id,
                            success,
                            error: if success {
                                None
                            } else {
                                Some(format!(
                                    "Only fetched {} of {} blocks",
                                    fetched,
                                    missing_cids.len()
                                ))
                            },
                        })
                        .await;
                }
                Err(e) => {
                    warn!(query_id = query_id.0, error = %e, "Bitswap fetch failed");
                    let _ = event_tx
                        .send(HostEvent::BitswapComplete {
                            query_id,
                            success: false,
                            error: Some(e.to_string()),
                        })
                        .await;
                }
            }
        });

        // Store the abort handle for cancellation support
        self.bitswap_queries
            .insert(query_id, task_handle.abort_handle());

        if response.send(Ok(query_id)).is_err() {
            debug!(cid = %cid, "BitswapSync command response dropped - caller cancelled");
        }
    }
}
