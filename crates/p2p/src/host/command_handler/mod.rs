//! Command handler for P2P host commands.

mod bitswap;
mod messaging;
mod network;
mod pubsub;

use iroh_bitswap::Store;
use tracing::{debug, info, warn};

use super::command::HostCommand;
use super::p2p_host::P2PHost;

impl<S: Store> P2PHost<S> {
    /// Handle a command from the handle.
    ///
    /// Returns false if the host should shutdown.
    pub(super) async fn handle_command(&mut self, command: HostCommand) -> bool {
        match command {
            HostCommand::Listen { addr, response } => {
                self.handle_listen(addr, response);
            }
            HostCommand::Dial {
                peer_id,
                addrs,
                response,
            } => {
                self.handle_dial(peer_id, addrs, response);
            }
            HostCommand::Shutdown => {
                return self.handle_shutdown().await;
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
            HostCommand::PeerAddresses { response } => {
                self.handle_peer_addresses(response);
            }
            HostCommand::Subscribe { topic, response } => {
                self.handle_subscribe(topic, response);
            }
            HostCommand::Unsubscribe { topic, response } => {
                self.handle_unsubscribe(topic, response);
            }
            HostCommand::Publish {
                topic,
                message,
                response,
            } => {
                self.handle_publish(topic, message, response);
            }
            HostCommand::SubscribedTopics { response } => {
                self.handle_subscribed_topics(response);
            }
            HostCommand::SendPushLog {
                peer_id,
                request,
                response,
            } => {
                self.handle_send_pushlog(peer_id, request, response);
            }
            HostCommand::SendPushLogResponse {
                channel,
                reply,
                response,
            } => {
                self.handle_send_pushlog_response(channel, reply, response);
            }
            HostCommand::SendTwoStreamResponse {
                peer_id,
                reply,
                response,
            } => {
                self.handle_send_two_stream_response(peer_id, reply, response);
            }
            HostCommand::SendTwoStreamRequest {
                peer_id,
                request,
                response,
            } => {
                self.handle_send_two_stream_request(peer_id, request, response);
            }
            HostCommand::SendDocSyncResponse {
                peer_id,
                reply,
                response,
            } => {
                self.handle_send_doc_sync_response(peer_id, reply, response);
            }
            HostCommand::SendDocSyncRequest {
                peer_id,
                request,
                response,
            } => {
                self.handle_send_doc_sync_request(peer_id, request, response);
            }
            HostCommand::SendBranchableSyncResponse {
                peer_id,
                reply,
                response,
            } => {
                self.handle_send_branchable_sync_response(peer_id, reply, response);
            }
            HostCommand::SendBranchableSyncRequest {
                peer_id,
                request,
                response,
            } => {
                self.handle_send_branchable_sync_request(peer_id, request, response);
            }
            HostCommand::SendSEArtifacts {
                peer_id,
                request,
                response,
            } => {
                self.handle_send_se_artifacts(peer_id, request, response);
            }
            HostCommand::CreateReplicator {
                peer_id,
                collections,
                response,
            } => {
                self.handle_create_replicator(peer_id, collections, response);
            }
            HostCommand::DeleteReplicator { peer_id, response } => {
                self.handle_delete_replicator(peer_id, response);
            }
            HostCommand::RemoveReplicatorCollections {
                peer_id,
                collections,
                response,
            } => {
                self.handle_remove_replicator_collections(peer_id, collections, response);
            }
            HostCommand::ListReplicators { response } => {
                let infos = self.replicators.list_replicator_info();
                if response.send(infos).is_err() {
                    debug!("ListReplicators command response dropped - caller cancelled");
                }
            }
            HostCommand::GetReplicator { peer_id, response } => {
                let info = self.replicators.get_replicator_info(&peer_id.to_string());
                if response.send(info).is_err() {
                    debug!(peer_id = %peer_id, "GetReplicator command response dropped - caller cancelled");
                }
            }
            HostCommand::SendCarRequest {
                peer_id,
                root_cid,
                response,
            } => {
                self.handle_send_car_request(peer_id, root_cid, response);
            }
            HostCommand::SendCarResponse {
                peer_id,
                car_data,
                response,
            } => {
                self.handle_send_car_response(peer_id, car_data, response);
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
                self.handle_bitswap_cancel(query_id, response);
            }
        }
        true
    }

    async fn handle_shutdown(&mut self) -> bool {
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
        false
    }
}
