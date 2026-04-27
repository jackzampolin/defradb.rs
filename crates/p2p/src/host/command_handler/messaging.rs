//! PushLog, TwoStream, DocSync, BranchableSync, and SE messaging commands.

use cid::Cid;
use iroh_bitswap::Store;
use libp2p::PeerId;
use tracing::debug;

use crate::error::{Error, Result};
use crate::host::ResponseChannel;
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, IdentityRequest,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};

use super::super::p2p_host::P2PHost;

impl<S: Store> P2PHost<S> {
    pub(super) fn handle_send_pushlog(
        &mut self,
        peer_id: PeerId,
        request: PushLogRequest,
        response: tokio::sync::oneshot::Sender<Result<PushLogReply>>,
    ) {
        let request_id = self
            .swarm
            .behaviour_mut()
            .send_pushlog_request(&peer_id, request);
        self.pending_requests.insert(request_id, response);
    }

    pub(super) fn handle_send_pushlog_response(
        &mut self,
        channel: ResponseChannel,
        reply: PushLogReply,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
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

    pub(super) fn handle_send_two_stream_response(
        &mut self,
        peer_id: PeerId,
        reply: PushLogReply,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut h = handler.lock().await;
            let result = h.send_response(peer_id, reply).await;
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendTwoStreamResponse command response dropped - caller cancelled");
            }
        });
    }

    pub(super) fn handle_send_two_stream_request(
        &mut self,
        peer_id: PeerId,
        request: PushLogRequest,
        response: tokio::sync::oneshot::Sender<Result<PushLogReply>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            // Phase 1: Send the request (needs handler lock for stream control).
            let (message_id, rx) = {
                let mut h = handler.lock().await;
                match h.start_request(peer_id, request).await {
                    Ok(pair) => pair,
                    Err(e) => {
                        if response.send(Err(e)).is_err() {
                            debug!(peer_id = %peer_id, "SendTwoStreamRequest command response dropped - caller cancelled");
                        }
                        return;
                    }
                }
            }; // Handler lock released here — response handler can now route replies.

            // Phase 2: Wait for response WITHOUT holding the handler lock.
            let result = match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                rx,
            )
            .await
            {
                Ok(Ok(reply)) => Ok(reply),
                Ok(Err(_)) => {
                    handler.lock().await.cleanup_pending(peer_id, &message_id);
                    Err(crate::error::Error::Transport(
                        "response channel closed".into(),
                    ))
                }
                Err(_) => {
                    handler.lock().await.cleanup_pending(peer_id, &message_id);
                    Err(crate::error::Error::Transport(
                        "timeout waiting for response".into(),
                    ))
                }
            };
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendTwoStreamRequest command response dropped - caller cancelled");
            }
        });
    }

    pub(super) fn handle_send_doc_sync_response(
        &mut self,
        peer_id: PeerId,
        reply: DocSyncReply,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut h = handler.lock().await;
            let result = h.send_doc_sync_response(peer_id, reply).await;
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendDocSyncResponse command response dropped - caller cancelled");
            }
        });
    }

    pub(super) fn handle_send_doc_sync_request(
        &mut self,
        peer_id: PeerId,
        request: DocSyncRequest,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
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

    pub(super) fn handle_send_branchable_sync_response(
        &mut self,
        peer_id: PeerId,
        reply: BranchableSyncReply,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut h = handler.lock().await;
            let result = h.send_branchable_sync_response(peer_id, reply).await;
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendBranchableSyncResponse command response dropped - caller cancelled");
            }
        });
    }

    pub(super) fn handle_send_branchable_sync_request(
        &mut self,
        peer_id: PeerId,
        request: BranchableSyncRequest,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
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

    pub(super) fn handle_send_se_artifacts(
        &mut self,
        peer_id: PeerId,
        request: PushSEArtifactsRequest,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut h = handler.lock().await;
            let result = h.send_se_artifacts_fire_and_forget(peer_id, request).await;
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendSEArtifacts command response dropped - caller cancelled");
            }
        });
    }

    pub(super) fn handle_send_car_request(
        &mut self,
        peer_id: PeerId,
        root_cid: Cid,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut h = handler.lock().await;
            let result = h.send_car_request(peer_id, root_cid).await;
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendCarRequest command response dropped");
            }
        });
    }

    pub(super) fn handle_send_car_response(
        &mut self,
        peer_id: PeerId,
        car_data: Vec<u8>,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut h = handler.lock().await;
            let result = h.send_car_response(peer_id, car_data).await;
            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "SendCarResponse command response dropped");
            }
        });
    }

    pub(super) fn handle_get_peer_identity(
        &mut self,
        peer_id: PeerId,
        response: tokio::sync::oneshot::Sender<Result<Option<identity::Did>>>,
    ) {
        if let Some(identity) = self.peer_identities.get(&peer_id).cloned() {
            let _ = response.send(Ok(Some(identity)));
            return;
        }

        let local_peer_id = self.swarm.local_peer_id().to_string();
        let keypair = self.keypair.clone();
        let handler = self.two_stream_handler.clone();
        self.spawned_tasks.spawn(async move {
            let mut request = IdentityRequest::new(local_peer_id.clone());
            let result = match crate::signing::sign_message(&keypair, &mut request) {
                Ok(()) => {
                    let mut h = handler.lock().await;
                    h.send_identity_request(peer_id, request).await
                }
                Err(error) => Err(crate::error::Error::Transport(format!(
                    "failed to sign identity request: {error}"
                ))),
            };

            let result = result.and_then(|reply| {
                if let Some(error) = reply.err_message.clone() {
                    return Err(crate::error::Error::Transport(error));
                }
                let token_identity = identity::from_token(&reply.identity_token)
                    .map_err(|e| {
                        crate::error::Error::Transport(format!(
                            "invalid peer identity token: {e}"
                        ))
                    })?;
                identity::verify_auth_token(&token_identity, &local_peer_id).map_err(|e| {
                    crate::error::Error::Transport(format!(
                        "peer identity token verification failed: {e}"
                    ))
                })?;
                let did = identity::Identity::did(&token_identity).map_err(|e| {
                    crate::error::Error::Transport(format!("failed to extract peer DID: {e}"))
                })?;
                Ok(Some(did))
            });

            if response.send(result).is_err() {
                debug!(peer_id = %peer_id, "GetPeerIdentity command response dropped - caller cancelled");
            }
        });
    }

    pub(super) fn handle_create_replicator(
        &mut self,
        peer_id: PeerId,
        collections: Vec<String>,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        debug!(peer_id = %peer_id, collections = ?collections, "Creating replicator");
        let peer_str = peer_id.to_string();
        self.replicators
            .set_peer_collections(&peer_str, &collections);
        if response.send(Ok(())).is_err() {
            debug!(peer_id = %peer_id, "CreateReplicator command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_delete_replicator(
        &mut self,
        peer_id: PeerId,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        debug!(peer_id = %peer_id, "Deleting replicator");
        self.replicators.remove_peer(&peer_id.to_string());
        if response.send(Ok(())).is_err() {
            debug!(peer_id = %peer_id, "DeleteReplicator command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_remove_replicator_collections(
        &mut self,
        peer_id: PeerId,
        collections: Vec<String>,
        response: tokio::sync::oneshot::Sender<Result<bool>>,
    ) {
        debug!(
            peer_id = %peer_id,
            collections = ?collections,
            "Removing collections from replicator"
        );

        let peer_str = peer_id.to_string();
        let fully_deleted = self
            .replicators
            .remove_peer_collections(&peer_str, &collections);

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
}
