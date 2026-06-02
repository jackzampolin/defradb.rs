//! Two-stream protocol event handling.

use iroh_bitswap::Store;
use tracing::{debug, error, info, warn};

use crate::explicit_replay;
use crate::two_stream::TwoStreamEvent;

use super::P2PHost;
use crate::host::event::HostEvent;

impl<S: Store> P2PHost<S> {
    /// Handle two-stream protocol events.
    pub(super) async fn handle_two_stream_event(&mut self, event: TwoStreamEvent) {
        match event {
            TwoStreamEvent::InboundRequest { peer_id, request } => {
                let explicit_replay_authorization = request
                    .explicit_replay_capability
                    .as_deref()
                    .and_then(|capability| {
                        match explicit_replay::verify_capability(
                            capability,
                            &peer_id.to_string(),
                            &self.swarm.local_peer_id().to_string(),
                            &request.collection_id,
                        ) {
                            Ok(authorization) => {
                                info!(
                                    peer_id = %peer_id,
                                    authorizer_did = %authorization.authorizer_did,
                                    collection_id = %authorization.collection_id,
                                    "Accepted explicit replay capability for two-stream PushLog"
                                );
                                Some(authorization)
                            }
                            Err(error) => {
                                warn!(
                                    peer_id = %peer_id,
                                    creator = %request.creator,
                                    error = %error,
                                    "Rejected explicit replay capability for two-stream PushLog"
                                );
                                None
                            }
                        }
                    });
                let is_explicit_replicator = explicit_replay_authorization.is_some();
                info!(
                    peer_id = %peer_id,
                    message_id = %request.message_id,
                    doc_id = %request.doc_id,
                    "Host received PushLog request via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::TwoStreamRequest {
                        peer_id,
                        request,
                        is_explicit_replicator,
                        explicit_replay_authorization,
                    })
                    .await
                    .is_err()
                {
                    error!(
                        peer_id = %peer_id,
                        "Failed to send TwoStreamRequest event - receiver dropped"
                    );
                } else {
                    info!(peer_id = %peer_id, "Forwarded TwoStreamRequest event to coordinator");
                }
            }
            TwoStreamEvent::DocSyncRequest { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %request.message_id,
                    doc_ids = ?request.doc_ids,
                    "Host received DocSync request via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::DocSyncRequest { peer_id, request })
                    .await
                    .is_err()
                {
                    error!(
                        peer_id = %peer_id,
                        "Failed to send DocSyncRequest event - receiver dropped"
                    );
                } else {
                    info!(peer_id = %peer_id, "Forwarded DocSyncRequest event to coordinator");
                }
            }
            TwoStreamEvent::DocSyncReply { peer_id, reply } => {
                debug!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    results_count = reply.results.len(),
                    "Host received DocSync reply via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::DocSyncReply { peer_id, reply })
                    .await
                    .is_err()
                {
                    error!(
                        peer_id = %peer_id,
                        "Failed to send DocSyncReply event - receiver dropped"
                    );
                } else {
                    debug!(peer_id = %peer_id, "Forwarded DocSyncReply event to coordinator");
                }
            }
            TwoStreamEvent::BranchableSyncRequest { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %request.message_id,
                    collection_id = %request.collection_id,
                    "Host received BranchableSync request via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::BranchableSyncRequest { peer_id, request })
                    .await
                    .is_err()
                {
                    error!(
                        peer_id = %peer_id,
                        "Failed to send BranchableSyncRequest event - receiver dropped"
                    );
                }
            }
            TwoStreamEvent::BranchableSyncReply { peer_id, reply } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    collection_id = %reply.collection_id,
                    heads_count = reply.heads.len(),
                    "Host received BranchableSync reply via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::BranchableSyncReply { peer_id, reply })
                    .await
                    .is_err()
                {
                    error!(
                        peer_id = %peer_id,
                        "Failed to send BranchableSyncReply event - receiver dropped"
                    );
                }
            }
            TwoStreamEvent::IdentityRequest { peer_id, request } => {
                let mut reply = match self.node_identity.as_ref() {
                    Some(node_identity) => match identity::new_token(
                        node_identity.as_ref(),
                        std::time::Duration::from_secs(60 * 60 * 24),
                        Some(request.peer_id.clone()),
                        None,
                    ) {
                        Ok(token) => {
                            crate::message::IdentityResponse::success(&request.message_id, token)
                        }
                        Err(error) => crate::message::IdentityResponse::error(
                            &request.message_id,
                            &format!("failed to generate identity token: {error}"),
                        ),
                    },
                    None => crate::message::IdentityResponse::error(
                        &request.message_id,
                        "node identity is not configured",
                    ),
                };

                match crate::signing::sign_message(self.keypair(), &mut reply) {
                    Ok(()) => {
                        let handler = self.two_stream_handler.clone();
                        self.spawned_tasks.spawn(async move {
                            let mut h = handler.lock().await;
                            if let Err(error) = h.send_identity_response(peer_id, reply).await {
                                warn!(peer_id = %peer_id, error = %error, "Failed to send identity response");
                            }
                        });
                    }
                    Err(error) => {
                        warn!(peer_id = %peer_id, error = %error, "Failed to sign identity response");
                    }
                }
            }
            TwoStreamEvent::IdentityReply { peer_id, reply } => {
                if let Some(error_message) = reply.err_message.as_deref() {
                    warn!(peer_id = %peer_id, error = %error_message, "Peer identity request returned error");
                    return;
                }

                match identity::from_token(&reply.identity_token).and_then(|identity| {
                    identity::verify_auth_token(
                        &identity,
                        &self.swarm.local_peer_id().to_string(),
                    )?;
                    identity::Identity::did(&identity)
                }) {
                    Ok(did) => {
                        self.peer_identities.insert(peer_id, did);
                    }
                    Err(error) => {
                        warn!(peer_id = %peer_id, error = %error, "Failed to verify peer identity token");
                    }
                }
            }
            TwoStreamEvent::CarFetchRequest { peer_id, root_cid } => {
                debug!(
                    peer_id = %peer_id,
                    root_cid = %root_cid,
                    "Host received CAR fetch request"
                );
                if self
                    .event_tx
                    .send(HostEvent::CarFetchRequest { peer_id, root_cid })
                    .await
                    .is_err()
                {
                    error!(peer_id = %peer_id, "Failed to send CarFetchRequest event");
                }
            }
            TwoStreamEvent::CarFetchResponse {
                peer_id,
                root_cid,
                car_data,
            } => {
                debug!(
                    peer_id = %peer_id,
                    root_cid = %root_cid,
                    car_bytes = car_data.len(),
                    "Host received CAR fetch response"
                );
                if self
                    .event_tx
                    .send(HostEvent::CarFetchResponse {
                        peer_id,
                        root_cid,
                        car_data,
                    })
                    .await
                    .is_err()
                {
                    error!(peer_id = %peer_id, "Failed to send CarFetchResponse event");
                }
            }
            TwoStreamEvent::SEArtifactsReceived { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    collection_id = %request.collection_id,
                    artifact_count = request.artifacts.len(),
                    "Host received SE artifacts via two-stream protocol"
                );
                // Re-encode to CBOR for the db layer receiver
                match serde_cbor::to_vec(&request) {
                    Ok(data) => {
                        if self
                            .event_tx
                            .send(HostEvent::SEArtifactsReceived { peer_id, data })
                            .await
                            .is_err()
                        {
                            error!(peer_id = %peer_id, "Failed to send SEArtifactsReceived event");
                        }
                    }
                    Err(e) => {
                        warn!(
                            peer_id = %peer_id,
                            error = %e,
                            "Failed to re-encode SE artifacts for forwarding"
                        );
                    }
                }
            }
            TwoStreamEvent::SEQueryRequest { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %request.message_id,
                    collection_id = %request.collection_id,
                    query_count = request.queries.len(),
                    "Host received SE query request via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::SEQueryRequest { peer_id, request })
                    .await
                    .is_err()
                {
                    error!(peer_id = %peer_id, "Failed to send SEQueryRequest event");
                }
            }
            TwoStreamEvent::SEQueryReply { peer_id, reply } => {
                debug!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    doc_count = reply.doc_ids.len(),
                    "Host received SE query reply via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::SEQueryReply { peer_id, reply })
                    .await
                    .is_err()
                {
                    error!(peer_id = %peer_id, "Failed to send SEQueryReply event");
                }
            }
            TwoStreamEvent::DecodeError { peer_id, error } => {
                warn!(
                    peer_id = %peer_id,
                    error = %error,
                    "Failed to decode two-stream message"
                );
            }
            TwoStreamEvent::ManageRequest { peer_id, .. } => {
                debug!(peer_id = %peer_id, "Received ManageRequest; routing not yet implemented");
            }
            TwoStreamEvent::ManageReply { peer_id, .. } => {
                debug!(peer_id = %peer_id, "Received ManageReply; routing not yet implemented");
            }
            TwoStreamEvent::ManageQueryRequest { peer_id, .. } => {
                debug!(
                    peer_id = %peer_id,
                    "Received ManageQueryRequest; routing not yet implemented"
                );
            }
            TwoStreamEvent::ManageQueryReply { peer_id, .. } => {
                debug!(
                    peer_id = %peer_id,
                    "Received ManageQueryReply; routing not yet implemented"
                );
            }
        }
    }
}
