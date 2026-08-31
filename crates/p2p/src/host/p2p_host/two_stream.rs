//! Two-stream protocol event handling.

use iroh_bitswap::Store;
use tracing::{debug, error, info, warn};

use crate::explicit_replay;
use crate::two_stream::TwoStreamEvent;

use super::P2PHost;
use crate::host::event::HostEvent;

fn identity_request_targets_peer(
    request: &crate::message::IdentityRequest,
    authenticated_peer: &libp2p::PeerId,
) -> bool {
    request.peer_id == authenticated_peer.to_string()
}

fn identity_response_for_request(
    request: &crate::message::IdentityRequest,
    authenticated_peer: &libp2p::PeerId,
    node_identity: Option<&identity::RawIdentity>,
) -> crate::message::IdentityResponse {
    if !identity_request_targets_peer(request, authenticated_peer) {
        return crate::message::IdentityResponse::error(
            &request.message_id,
            "identity request peer did not match authenticated transport peer",
        );
    }

    let Some(node_identity) = node_identity else {
        return crate::message::IdentityResponse::identity_unconfigured(&request.message_id);
    };

    match identity::new_token(
        node_identity,
        std::time::Duration::from_secs(60 * 60 * 24),
        Some(authenticated_peer.to_string()),
        None,
    ) {
        Ok(token) => crate::message::IdentityResponse::success(&request.message_id, token),
        Err(error) => crate::message::IdentityResponse::error(
            &request.message_id,
            &format!("failed to generate identity token: {error}"),
        ),
    }
}

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
                if !self
                    .forward_event(HostEvent::TwoStreamRequest {
                        peer_id,
                        request,
                        is_explicit_replicator,
                        explicit_replay_authorization,
                    })
                    .await
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
                if !self
                    .forward_event(HostEvent::DocSyncRequest { peer_id, request })
                    .await
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
                if !self
                    .forward_event(HostEvent::DocSyncReply { peer_id, reply })
                    .await
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
                if !self
                    .forward_event(HostEvent::BranchableSyncRequest { peer_id, request })
                    .await
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
                if !self
                    .forward_event(HostEvent::BranchableSyncReply { peer_id, reply })
                    .await
                {
                    error!(
                        peer_id = %peer_id,
                        "Failed to send BranchableSyncReply event - receiver dropped"
                    );
                }
            }
            TwoStreamEvent::IdentityRequest { peer_id, request } => {
                let mut reply = identity_response_for_request(
                    &request,
                    &peer_id,
                    self.node_identity.as_deref(),
                );

                match crate::signing::sign_message(self.keypair(), &mut reply) {
                    Ok(()) => {
                        let handler = self.two_stream_handler.clone();
                        self.spawned_tasks.spawn(async move {
                            let mut h = handler.lock().await.clone();
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
                    Ok(_did) => {}
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
                if !self
                    .forward_event(HostEvent::CarFetchRequest { peer_id, root_cid })
                    .await
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
                if !self
                    .forward_event(HostEvent::CarFetchResponse {
                        peer_id,
                        root_cid,
                        car_data,
                    })
                    .await
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
                match defra_core::cbor::to_vec(&request) {
                    Ok(data) => {
                        if !self
                            .forward_event(HostEvent::SEArtifactsReceived { peer_id, data })
                            .await
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
                if !self
                    .forward_event(HostEvent::SEQueryRequest { peer_id, request })
                    .await
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
                if !self
                    .forward_event(HostEvent::SEQueryReply { peer_id, reply })
                    .await
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
            TwoStreamEvent::ManageRequest { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %request.message_id,
                    "Host received manage request via two-stream protocol"
                );
                if !self
                    .forward_event(HostEvent::ManageRequest { peer_id, request })
                    .await
                {
                    error!(peer_id = %peer_id, "Failed to send ManageRequest event");
                }
            }
            TwoStreamEvent::ManageReply { peer_id, reply } => {
                debug!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    "Host received manage reply via two-stream protocol"
                );
                if !self
                    .forward_event(HostEvent::ManageReply { peer_id, reply })
                    .await
                {
                    error!(peer_id = %peer_id, "Failed to send ManageReply event");
                }
            }
            TwoStreamEvent::ManageQueryRequest { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %request.message_id,
                    "Host received manage query request via two-stream protocol"
                );
                if !self
                    .forward_event(HostEvent::ManageQueryRequest { peer_id, request })
                    .await
                {
                    error!(peer_id = %peer_id, "Failed to send ManageQueryRequest event");
                }
            }
            TwoStreamEvent::ManageQueryReply { peer_id, reply } => {
                debug!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    "Host received manage query reply via two-stream protocol"
                );
                if !self
                    .forward_event(HostEvent::ManageQueryReply { peer_id, reply })
                    .await
                {
                    error!(peer_id = %peer_id, "Failed to send ManageQueryReply event");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{identity_request_targets_peer, identity_response_for_request};
    use identity::Identity as _;

    #[test]
    fn identity_request_cannot_forward_audience_for_another_transport_peer() {
        let authenticated = libp2p::PeerId::random();
        let victim = libp2p::PeerId::random();

        let forwarded = crate::message::IdentityRequest::new(victim.to_string());
        assert!(!identity_request_targets_peer(&forwarded, &authenticated));
        let node_identity =
            identity::RawIdentity::from_private_key(crypto::generate_ed25519().unwrap()).unwrap();
        let rejected =
            identity_response_for_request(&forwarded, &authenticated, Some(&node_identity));
        assert!(rejected.err_message.is_some());
        assert!(rejected.identity_token.is_empty());

        let bound = crate::message::IdentityRequest::new(authenticated.to_string());
        assert!(identity_request_targets_peer(&bound, &authenticated));
        let accepted = identity_response_for_request(&bound, &authenticated, Some(&node_identity));
        assert!(accepted.err_message.is_none());
        let token_identity = identity::from_token(&accepted.identity_token).unwrap();
        identity::verify_auth_token(&token_identity, &authenticated.to_string()).unwrap();
        assert_eq!(
            identity::Identity::did(&token_identity).unwrap(),
            node_identity.did().unwrap()
        );
    }

    #[test]
    fn identity_request_without_node_identity_returns_explicit_none_sentinel() {
        let authenticated = libp2p::PeerId::random();
        let request = crate::message::IdentityRequest::new(authenticated.to_string());
        let response = identity_response_for_request(&request, &authenticated, None);
        assert_eq!(
            response.err_message.as_deref(),
            Some(crate::message::IDENTITY_UNCONFIGURED_ERROR)
        );
        assert!(response.identity_token.is_empty());
    }
}
