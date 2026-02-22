//! Two-stream protocol event handling.

use iroh_bitswap::Store;
use tracing::{debug, error, info, warn};

use crate::two_stream::TwoStreamEvent;

use super::P2PHost;
use crate::host::event::HostEvent;

impl<S: Store> P2PHost<S> {
    /// Handle two-stream protocol events.
    pub(super) async fn handle_two_stream_event(&mut self, event: TwoStreamEvent) {
        match event {
            TwoStreamEvent::InboundRequest { peer_id, request } => {
                info!(
                    peer_id = %peer_id,
                    message_id = %request.metadata.message_id,
                    doc_id = %request.doc_id,
                    "Host received PushLog request via two-stream protocol"
                );
                if self
                    .event_tx
                    .send(HostEvent::TwoStreamRequest { peer_id, request })
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
                    message_id = %request.metadata.message_id,
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
                    message_id = %request.metadata.message_id,
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
            TwoStreamEvent::DecodeError { peer_id, error } => {
                warn!(
                    peer_id = %peer_id,
                    error = %error,
                    "Failed to decode two-stream message"
                );
            }
        }
    }
}
