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
