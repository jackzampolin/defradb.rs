//! Host event handling for the sync coordinator.

mod bitswap;
mod branchable_sync;
pub(crate) mod car;
mod doc_sync;
mod gossip;
mod pushlog;

use blockstore::Blockstore;

use super::SyncCoordinator;
use crate::error::Result;
use crate::host::HostEvent;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Handle an event from the P2P host.
    ///
    /// This should be called from the event loop that processes HostEvents.
    pub async fn handle_host_event(&self, event: HostEvent) -> Result<()> {
        match event {
            HostEvent::PeerConnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer connected");
                self.peer_state.peer_connected(peer_id);
            }
            HostEvent::PeerDisconnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer disconnected");
                self.peer_state.peer_disconnected(&peer_id);
            }
            HostEvent::PeerSubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
                self.peer_state.peer_subscribed(&peer_id, topic);
            }
            HostEvent::PeerUnsubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
                self.peer_state.peer_unsubscribed(&peer_id, &topic);
            }
            HostEvent::GossipMessage {
                propagation_source,
                message,
                topic,
                ..
            } => {
                self.handle_gossip_message(propagation_source, message, topic)
                    .await?;
            }
            HostEvent::PushLogRequest {
                peer_id,
                request,
                channel,
            } => {
                self.handle_pushlog_request(peer_id, request, channel)
                    .await?;
            }
            HostEvent::TwoStreamRequest { peer_id, request } => {
                self.handle_two_stream_request(peer_id, request).await?;
            }
            HostEvent::BitswapBlockReceived {
                query_id,
                cid,
                data,
            } => {
                self.handle_bitswap_block_received(query_id, cid, data)
                    .await?;
            }
            HostEvent::BitswapComplete {
                query_id,
                success,
                error,
            } => {
                self.handle_bitswap_complete(query_id, success, error)
                    .await?;
            }
            HostEvent::DocSyncRequest { peer_id, request } => {
                self.handle_doc_sync_request(peer_id, request).await?;
            }
            HostEvent::DocSyncReply { peer_id, reply } => {
                self.handle_doc_sync_reply(peer_id, reply).await?;
            }
            HostEvent::BranchableSyncRequest { peer_id, request } => {
                self.handle_branchable_sync_request(peer_id, request)
                    .await?;
            }
            HostEvent::BranchableSyncReply { peer_id, reply } => {
                self.handle_branchable_sync_reply(peer_id, reply).await?;
            }
            HostEvent::CarFetchRequest { peer_id, root_cid } => {
                self.handle_car_fetch_request(peer_id, root_cid).await?;
            }
            HostEvent::CarFetchResponse {
                peer_id,
                root_cid,
                car_data,
            } => {
                self.handle_car_fetch_response(peer_id, root_cid, car_data)
                    .await?;
            }
            other => {
                // Other events (peer discovery, listening, etc.) don't need sync handling
                tracing::trace!(event = ?other, "Ignoring non-sync host event");
            }
        }
        Ok(())
    }
}
