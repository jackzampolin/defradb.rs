//! P2P host handle for interacting with the host.

use cid::Cid;
use libp2p::{gossipsub, identity::Keypair, Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::message::{PushLogBroadcast, PushLogReply, PushLogRequest};
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::QueryId;

use super::command::HostCommand;
use super::ResponseChannel;

/// Handle to interact with the P2P host.
#[derive(Clone)]
pub struct P2PHostHandle {
    pub(super) command_tx: mpsc::Sender<HostCommand>,
    /// Local public key encoded as protobuf (for use in P2P message metadata).
    local_public_key_proto: Vec<u8>,
    /// Local peer ID for message metadata.
    local_peer_id: PeerId,
    /// Keypair for signing messages.
    keypair: Keypair,
}

impl P2PHostHandle {
    /// Create a new handle (internal use only).
    pub(super) fn new(
        command_tx: mpsc::Sender<HostCommand>,
        local_public_key_proto: Vec<u8>,
        local_peer_id: PeerId,
        keypair: Keypair,
    ) -> Self {
        Self {
            command_tx,
            local_public_key_proto,
            local_peer_id,
            keypair,
        }
    }

    /// Get the local public key encoded as protobuf.
    ///
    /// This is used for setting the pubkey field in P2P message metadata.
    pub fn local_public_key_proto(&self) -> &[u8] {
        &self.local_public_key_proto
    }

    /// Get the local peer ID.
    ///
    /// This is synchronous since we cache the peer ID in the handle.
    pub fn local_peer_id_cached(&self) -> PeerId {
        self.local_peer_id
    }

    /// Get a reference to the keypair for signing messages.
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    /// Start listening on the given multiaddress.
    pub async fn listen(&self, addr: Multiaddr) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::Listen {
                addr,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Dial a peer at the given addresses.
    pub async fn dial(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::Dial {
                peer_id,
                addrs,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a PushLog request to a peer and wait for the response.
    pub async fn send_pushlog(
        &self,
        peer_id: PeerId,
        request: PushLogRequest,
    ) -> Result<PushLogReply> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendPushLog {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a PushLog response through a response channel.
    ///
    /// This is used to respond to incoming PushLog requests received via
    /// `HostEvent::PushLogRequest`.
    pub async fn send_pushlog_response(
        &self,
        channel: ResponseChannel,
        reply: PushLogReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendPushLogResponse {
                channel,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Get the local peer ID.
    pub async fn local_peer_id(&self) -> Result<PeerId> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::LocalPeerId {
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Get addresses the host is listening on.
    pub async fn listen_addresses(&self) -> Result<Vec<Multiaddr>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::ListenAddresses {
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Get list of connected peers.
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::ConnectedPeers {
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Shutdown the P2P host.
    pub async fn shutdown(&self) -> Result<()> {
        self.command_tx
            .send(HostCommand::Shutdown)
            .await
            .map_err(|_| Error::ChannelSend)
    }

    /// Subscribe to a GossipSub topic.
    ///
    /// Returns `true` if this is a new subscription, `false` if already subscribed.
    pub async fn subscribe(&self, topic: DefraTopic) -> Result<bool> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::Subscribe {
                topic,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Unsubscribe from a GossipSub topic.
    ///
    /// Returns `true` if was subscribed, `false` if wasn't subscribed.
    pub async fn unsubscribe(&self, topic: DefraTopic) -> Result<bool> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::Unsubscribe {
                topic,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Publish a message to a GossipSub topic.
    ///
    /// Returns the message ID on success.
    pub async fn publish(
        &self,
        topic: DefraTopic,
        message: PushLogBroadcast,
    ) -> Result<gossipsub::MessageId> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::Publish {
                topic,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Get list of subscribed topics.
    pub async fn subscribed_topics(&self) -> Result<Vec<String>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SubscribedTopics {
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Start a Bitswap sync operation to fetch missing blocks.
    ///
    /// This initiates a DAG sync that will fetch the specified block and all
    /// its linked blocks from the given providers.
    ///
    /// # Arguments
    ///
    /// * `cid` - The root CID to sync
    /// * `providers` - Peer IDs that may have the blocks
    /// * `missing` - Known missing CIDs to fetch
    ///
    /// # Returns
    ///
    /// A `QueryId` that can be used to track progress and cancel the query.
    pub async fn bitswap_sync(
        &self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> Result<QueryId> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::BitswapSync {
                cid,
                providers,
                missing,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Cancel an in-progress Bitswap query.
    ///
    /// # Returns
    ///
    /// `true` if a query was cancelled, `false` if no query was found.
    pub async fn bitswap_cancel(&self, query_id: QueryId) -> Result<bool> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::BitswapCancel {
                query_id,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Set (add/update) a replicator.
    ///
    /// Adds the peer as a replicator for the specified collections.
    /// If the peer is already a replicator, updates their collections.
    pub async fn set_replicator(&self, peer_id: PeerId, collections: Vec<String>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SetReplicator {
                peer_id,
                collections,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Delete a replicator.
    ///
    /// Removes the peer from all collections they were replicating.
    pub async fn delete_replicator(&self, peer_id: PeerId) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::DeleteReplicator {
                peer_id,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Remove specific collections from a replicator.
    ///
    /// This matches Go DefraDB's partial removal behavior:
    /// - Removes only the specified collections from the replicator
    /// - If the replicator has no collections left, they are fully deleted
    ///
    /// Returns `true` if the replicator was fully deleted (no collections remain).
    pub async fn remove_replicator_collections(
        &self,
        peer_id: PeerId,
        collections: Vec<String>,
    ) -> Result<bool> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::RemoveReplicatorCollections {
                peer_id,
                collections,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Get all registered replicators.
    pub async fn get_all_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::GetAllReplicators {
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Get replicator info for a specific peer.
    ///
    /// Returns None if the peer is not a replicator.
    pub async fn get_replicator(&self, peer_id: PeerId) -> Result<Option<ReplicatorInfo>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::GetReplicator {
                peer_id,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }

    /// Send a PushLog response via two-stream protocol (Go compatibility).
    ///
    /// This sends a response on a NEW stream, matching Go's two-stream pattern.
    pub async fn send_two_stream_response(
        &self,
        peer_id: PeerId,
        reply: PushLogReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendTwoStreamResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a PushLog request via two-stream protocol and wait for response.
    ///
    /// This uses Go's two-stream pattern: request on one stream, response on another.
    pub async fn send_two_stream_request(
        &self,
        peer_id: PeerId,
        request: PushLogRequest,
    ) -> Result<PushLogReply> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendTwoStreamRequest {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a DocSync response via two-stream protocol.
    ///
    /// This sends a response on a NEW stream, matching Go's two-stream pattern.
    pub async fn send_doc_sync_response(
        &self,
        peer_id: PeerId,
        reply: crate::message::DocSyncReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendDocSyncResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a DocSync request via two-stream protocol.
    ///
    /// The request is sent asynchronously. The response will arrive as a
    /// HostEvent::DocSyncReply which the coordinator will handle.
    pub async fn send_doc_sync_request(
        &self,
        peer_id: PeerId,
        request: crate::message::DocSyncRequest,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendDocSyncRequest {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a BranchableSync response via two-stream protocol.
    pub async fn send_branchable_sync_response(
        &self,
        peer_id: PeerId,
        reply: crate::message::BranchableSyncReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendBranchableSyncResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a BranchableSync request via two-stream protocol (fire-and-forget).
    pub async fn send_branchable_sync_request(
        &self,
        peer_id: PeerId,
        request: crate::message::BranchableSyncRequest,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendBranchableSyncRequest {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send SE artifacts to a peer via the SE two-stream protocol.
    ///
    /// This sends a PushSEArtifactsRequest on the SE request protocol.
    /// The response is not awaited (fire-and-forget).
    pub async fn send_se_artifacts(
        &self,
        peer_id: PeerId,
        request: crate::message::PushSEArtifactsRequest,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendSEArtifacts {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Get connected peers with their full multiaddrs (Go-compatible ActivePeers).
    pub async fn peer_addresses(&self) -> Result<Vec<String>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::PeerAddresses {
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
    }
}
