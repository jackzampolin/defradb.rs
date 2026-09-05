//! P2P host handle for interacting with the host.

use cid::Cid;
use libp2p::{gossipsub, identity::Keypair, Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::explicit_replay::ExplicitReplayCapabilityCache;
use crate::message::{PushLogBroadcast, PushLogReply, PushLogRequest};
use crate::replicator::ReplicatorInfo;
use crate::signing::sign_message;
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
    /// Optional explicit replay capabilities keyed by (peer_id, collection_id).
    explicit_replay_capabilities: ExplicitReplayCapabilityCache,
}

impl P2PHostHandle {
    fn set_explicit_replay_capability_inner(
        &self,
        peer_id: &PeerId,
        collections: &[String],
        capability: &str,
    ) {
        for collection_id in collections {
            if let Err(error) = self.explicit_replay_capabilities.set(
                &self.local_peer_id.to_string(),
                &peer_id.to_string(),
                collection_id,
                capability,
            ) {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id,
                    error = %error,
                    "Refusing to cache invalid explicit replay capability"
                );
            }
        }
    }

    fn clear_explicit_replay_capability_inner(&self, peer_id: &PeerId, collections: &[String]) {
        self.explicit_replay_capabilities
            .clear(&peer_id.to_string(), collections);
    }

    fn clear_all_explicit_replay_capabilities_inner(&self, peer_id: &PeerId) {
        let peer_id = peer_id.to_string();
        self.explicit_replay_capabilities.clear_all(&peer_id);
    }

    fn attach_explicit_replay_capability(&self, peer_id: &PeerId, request: &mut PushLogRequest) {
        self.explicit_replay_capabilities
            .attach(&peer_id.to_string(), request);
    }

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
            explicit_replay_capabilities: ExplicitReplayCapabilityCache::default(),
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

    /// Disconnect from a peer, hanging up any live connection.
    ///
    /// Idempotent: disconnecting an already-absent peer returns `Ok(())`.
    pub async fn disconnect(&self, peer_id: PeerId) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::Disconnect {
                peer_id,
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

    /// Resolve and cache a peer's DEFRA identity through the Go-compatible identity protocol.
    pub async fn get_peer_identity(&self, peer_id: PeerId) -> Result<Option<identity::Did>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::GetPeerIdentity {
                peer_id,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Get all peers known to GossipSub for a topic (mesh + non-mesh).
    pub async fn topic_peers(&self, topic: DefraTopic) -> Result<Vec<PeerId>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::TopicPeers {
                topic,
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

    /// Subscribe to an arbitrary topic string (for pubsub_rpc response
    /// sub-topics that aren't enumerated in [`DefraTopic`]).
    pub async fn subscribe_raw(&self, topic: String) -> Result<bool> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SubscribeRaw {
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
    #[tracing::instrument(
        name = "p2p.gossip.publish",
        level = "debug",
        skip(self, message),
        fields(topic = ?topic),
    )]
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

    /// Publish raw bytes to a GossipSub topic.
    ///
    /// Used by the `pubsub_rpc` layer (issue #828) where the publisher
    /// controls the wire format end-to-end — the request is an opaque
    /// CBOR-encoded struct or an `InternalResponse` envelope. The topic
    /// string may be a dynamically-named per-peer response sub-topic, so
    /// it is passed as a plain `String` rather than a `DefraTopic`
    /// variant.
    pub async fn publish_raw(&self, topic: String, data: Vec<u8>) -> Result<gossipsub::MessageId> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::PublishRaw {
                topic,
                data,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Register a topic as `pubsub_rpc`-owned.
    ///
    /// Incoming messages on this topic (and its `<topic>/<peer>/_response`
    /// sub-topics) will be delivered to the consumer as
    /// [`super::event::HostEvent::GossipRawMessage`] instead of the default
    /// PushLog-broadcast decoding. Safe to call multiple times with the
    /// same topic; the host stores a set, not a list.
    pub async fn register_pubsub_rpc_topic(&self, topic: String) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::RegisterPubsubRpcTopic {
                topic,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)
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
    pub async fn create_replicator(&self, peer_id: PeerId, collections: Vec<String>) -> Result<()> {
        let info = ReplicatorInfo::from_raw(peer_id.to_string(), collections, Vec::new());
        self.create_replicator_info(peer_id, info).await
    }

    /// Set (add/update) a replicator using the full persisted metadata record.
    pub async fn create_replicator_info(
        &self,
        peer_id: PeerId,
        info: ReplicatorInfo,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::CreateReplicator {
                peer_id,
                info,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Cache an explicit replay capability for future two-stream pushes.
    ///
    /// This is primarily intended for tests and lower-level integrations.
    pub fn set_explicit_replay_capability(
        &self,
        peer_id: PeerId,
        collections: &[String],
        capability: &str,
    ) {
        self.set_explicit_replay_capability_inner(&peer_id, collections, capability)
    }

    /// Clear cached explicit replay capabilities for the provided collections.
    pub fn clear_explicit_replay_capability(&self, peer_id: PeerId, collections: &[String]) {
        self.clear_explicit_replay_capability_inner(&peer_id, collections);
    }

    /// Check whether the cached explicit replay capability matches the provided value.
    pub fn explicit_replay_capability_matches(
        &self,
        peer_id: PeerId,
        collection: &str,
        capability: Option<&str>,
    ) -> bool {
        self.explicit_replay_capabilities
            .matches(&peer_id.to_string(), collection, capability)
    }

    /// Delete a replicator.
    ///
    /// Removes the peer from all collections they were replicating.
    pub async fn delete_replicator(&self, peer_id: PeerId) -> Result<()> {
        if let Some(info) = self.get_replicator(peer_id).await? {
            self.clear_explicit_replay_capability_inner(&peer_id, &info.collections);
        } else {
            self.clear_all_explicit_replay_capabilities_inner(&peer_id);
        }

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
        self.clear_explicit_replay_capability_inner(&peer_id, &collections);

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
    pub async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::ListReplicators {
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
    #[tracing::instrument(
        name = "p2p.push_log.send",
        level = "debug",
        skip(self, request),
        fields(peer = %peer_id),
    )]
    pub async fn send_two_stream_request(
        &self,
        peer_id: PeerId,
        mut request: PushLogRequest,
    ) -> Result<PushLogReply> {
        self.attach_explicit_replay_capability(&peer_id, &mut request);
        sign_message(self.keypair(), &mut request)?;

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
    /// The response is not awaited (fire-and-forget). The request is signed so
    /// Go peers (which verify on receipt) accept it.
    pub async fn send_se_artifacts(
        &self,
        peer_id: PeerId,
        mut request: crate::message::PushSEArtifactsRequest,
    ) -> Result<()> {
        sign_message(self.keypair(), &mut request)?;
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

    /// Send a PushSEArtifacts reply on the SE response protocol.
    ///
    /// Acknowledges an inbound artifact push. Go's push waits for this reply, so
    /// a Rust replicator must send it after storing the artifacts.
    pub async fn send_se_artifacts_response(
        &self,
        peer_id: PeerId,
        reply: crate::message::PushSEArtifactsReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendSEArtifactsResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send an SE query request to a peer via the SE query two-stream protocol.
    ///
    /// The response arrives asynchronously as [`HostEvent::SEQueryReply`].
    pub async fn send_se_query_request(
        &self,
        peer_id: PeerId,
        request: crate::message::QuerySEArtifactsRequest,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendSEQueryRequest {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send an SE query response to a peer via the SE query two-stream protocol.
    pub async fn send_se_query_response(
        &self,
        peer_id: PeerId,
        reply: crate::message::QuerySEArtifactsReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendSEQueryResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a management mutate request to a peer via the manage two-stream protocol.
    ///
    /// The response arrives asynchronously as [`HostEvent::ManageReply`].
    pub async fn send_manage_request(
        &self,
        peer_id: PeerId,
        request: crate::message::ManageRequest,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendManageRequest {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a management mutate response to a peer via the manage two-stream protocol.
    pub async fn send_manage_response(
        &self,
        peer_id: PeerId,
        reply: crate::message::ManageReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendManageResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a management query request to a peer via the manage query two-stream protocol.
    ///
    /// The response arrives asynchronously as [`HostEvent::ManageQueryReply`].
    pub async fn send_manage_query_request(
        &self,
        peer_id: PeerId,
        request: crate::message::ManageQueryRequest,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendManageQueryRequest {
                peer_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a management query response to a peer via the manage query two-stream protocol.
    pub async fn send_manage_query_response(
        &self,
        peer_id: PeerId,
        reply: crate::message::ManageQueryReply,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendManageQueryResponse {
                peer_id,
                reply,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a CAR request to a peer (request DAG as CARv1).
    pub async fn send_car_request(&self, peer_id: PeerId, root_cid: Cid) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendCarRequest {
                peer_id,
                root_cid,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Send a CAR response to a peer (CARv1 bytes).
    pub async fn send_car_response(&self, peer_id: PeerId, car_data: Vec<u8>) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(HostCommand::SendCarResponse {
                peer_id,
                car_data,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::ChannelSend)?;
        response_rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Poll until a peer is connected or timeout expires.
    ///
    /// Uses 50ms polling interval (matches Go behavior).
    pub async fn poll_until_connected(
        &self,
        peer_id: PeerId,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let connected = self.connected_peers().await?;
            if connected.contains(&peer_id) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::ConnectionTimeout(peer_id.to_string()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Resolve full multiaddr strings for connected peers.
    ///
    /// Retries up to 5 times with 100ms delay, falling back to
    /// a caller-provided cache for peers not in the host's address book.
    pub async fn resolve_peer_addresses(
        &self,
        connected: &[PeerId],
        get_cached: impl Fn(&str) -> Option<String>,
    ) -> Result<Vec<String>> {
        let mut host_addrs = Vec::new();
        let mut covered = std::collections::HashSet::new();

        for attempt in 0..5 {
            host_addrs = self.peer_addresses().await?;
            covered.clear();
            for addr_str in &host_addrs {
                if let Some(pid) = addr_str.rsplit("/p2p/").next() {
                    covered.insert(pid.to_string());
                }
            }
            let all_resolved = connected.iter().all(|pid| {
                let pid_str = pid.to_string();
                covered.contains(&pid_str) || get_cached(&pid_str).is_some()
            });
            if all_resolved || attempt == 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Append cached addresses for unresolved peers
        for pid in connected {
            let pid_str = pid.to_string();
            if !covered.contains(&pid_str) {
                if let Some(cached_addr) = get_cached(&pid_str) {
                    host_addrs.push(cached_addr);
                }
            }
        }

        Ok(host_addrs)
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
