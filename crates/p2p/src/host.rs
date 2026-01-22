// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! P2P Host implementation for DefraDB.
//!
//! This module provides the main P2P host that manages the libp2p swarm,
//! handles peer connections, and coordinates CRDT synchronization.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cid::Cid;
use futures::StreamExt;
use iroh_bitswap::{BitswapEvent, Store};
use libp2p::{
    gossipsub, identity::Keypair, mdns, noise, request_response, swarm::SwarmEvent, tcp, yamux,
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::QueryId;

use crate::behaviour::{DefraBehaviour, DefraEvent};
use crate::bitswap::ReplicatorRegistry;
use crate::error::{Error, Result};
use crate::message::{PushLogBroadcast, PushLogReply, PushLogRequest};
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::two_stream::{TwoStreamEvent, TwoStreamHandler, TwoStreamRunner};

/// Default idle connection timeout.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Commands that can be sent to the P2P host.
#[derive(Debug)]
pub enum HostCommand {
    /// Start listening on an address.
    Listen {
        addr: Multiaddr,
        response: oneshot::Sender<Result<()>>,
    },

    /// Dial a peer at the given addresses.
    Dial {
        peer_id: PeerId,
        addrs: Vec<Multiaddr>,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a PushLog request to a peer.
    SendPushLog {
        peer_id: PeerId,
        request: PushLogRequest,
        response: oneshot::Sender<Result<PushLogReply>>,
    },

    /// Send a PushLog response through a response channel.
    SendPushLogResponse {
        channel: ResponseChannel,
        reply: PushLogReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Get the local peer ID.
    LocalPeerId { response: oneshot::Sender<PeerId> },

    /// Get listening addresses.
    ListenAddresses {
        response: oneshot::Sender<Vec<Multiaddr>>,
    },

    /// Get connected peers.
    ConnectedPeers {
        response: oneshot::Sender<Vec<PeerId>>,
    },

    /// Subscribe to a GossipSub topic.
    Subscribe {
        topic: DefraTopic,
        response: oneshot::Sender<Result<bool>>,
    },

    /// Unsubscribe from a GossipSub topic.
    Unsubscribe {
        topic: DefraTopic,
        response: oneshot::Sender<Result<bool>>,
    },

    /// Publish a message to a GossipSub topic.
    Publish {
        topic: DefraTopic,
        message: PushLogBroadcast,
        response: oneshot::Sender<Result<gossipsub::MessageId>>,
    },

    /// Get subscribed topics.
    SubscribedTopics {
        response: oneshot::Sender<Vec<String>>,
    },

    /// Shutdown the host.
    Shutdown,

    /// Start a Bitswap sync operation to fetch missing blocks.
    BitswapSync {
        cid: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
        response: oneshot::Sender<Result<QueryId>>,
    },

    /// Cancel a Bitswap query.
    BitswapCancel {
        query_id: QueryId,
        response: oneshot::Sender<bool>,
    },

    /// Set (add/update) a replicator.
    ///
    /// Adds the peer as a replicator for the specified collections.
    /// If the peer is already a replicator, updates their collections.
    SetReplicator {
        peer_id: PeerId,
        collections: Vec<String>,
        response: oneshot::Sender<Result<()>>,
    },

    /// Delete a replicator.
    ///
    /// Removes the peer from all collections they were replicating.
    DeleteReplicator {
        peer_id: PeerId,
        response: oneshot::Sender<Result<()>>,
    },

    /// Remove specific collections from a replicator.
    ///
    /// If the replicator has no collections left after removal, they are deleted.
    /// This matches Go DefraDB's partial removal behavior.
    RemoveReplicatorCollections {
        peer_id: PeerId,
        collections: Vec<String>,
        response: oneshot::Sender<Result<bool>>, // true if replicator was fully deleted
    },

    /// Get all registered replicators.
    GetAllReplicators {
        response: oneshot::Sender<Vec<ReplicatorInfo>>,
    },

    /// Get replicator info for a specific peer.
    GetReplicator {
        peer_id: PeerId,
        response: oneshot::Sender<Option<ReplicatorInfo>>,
    },

    /// Send a PushLog response via two-stream protocol (Go compatibility).
    SendTwoStreamResponse {
        peer_id: PeerId,
        reply: PushLogReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a PushLog request via two-stream protocol (Go compatibility).
    SendTwoStreamRequest {
        peer_id: PeerId,
        request: PushLogRequest,
        response: oneshot::Sender<Result<PushLogReply>>,
    },
}

/// Events emitted by the P2P host.
#[derive(Debug)]
pub enum HostEvent {
    /// A new peer was discovered.
    PeerDiscovered(PeerId),

    /// A peer connection was established.
    PeerConnected(PeerId),

    /// A peer disconnected.
    PeerDisconnected(PeerId),

    /// Received a PushLog request.
    PushLogRequest {
        peer_id: PeerId,
        request: PushLogRequest,
        channel: ResponseChannel,
    },

    /// Started listening on an address.
    Listening(Multiaddr),

    /// Received a GossipSub message.
    GossipMessage {
        /// The peer that propagated the message.
        propagation_source: PeerId,
        /// The message ID.
        message_id: gossipsub::MessageId,
        /// The topic the message was received on.
        topic: String,
        /// The message payload.
        message: PushLogBroadcast,
    },

    /// A peer subscribed to a topic we're also subscribed to.
    PeerSubscribed { peer_id: PeerId, topic: String },

    /// A peer unsubscribed from a topic.
    PeerUnsubscribed { peer_id: PeerId, topic: String },

    /// Bitswap sync progress update.
    BitswapProgress {
        query_id: QueryId,
        missing_count: usize,
    },

    /// Bitswap sync completed (success or failure).
    BitswapComplete {
        query_id: QueryId,
        success: bool,
        error: Option<String>,
    },

    /// Block received via Bitswap - needs to be stored and processed.
    BitswapBlockReceived {
        query_id: QueryId,
        cid: Cid,
        data: Vec<u8>,
    },

    /// Received a PushLog request via two-stream protocol (Go compatibility).
    TwoStreamRequest {
        peer_id: PeerId,
        request: PushLogRequest,
    },
}

/// Opaque response channel for sending PushLog responses.
#[derive(Debug)]
pub struct ResponseChannel(request_response::ResponseChannel<PushLogReply>);

/// Handle to interact with the P2P host.
#[derive(Clone)]
pub struct P2PHostHandle {
    command_tx: mpsc::Sender<HostCommand>,
    /// Local public key encoded as protobuf (for use in P2P message metadata).
    local_public_key_proto: Vec<u8>,
    /// Local peer ID for message metadata.
    local_peer_id: PeerId,
    /// Keypair for signing messages.
    keypair: Keypair,
}

impl P2PHostHandle {
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
}

/// P2P Host that manages the libp2p swarm.
pub struct P2PHost<S: Store> {
    swarm: Swarm<DefraBehaviour<S>>,
    keypair: Keypair,
    command_rx: mpsc::Receiver<HostCommand>,
    event_tx: mpsc::Sender<HostEvent>,
    pending_requests:
        HashMap<request_response::OutboundRequestId, oneshot::Sender<Result<PushLogReply>>>,
    /// Replicator registry for access control
    replicators: Arc<ReplicatorRegistry>,
    /// Two-stream handler for Go compatibility
    two_stream_handler: Arc<tokio::sync::Mutex<TwoStreamHandler>>,
    /// Receiver for two-stream events
    two_stream_event_rx: mpsc::Receiver<TwoStreamEvent>,
    /// Tracked spawned tasks for graceful shutdown
    spawned_tasks: tokio::task::JoinSet<()>,
    /// Bitswap query abort handles for cancellation support
    bitswap_queries: HashMap<QueryId, tokio::task::AbortHandle>,
}

impl<S: Store> P2PHost<S> {
    /// Create a new P2P host with a generated identity and the given blockstore.
    ///
    /// # Arguments
    ///
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    ///
    /// # Returns
    ///
    /// A tuple of (P2PHost, P2PHostHandle, HostEvent receiver, ReplicatorRegistry).
    /// The ReplicatorRegistry is shared and can be used for access control decisions.
    pub async fn new(
        bitswap_store: S,
    ) -> Result<(
        Self,
        P2PHostHandle,
        mpsc::Receiver<HostEvent>,
        Arc<ReplicatorRegistry>,
    )> {
        let keypair = Keypair::generate_ed25519();
        Self::with_keypair(keypair, bitswap_store).await
    }

    /// Create a new P2P host with the given keypair and blockstore.
    ///
    /// # Arguments
    ///
    /// * `keypair` - The identity keypair for this node
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    ///
    /// # Returns
    ///
    /// A tuple of (P2PHost, P2PHostHandle, HostEvent receiver, ReplicatorRegistry).
    /// The ReplicatorRegistry is shared and can be used for access control decisions.
    ///
    /// # Note
    ///
    /// This must be called within a tokio runtime context as Bitswap spawns
    /// background tasks for operations.
    pub async fn with_keypair(
        keypair: Keypair,
        bitswap_store: S,
    ) -> Result<(
        Self,
        P2PHostHandle,
        mpsc::Receiver<HostEvent>,
        Arc<ReplicatorRegistry>,
    )> {
        let local_peer_id = keypair.public().to_peer_id();
        let local_public_key = keypair.public();

        // Encode the public key as protobuf for use in P2P message metadata
        let local_public_key_proto = local_public_key.encode_protobuf();

        info!("Local peer ID: {}", local_peer_id);

        // Pass keypair and blockstore to behaviour for message signing and block exchange
        // DefraBehaviour::new is now async
        let behaviour = DefraBehaviour::new(
            local_peer_id,
            local_public_key,
            keypair.clone(),
            bitswap_store,
        )
        .await
        .map_err(|e| Error::Behaviour(e.to_string()))?;

        let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(|e| Error::Transport(e.to_string()))?
            .with_behaviour(|_key| behaviour)
            .expect("behaviour already constructed")
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
            .build();

        let (command_tx, command_rx) = mpsc::channel(256);
        let (event_tx, event_rx) = mpsc::channel(256);

        let handle = P2PHostHandle {
            command_tx,
            local_public_key_proto,
            local_peer_id,
            keypair: keypair.clone(),
        };

        // Create the replicator registry for access control
        let replicators = Arc::new(ReplicatorRegistry::new());

        // Set up two-stream protocol for Go compatibility
        let mut control = swarm.behaviour().stream.new_control();
        let request_streams = control
            .accept(TwoStreamHandler::request_protocol())
            .map_err(|_| Error::Behaviour("Failed to register request protocol".into()))?;
        let response_streams = control
            .accept(TwoStreamHandler::response_protocol())
            .map_err(|_| Error::Behaviour("Failed to register response protocol".into()))?;

        let two_stream_handler = Arc::new(tokio::sync::Mutex::new(TwoStreamHandler::new(control)));
        let (two_stream_event_tx, two_stream_event_rx) = mpsc::channel(256);

        // Spawn the two-stream runner as a background task
        let runner = TwoStreamRunner::new(
            Arc::clone(&two_stream_handler),
            request_streams,
            response_streams,
            two_stream_event_tx,
        );
        tokio::spawn(runner.run());

        let host = Self {
            swarm,
            keypair,
            command_rx,
            event_tx,
            pending_requests: HashMap::new(),
            replicators: Arc::clone(&replicators),
            two_stream_handler,
            two_stream_event_rx,
            spawned_tasks: tokio::task::JoinSet::new(),
            bitswap_queries: HashMap::new(),
        };

        Ok((host, handle, event_rx, replicators))
    }

    /// Get the local peer ID.
    pub fn local_peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }

    /// Get the keypair.
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    /// Run the P2P host event loop.
    ///
    /// This method runs until shutdown is requested.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    match command {
                        Some(cmd) => {
                            if !self.handle_command(cmd).await {
                                break;
                            }
                        }
                        None => {
                            info!("Command channel closed, shutting down");
                            break;
                        }
                    }
                }
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
                // Handle two-stream protocol events (Go compatibility)
                Some(two_stream_event) = self.two_stream_event_rx.recv() => {
                    self.handle_two_stream_event(two_stream_event).await;
                }
            }
        }

        info!("P2P host shutdown complete");
    }

    /// Handle two-stream protocol events.
    async fn handle_two_stream_event(&mut self, event: TwoStreamEvent) {
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
            TwoStreamEvent::DecodeError { peer_id, error } => {
                warn!(
                    peer_id = %peer_id,
                    error = %error,
                    "Failed to decode two-stream message"
                );
            }
        }
    }

    /// Handle a command from the handle.
    ///
    /// Returns false if the host should shutdown.
    async fn handle_command(&mut self, command: HostCommand) -> bool {
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
                    .send_pushlog_response(channel.0, reply)
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
                info!("Shutdown requested, waiting for {} spawned tasks to complete", self.spawned_tasks.len());
                // Wait for all spawned tasks to complete with a timeout
                let timeout_duration = std::time::Duration::from_secs(5);
                let shutdown_start = std::time::Instant::now();
                while !self.spawned_tasks.is_empty() {
                    if shutdown_start.elapsed() > timeout_duration {
                        warn!("Shutdown timeout exceeded, aborting {} remaining tasks", self.spawned_tasks.len());
                        self.spawned_tasks.abort_all();
                        break;
                    }
                    // Try to join tasks with a short timeout
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        self.spawned_tasks.join_next(),
                    ).await {
                        Ok(Some(Ok(()))) => {
                            debug!("Spawned task completed during shutdown");
                        }
                        Ok(Some(Err(e))) => {
                            warn!("Spawned task failed during shutdown: {}", e);
                        }
                        Ok(None) => break, // No more tasks
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
                                if let Err(e) = event_tx.send(HostEvent::BitswapBlockReceived {
                                    query_id,
                                    cid: block_cid,
                                    data: block_data,
                                }).await {
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
                            let _ = event_tx.send(HostEvent::BitswapComplete {
                                query_id,
                                success,
                                error: if success { None } else { Some(format!("Only fetched {} of {} blocks", fetched, missing_cids.len())) },
                            }).await;
                        }
                        Err(e) => {
                            warn!(query_id = query_id.0, error = %e, "Bitswap fetch failed");
                            let _ = event_tx.send(HostEvent::BitswapComplete {
                                query_id,
                                success: false,
                                error: Some(e.to_string()),
                            }).await;
                        }
                    }
                });

                // Store the abort handle for cancellation support
                self.bitswap_queries.insert(query_id, task_handle.abort_handle());

                if response.send(Ok(query_id)).is_err() {
                    debug!(cid = %cid, "BitswapSync command response dropped - caller cancelled");
                }
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
                    let result = h.send_response(peer_id, reply).await.map_err(|e| e);
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
                    let result = h.send_request(peer_id, request).await.map_err(|e| e);
                    if response.send(result).is_err() {
                        debug!(peer_id = %peer_id, "SendTwoStreamRequest command response dropped - caller cancelled");
                    }
                });
            }
        }
        true
    }

    /// Dial a peer at the given addresses.
    fn dial_peer(&mut self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
        for addr in addrs {
            let dial_addr = addr.with(libp2p::multiaddr::Protocol::P2p(peer_id));
            match self.swarm.dial(dial_addr.clone()) {
                Ok(_) => {
                    debug!("Dialing peer {} at {}", peer_id, dial_addr);
                    return Ok(());
                }
                Err(e) => {
                    warn!("Failed to dial {}: {}", dial_addr, e);
                }
            }
        }
        Err(Error::Dial(format!("Failed to dial peer {}", peer_id)))
    }

    /// Handle a swarm event.
    async fn handle_swarm_event(&mut self, event: SwarmEvent<DefraEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on {}", address);
                if self
                    .event_tx
                    .send(HostEvent::Listening(address.clone()))
                    .await
                    .is_err()
                {
                    warn!(address = %address, "Failed to send Listening event - receiver dropped");
                }
            }

            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to peer: {}", peer_id);
                if self
                    .event_tx
                    .send(HostEvent::PeerConnected(peer_id))
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, "Failed to send PeerConnected event - receiver dropped");
                }
            }

            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer: {}", peer_id);
                if self
                    .event_tx
                    .send(HostEvent::PeerDisconnected(peer_id))
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, "Failed to send PeerDisconnected event - receiver dropped");
                }
            }

            SwarmEvent::Behaviour(DefraEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer_id, addr) in peers {
                    debug!("mDNS discovered peer: {} at {}", peer_id, addr);
                    debug!(peer_id = %peer_id, address = %addr, "Adding external address from mDNS discovery");
                    self.swarm.add_external_address(addr);
                    if self
                        .event_tx
                        .send(HostEvent::PeerDiscovered(peer_id))
                        .await
                        .is_err()
                    {
                        warn!(peer_id = %peer_id, "Failed to send PeerDiscovered event - receiver dropped");
                    }
                }
            }

            SwarmEvent::Behaviour(DefraEvent::Mdns(mdns::Event::Expired(peers))) => {
                for (peer_id, _addr) in peers {
                    debug!("mDNS peer expired: {}", peer_id);
                }
            }

            SwarmEvent::Behaviour(DefraEvent::Identify(identify_event)) => {
                self.handle_identify_event(identify_event).await;
            }

            SwarmEvent::Behaviour(DefraEvent::PushLog(pushlog_event)) => {
                self.handle_pushlog_event(pushlog_event).await;
            }

            SwarmEvent::Behaviour(DefraEvent::GossipSub(gossipsub_event)) => {
                self.handle_gossipsub_event(gossipsub_event).await;
            }

            SwarmEvent::Behaviour(DefraEvent::Bitswap(bitswap_event)) => {
                self.handle_bitswap_event(bitswap_event).await;
            }

            SwarmEvent::Behaviour(DefraEvent::Kademlia(kad_event)) => {
                self.handle_kademlia_event(kad_event).await;
            }

            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                warn!(
                    peer_id = ?peer_id,
                    error = %error,
                    "Outgoing connection failed"
                );
            }

            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                warn!(
                    local_addr = %local_addr,
                    remote_addr = %send_back_addr,
                    error = %error,
                    "Incoming connection failed"
                );
            }

            SwarmEvent::ListenerError { listener_id, error } => {
                error!(
                    listener_id = ?listener_id,
                    error = %error,
                    "Listener error"
                );
            }

            SwarmEvent::ListenerClosed {
                listener_id,
                reason,
                ..
            } => {
                warn!(
                    listener_id = ?listener_id,
                    reason = ?reason,
                    "Listener closed"
                );
            }

            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {
                debug!(
                    listener_id = ?listener_id,
                    address = %address,
                    "Listen address expired"
                );
            }

            SwarmEvent::Dialing {
                peer_id: Some(peer_id),
                ..
            } => {
                debug!(peer_id = %peer_id, "Dialing peer");
            }

            SwarmEvent::Dialing { peer_id: None, .. } => {
                // Dialing without a specific peer ID (rare, usually has peer_id)
            }

            _ => {
                // Other swarm events (e.g., Dialing, NewExternalAddrCandidate) are
                // handled by libp2p internally and don't require explicit handling
            }
        }
    }

    /// Handle identify protocol events.
    async fn handle_identify_event(&mut self, event: libp2p::identify::Event) {
        match event {
            libp2p::identify::Event::Received { peer_id, info, .. } => {
                debug!(
                    "Identified peer {}: {} with {} addresses, {} protocols",
                    peer_id,
                    info.agent_version,
                    info.listen_addrs.len(),
                    info.protocols.len()
                );

                // Inform Bitswap about the peer's supported protocols
                // This is critical for Bitswap to know this peer can serve blocks
                let protocols: Vec<String> =
                    info.protocols.iter().map(|p| p.to_string()).collect();
                debug!(
                    peer_id = %peer_id,
                    protocols = ?protocols,
                    "Informing Bitswap of peer protocols"
                );
                self.swarm.behaviour().on_identify(&peer_id, &protocols);

                for addr in &info.listen_addrs {
                    debug!(peer_id = %peer_id, address = %addr, "Adding external address from identify");
                }
                for addr in info.listen_addrs {
                    self.swarm.add_external_address(addr);
                }
            }
            libp2p::identify::Event::Sent { peer_id, .. } => {
                debug!("Sent identify info to {}", peer_id);
            }
            libp2p::identify::Event::Pushed { peer_id, .. } => {
                debug!("Pushed identify info to {}", peer_id);
            }
            libp2p::identify::Event::Error { peer_id, error, .. } => {
                warn!("Identify error with {}: {}", peer_id, error);
            }
        }
    }

    /// Handle PushLog request-response events.
    async fn handle_pushlog_event(
        &mut self,
        event: request_response::Event<PushLogRequest, PushLogReply>,
    ) {
        match event {
            request_response::Event::Message { peer, message } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    debug!("Received PushLog request from {}", peer);
                    if self
                        .event_tx
                        .send(HostEvent::PushLogRequest {
                            peer_id: peer,
                            request,
                            channel: ResponseChannel(channel),
                        })
                        .await
                        .is_err()
                    {
                        error!(peer_id = %peer, "Failed to send PushLogRequest event - receiver dropped, request will not be processed");
                    }
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    debug!("Received PushLog response for request {:?}", request_id);
                    if let Some(sender) = self.pending_requests.remove(&request_id) {
                        if sender.send(Ok(response)).is_err() {
                            debug!(request_id = ?request_id, "PushLog response dropped - caller cancelled");
                        }
                    }
                }
            },

            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                error!("Outbound request {:?} failed: {:?}", request_id, error);
                if let Some(sender) = self.pending_requests.remove(&request_id) {
                    if sender
                        .send(Err(Error::Transport(format!("{:?}", error))))
                        .is_err()
                    {
                        debug!(request_id = ?request_id, "PushLog error response dropped - caller cancelled");
                    }
                }
            }

            request_response::Event::InboundFailure { peer, error, .. } => {
                warn!("Inbound request from {} failed: {:?}", peer, error);
            }

            request_response::Event::ResponseSent { peer, .. } => {
                debug!("Response sent to {}", peer);
            }
        }
    }

    /// Handle GossipSub events.
    async fn handle_gossipsub_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                let topic = message.topic.to_string();
                debug!(
                    "Received gossipsub message {} on topic {} from {}",
                    message_id, topic, propagation_source
                );

                // Decode the message payload
                // Go sends PushLogRequest with MetaData, then we convert to PushLogBroadcast
                match serde_cbor::from_slice::<PushLogRequest>(&message.data) {
                    Ok(request) => {
                        // Convert to broadcast format (strips metadata)
                        let broadcast = PushLogBroadcast::from_request(&request);
                        if self
                            .event_tx
                            .send(HostEvent::GossipMessage {
                                propagation_source,
                                message_id: message_id.clone(),
                                topic: topic.clone(),
                                message: broadcast,
                            })
                            .await
                            .is_err()
                        {
                            error!(
                                peer_id = %propagation_source,
                                message_id = ?message_id,
                                topic = %topic,
                                "Failed to send GossipMessage event - receiver dropped, message will not be processed"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            peer_id = %propagation_source,
                            topic = %topic,
                            message_size = message.data.len(),
                            error = %e,
                            "Failed to decode gossipsub message"
                        );
                    }
                }
            }

            gossipsub::Event::Subscribed { peer_id, topic } => {
                debug!("Peer {} subscribed to {}", peer_id, topic);
                if self
                    .event_tx
                    .send(HostEvent::PeerSubscribed {
                        peer_id,
                        topic: topic.to_string(),
                    })
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, topic = %topic, "Failed to send PeerSubscribed event - receiver dropped");
                }
            }

            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                debug!("Peer {} unsubscribed from {}", peer_id, topic);
                if self
                    .event_tx
                    .send(HostEvent::PeerUnsubscribed {
                        peer_id,
                        topic: topic.to_string(),
                    })
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, topic = %topic, "Failed to send PeerUnsubscribed event - receiver dropped");
                }
            }

            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                debug!("Peer {} does not support gossipsub", peer_id);
            }
        }
    }

    /// Handle Bitswap events.
    async fn handle_bitswap_event(&mut self, event: BitswapEvent) {
        // iroh-bitswap events are for higher-level coordination
        // Block exchange happens transparently through the Client
        match event {
            BitswapEvent::Provide { key } => {
                debug!(cid = %key, "Bitswap requests to provide block");
                // Could integrate with Kademlia DHT to provide this key
            }
            BitswapEvent::FindProviders { key, response, limit } => {
                debug!(cid = %key, limit = limit, "Bitswap requests to find providers");
                // Could query Kademlia DHT to find providers
                // For now, send empty set (peer discovery via mDNS/manual dial)
                let _ = response.send(Ok(std::collections::HashSet::new())).await;
            }
            BitswapEvent::Ping { peer, response } => {
                debug!(peer_id = %peer, "Bitswap ping request");
                // Could implement ping latency measurement
                let _ = response.send(None);
            }
        }
    }

    /// Handle Kademlia DHT events.
    async fn handle_kademlia_event(&mut self, event: libp2p::kad::Event) {
        use libp2p::kad;

        match event {
            kad::Event::RoutingUpdated {
                peer, addresses, ..
            } => {
                debug!(
                    peer_id = %peer,
                    addresses = ?addresses,
                    "Kademlia routing table updated"
                );
            }

            kad::Event::OutboundQueryProgressed { id, result, .. } => {
                debug!(query_id = ?id, "Kademlia query progressed: {:?}", result);
            }

            kad::Event::InboundRequest { request } => {
                debug!("Kademlia inbound request: {:?}", request);
            }

            kad::Event::RoutablePeer { peer, address } => {
                debug!(peer_id = %peer, address = %address, "Found routable peer via Kademlia");
            }

            kad::Event::PendingRoutablePeer { peer, address } => {
                debug!(
                    peer_id = %peer,
                    address = %address,
                    "Found pending routable peer via Kademlia"
                );
            }

            kad::Event::UnroutablePeer { peer } => {
                debug!(peer_id = %peer, "Peer is unroutable via Kademlia");
            }

            kad::Event::ModeChanged { new_mode } => {
                debug!(mode = ?new_mode, "Kademlia mode changed");
            }
        }
    }

    /// Send a PushLog response through the given channel.
    pub fn send_pushlog_response(&mut self, channel: ResponseChannel, response: PushLogReply) {
        if let Err(resp) = self
            .swarm
            .behaviour_mut()
            .send_pushlog_response(channel.0, response)
        {
            error!("Failed to send PushLog response: message_id={}", resp.message_id);
        }
    }
}
