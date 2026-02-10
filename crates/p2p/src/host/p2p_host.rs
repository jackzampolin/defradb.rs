//! P2P Host implementation for DefraDB.
//!
//! This module provides the main P2P host that manages the libp2p swarm,
//! handles peer connections, and coordinates CRDT synchronization.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use iroh_bitswap::{BitswapEvent, Store};
use libp2p::{
    gossipsub, identity::Keypair, noise, request_response, swarm::SwarmEvent, tcp, yamux,
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::behaviour::{DefraBehaviour, DefraEvent};
use crate::bitswap::ReplicatorRegistry;
use crate::error::{Error, Result};
use crate::message::{PushLogBroadcast, PushLogReply, PushLogRequest};
use crate::two_stream::{TwoStreamEvent, TwoStreamHandler, TwoStreamRunner};
use crate::QueryId;

use super::command::HostCommand;
use super::event::HostEvent;
use super::handle::P2PHostHandle;
use super::ResponseChannel;

/// Default idle connection timeout.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// P2P Host that manages the libp2p swarm.
pub struct P2PHost<S: Store> {
    pub(super) swarm: Swarm<DefraBehaviour<S>>,
    pub(super) keypair: Keypair,
    pub(super) command_rx: mpsc::Receiver<HostCommand>,
    pub(super) event_tx: mpsc::Sender<HostEvent>,
    pub(super) pending_requests: HashMap<
        request_response::OutboundRequestId,
        tokio::sync::oneshot::Sender<Result<PushLogReply>>,
    >,
    /// Replicator registry for access control
    pub(super) replicators: Arc<ReplicatorRegistry>,
    /// Two-stream handler for Go compatibility
    pub(super) two_stream_handler: Arc<tokio::sync::Mutex<TwoStreamHandler>>,
    /// Receiver for two-stream events
    pub(super) two_stream_event_rx: mpsc::Receiver<TwoStreamEvent>,
    /// Tracked spawned tasks for graceful shutdown
    pub(super) spawned_tasks: tokio::task::JoinSet<()>,
    /// Bitswap query abort handles for cancellation support
    pub(super) bitswap_queries: HashMap<QueryId, tokio::task::AbortHandle>,
    /// Per-peer addresses learned from connections and identify protocol.
    /// Used by ActivePeers to return full multiaddrs (Go-compatible).
    pub(super) peer_addrs: HashMap<PeerId, Multiaddr>,
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

    /// Create a new P2P host with pubsub optionally disabled.
    pub async fn with_pubsub(
        bitswap_store: S,
        enable_pubsub: bool,
    ) -> Result<(
        Self,
        P2PHostHandle,
        mpsc::Receiver<HostEvent>,
        Arc<ReplicatorRegistry>,
    )> {
        let keypair = Keypair::generate_ed25519();
        Self::with_keypair_and_config(keypair, bitswap_store, enable_pubsub).await
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
        Self::with_keypair_and_config(keypair, bitswap_store, true).await
    }

    /// Create a new P2P host with the given keypair, blockstore, and pubsub config.
    pub async fn with_keypair_and_config(
        keypair: Keypair,
        bitswap_store: S,
        enable_pubsub: bool,
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
        let behaviour = DefraBehaviour::new(
            local_peer_id,
            local_public_key,
            keypair.clone(),
            bitswap_store,
            enable_pubsub,
        )
        .await
        .map_err(|e| Error::Behaviour(e.to_string()))?;

        // Enable TCP port reuse to match Go-libp2p behavior.
        // Go-libp2p reuses the listen port for outgoing connections, so the
        // remote side sees the listen address (not an ephemeral port).
        // This is critical for ActivePeers to return correct addresses.
        let tcp_config = tcp::Config::default().port_reuse(true);
        let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
            .map_err(|e| Error::Transport(e.to_string()))?
            .with_behaviour(|_key| behaviour)
            .expect("behaviour already constructed")
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
            .build();

        let (command_tx, command_rx) = mpsc::channel(256);
        let (event_tx, event_rx) = mpsc::channel(256);

        let handle = P2PHostHandle::new(
            command_tx,
            local_public_key_proto,
            local_peer_id,
            keypair.clone(),
        );

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
        let se_request_streams = control
            .accept(TwoStreamHandler::se_request_protocol())
            .map_err(|_| Error::Behaviour("Failed to register SE request protocol".into()))?;
        let se_response_streams = control
            .accept(TwoStreamHandler::se_response_protocol())
            .map_err(|_| Error::Behaviour("Failed to register SE response protocol".into()))?;

        let handler = TwoStreamHandler::new(control);
        let pending = handler.pending_responses();
        let two_stream_handler = Arc::new(tokio::sync::Mutex::new(handler));
        let (two_stream_event_tx, two_stream_event_rx) = mpsc::channel(256);

        // Spawn the two-stream runner as a background task
        let runner = TwoStreamRunner::new(
            pending,
            request_streams,
            response_streams,
            se_request_streams,
            se_response_streams,
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
            peer_addrs: HashMap::new(),
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
            // Biased select: process swarm events before commands.
            // This ensures ConnectionEstablished events (which update peer_addrs)
            // are processed before PeerAddresses commands read them.
            tokio::select! {
                biased;

                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
                // Handle two-stream protocol events (Go compatibility)
                Some(two_stream_event) = self.two_stream_event_rx.recv() => {
                    self.handle_two_stream_event(two_stream_event).await;
                }
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

    /// Dial a peer at the given addresses.
    pub(super) fn dial_peer(&mut self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
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

            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                info!("Connected to peer: {}", peer_id);
                // Store the remote peer's address from the connection endpoint.
                // For dialer: the address we dialed (peer's listen addr).
                // For listener: the send_back_addr. With TCP port reuse enabled,
                // this IS the peer's listen address (Go-compatible behavior).
                let peer_addr = match &endpoint {
                    libp2p::core::ConnectedPoint::Dialer { address, .. } => address.clone(),
                    libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => {
                        send_back_addr.clone()
                    }
                };
                self.peer_addrs.insert(peer_id, peer_addr.clone());

                // Add peer to Kademlia BEFORE bootstrap. Kademlia's own
                // ConnectionEstablished handler doesn't add peers to the
                // routing table until protocol negotiation completes (async).
                // We add the address now so bootstrap() has a peer to query.
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, peer_addr);

                // Trigger Kademlia bootstrap to discover peers through the DHT.
                // When node2 connects to node0 (who already knows node1),
                // this causes node2 to query node0, discover node1, and dial it.
                let _ = self.swarm.behaviour_mut().kademlia.bootstrap();

                if self
                    .event_tx
                    .send(HostEvent::PeerConnected(peer_id))
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, "Failed to send PeerConnected event - receiver dropped");
                }
            }

            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                info!("Disconnected from peer: {}", peer_id);
                if num_established == 0 {
                    self.peer_addrs.remove(&peer_id);
                }
                if self
                    .event_tx
                    .send(HostEvent::PeerDisconnected(peer_id))
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, "Failed to send PeerDisconnected event - receiver dropped");
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
                // Update stored address with the peer's first listen address.
                // This corrects the ephemeral send_back_addr for incoming connections.
                if let Some(listen_addr) = info.listen_addrs.first() {
                    self.peer_addrs.insert(peer_id, listen_addr.clone());
                }

                debug!(
                    "Identified peer {}: {} with {} addresses, {} protocols",
                    peer_id,
                    info.agent_version,
                    info.listen_addrs.len(),
                    info.protocols.len()
                );

                // Inform Bitswap about the peer's supported protocols
                // This is critical for Bitswap to know this peer can serve blocks
                let protocols: Vec<String> = info.protocols.iter().map(|p| p.to_string()).collect();
                debug!(
                    peer_id = %peer_id,
                    protocols = ?protocols,
                    "Informing Bitswap of peer protocols"
                );
                self.swarm.behaviour().on_identify(&peer_id, &protocols);

                // Store the peer's listen addresses in Kademlia for routing.
                // Do NOT call add_external_address — those are the REMOTE peer's
                // addresses, not ours. Adding them as local external addresses
                // causes address cross-contamination between peers.
                for addr in &info.listen_addrs {
                    debug!(peer_id = %peer_id, address = %addr, "Adding peer address to Kademlia");
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr.clone());
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
                            channel: ResponseChannel::new(channel),
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

                // Decode the message payload.
                // Rust-to-Rust sends PushLogBroadcast (no MetaData).
                // Go-to-Rust sends PushLogRequest (with MetaData).
                // Try PushLogBroadcast first, then fall back to PushLogRequest.
                let broadcast =
                    serde_cbor::from_slice::<PushLogBroadcast>(&message.data).or_else(|_| {
                        serde_cbor::from_slice::<PushLogRequest>(&message.data)
                            .map(|req| PushLogBroadcast::from_request(&req))
                    });

                match broadcast {
                    Ok(broadcast) => {
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
                            "Failed to decode gossipsub message as PushLogBroadcast or PushLogRequest"
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
            BitswapEvent::FindProviders {
                key,
                response,
                limit,
            } => {
                debug!(cid = %key, limit = limit, "Bitswap requests to find providers");
                // Could query Kademlia DHT to find providers
                // For now, send empty set (peer discovery via manual dial)
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
            .send_pushlog_response(channel.into_inner(), response)
        {
            error!(
                "Failed to send PushLog response: message_id={}",
                resp.message_id
            );
        }
    }
}
