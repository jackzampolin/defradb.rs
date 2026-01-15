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
use std::time::Duration;

use cid::Cid;
use futures::StreamExt;
use libipld::DefaultParams;
use libp2p::{
    gossipsub, identity::Keypair, mdns, noise, request_response, swarm::SwarmEvent, tcp, yamux,
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use libp2p_bitswap_next::{BitswapEvent, BitswapStore, QueryId};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::behaviour::{DefraBehaviour, DefraEvent};
use crate::error::{Error, Result};
use crate::message::{PushLogBroadcast, PushLogReply, PushLogRequest};
use crate::topics::DefraTopic;

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
}

/// Opaque response channel for sending PushLog responses.
#[derive(Debug)]
pub struct ResponseChannel(request_response::ResponseChannel<PushLogReply>);

/// Handle to interact with the P2P host.
#[derive(Clone)]
pub struct P2PHostHandle {
    command_tx: mpsc::Sender<HostCommand>,
}

impl P2PHostHandle {
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
}

/// P2P Host that manages the libp2p swarm.
pub struct P2PHost {
    swarm: Swarm<DefraBehaviour>,
    keypair: Keypair,
    command_rx: mpsc::Receiver<HostCommand>,
    event_tx: mpsc::Sender<HostEvent>,
    pending_requests:
        HashMap<request_response::OutboundRequestId, oneshot::Sender<Result<PushLogReply>>>,
}

impl P2PHost {
    /// Create a new P2P host with a generated identity and the given blockstore.
    ///
    /// # Arguments
    ///
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    pub fn new<S: BitswapStore<Params = DefaultParams>>(
        bitswap_store: S,
    ) -> Result<(Self, P2PHostHandle, mpsc::Receiver<HostEvent>)> {
        let keypair = Keypair::generate_ed25519();
        Self::with_keypair(keypair, bitswap_store)
    }

    /// Create a new P2P host with the given keypair and blockstore.
    ///
    /// # Arguments
    ///
    /// * `keypair` - The identity keypair for this node
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    ///
    /// # Note
    ///
    /// This must be called within a tokio runtime context as Bitswap spawns
    /// a background task for database operations.
    pub fn with_keypair<S: BitswapStore<Params = DefaultParams>>(
        keypair: Keypair,
        bitswap_store: S,
    ) -> Result<(Self, P2PHostHandle, mpsc::Receiver<HostEvent>)> {
        let local_peer_id = keypair.public().to_peer_id();
        let local_public_key = keypair.public();

        info!("Local peer ID: {}", local_peer_id);

        // Pass keypair and blockstore to behaviour for message signing and block exchange
        let behaviour =
            DefraBehaviour::new(local_peer_id, local_public_key, keypair.clone(), bitswap_store)
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

        let handle = P2PHostHandle { command_tx };

        let host = Self {
            swarm,
            keypair,
            command_rx,
            event_tx,
            pending_requests: HashMap::new(),
        };

        Ok((host, handle, event_rx))
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
            }
        }

        info!("P2P host shutdown complete");
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
                    .map_err(|resp| Error::ResponseSend(format!("{:?}", resp.metadata)));
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
                info!("Shutdown requested");
                return false;
            }

            HostCommand::BitswapSync {
                cid,
                providers,
                missing,
                response,
            } => {
                debug!(
                    cid = %cid,
                    providers = ?providers,
                    missing_count = missing.len(),
                    "Starting Bitswap sync"
                );
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .bitswap_sync(cid, providers, missing.into_iter());
                if response.send(Ok(query_id)).is_err() {
                    debug!(cid = %cid, "BitswapSync command response dropped - caller cancelled");
                }
            }

            HostCommand::BitswapCancel { query_id, response } => {
                debug!(query_id = ?query_id, "Cancelling Bitswap query");
                let cancelled = self.swarm.behaviour_mut().bitswap_cancel(query_id);
                if response.send(cancelled).is_err() {
                    debug!(query_id = ?query_id, "BitswapCancel command response dropped - caller cancelled");
                }
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
                if self.event_tx.send(HostEvent::Listening(address.clone())).await.is_err() {
                    warn!(address = %address, "Failed to send Listening event - receiver dropped");
                }
            }

            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to peer: {}", peer_id);
                if self.event_tx.send(HostEvent::PeerConnected(peer_id)).await.is_err() {
                    warn!(peer_id = %peer_id, "Failed to send PeerConnected event - receiver dropped");
                }
            }

            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer: {}", peer_id);
                if self.event_tx.send(HostEvent::PeerDisconnected(peer_id)).await.is_err() {
                    warn!(peer_id = %peer_id, "Failed to send PeerDisconnected event - receiver dropped");
                }
            }

            SwarmEvent::Behaviour(DefraEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer_id, addr) in peers {
                    debug!("mDNS discovered peer: {} at {}", peer_id, addr);
                    debug!(peer_id = %peer_id, address = %addr, "Adding external address from mDNS discovery");
                    self.swarm.add_external_address(addr);
                    if self.event_tx.send(HostEvent::PeerDiscovered(peer_id)).await.is_err() {
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

            SwarmEvent::OutgoingConnectionError {
                peer_id, error, ..
            } => {
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

            SwarmEvent::ExpiredListenAddr { listener_id, address } => {
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
                    "Identified peer {}: {} with {} addresses",
                    peer_id,
                    info.agent_version,
                    info.listen_addrs.len()
                );
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
                    if sender.send(Err(Error::Transport(format!("{:?}", error)))).is_err() {
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
                match serde_cbor::from_slice::<PushLogBroadcast>(&message.data) {
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
        match event {
            BitswapEvent::Progress(query_id, missing_count) => {
                debug!(
                    query_id = ?query_id,
                    missing_count = missing_count,
                    "Bitswap sync progress"
                );
                if self
                    .event_tx
                    .send(HostEvent::BitswapProgress {
                        query_id,
                        missing_count,
                    })
                    .await
                    .is_err()
                {
                    warn!(
                        query_id = ?query_id,
                        "Failed to send BitswapProgress event - receiver dropped"
                    );
                }
            }

            BitswapEvent::Complete(query_id, result) => {
                let (success, error) = match result {
                    Ok(()) => {
                        debug!(query_id = ?query_id, "Bitswap sync completed successfully");
                        (true, None)
                    }
                    Err(e) => {
                        warn!(query_id = ?query_id, error = %e, "Bitswap sync failed");
                        (false, Some(e.to_string()))
                    }
                };

                if self
                    .event_tx
                    .send(HostEvent::BitswapComplete {
                        query_id,
                        success,
                        error,
                    })
                    .await
                    .is_err()
                {
                    warn!(
                        query_id = ?query_id,
                        "Failed to send BitswapComplete event - receiver dropped"
                    );
                }
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
            error!("Failed to send PushLog response: {:?}", resp.metadata);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MockBitswapStore;

    #[tokio::test]
    async fn test_host_creation() {
        let store = MockBitswapStore::new();
        let result = P2PHost::new(store);
        assert!(result.is_ok());

        let (host, handle, _events) = result.unwrap();
        let peer_id = host.local_peer_id();
        assert_ne!(peer_id.to_string(), "");

        // Shutdown
        handle.shutdown().await.unwrap();
    }
}
