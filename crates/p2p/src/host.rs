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

use futures::StreamExt;
use libp2p::{
    gossipsub, identity::Keypair, mdns, noise, request_response, swarm::SwarmEvent, tcp, yamux,
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
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
    /// Create a new P2P host with a generated identity.
    pub fn new() -> Result<(Self, P2PHostHandle, mpsc::Receiver<HostEvent>)> {
        let keypair = Keypair::generate_ed25519();
        Self::with_keypair(keypair)
    }

    /// Create a new P2P host with the given keypair.
    pub fn with_keypair(
        keypair: Keypair,
    ) -> Result<(Self, P2PHostHandle, mpsc::Receiver<HostEvent>)> {
        let local_peer_id = keypair.public().to_peer_id();
        let local_public_key = keypair.public();

        info!("Local peer ID: {}", local_peer_id);

        // Pass keypair to behaviour for message signing/verification
        let behaviour = DefraBehaviour::new(local_peer_id, local_public_key, keypair.clone())
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
                    .listen_on(addr)
                    .map(|_| ())
                    .map_err(|e| Error::Transport(e.to_string()));
                let _ = response.send(result);
            }

            HostCommand::Dial {
                peer_id,
                addrs,
                response,
            } => {
                let result = self.dial_peer(peer_id, addrs);
                let _ = response.send(result);
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

            HostCommand::LocalPeerId { response } => {
                let _ = response.send(*self.swarm.local_peer_id());
            }

            HostCommand::ListenAddresses { response } => {
                let addrs: Vec<_> = self.swarm.listeners().cloned().collect();
                let _ = response.send(addrs);
            }

            HostCommand::ConnectedPeers { response } => {
                let peers: Vec<_> = self.swarm.connected_peers().cloned().collect();
                let _ = response.send(peers);
            }

            HostCommand::Subscribe { topic, response } => {
                let ident_topic = topic.to_ident_topic();
                let result = self
                    .swarm
                    .behaviour_mut()
                    .subscribe(&ident_topic)
                    .map_err(|e| Error::GossipSubSubscription(e.to_string()));
                let _ = response.send(result);
            }

            HostCommand::Unsubscribe { topic, response } => {
                let ident_topic = topic.to_ident_topic();
                let result = self
                    .swarm
                    .behaviour_mut()
                    .unsubscribe(&ident_topic)
                    .map_err(|e| Error::GossipSubUnsubscribe(e.to_string()));
                let _ = response.send(result);
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
                let _ = response.send(result);
            }

            HostCommand::SubscribedTopics { response } => {
                let topics: Vec<String> = self
                    .swarm
                    .behaviour()
                    .subscribed_topics()
                    .map(|t| t.to_string())
                    .collect();
                let _ = response.send(topics);
            }

            HostCommand::Shutdown => {
                info!("Shutdown requested");
                return false;
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
                let _ = self.event_tx.send(HostEvent::Listening(address)).await;
            }

            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connected to peer: {}", peer_id);
                let _ = self.event_tx.send(HostEvent::PeerConnected(peer_id)).await;
            }

            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Disconnected from peer: {}", peer_id);
                let _ = self
                    .event_tx
                    .send(HostEvent::PeerDisconnected(peer_id))
                    .await;
            }

            SwarmEvent::Behaviour(DefraEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer_id, addr) in peers {
                    debug!("mDNS discovered peer: {} at {}", peer_id, addr);
                    self.swarm.add_external_address(addr);
                    let _ = self.event_tx.send(HostEvent::PeerDiscovered(peer_id)).await;
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

            _ => {}
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
                    let _ = self
                        .event_tx
                        .send(HostEvent::PushLogRequest {
                            peer_id: peer,
                            request,
                            channel: ResponseChannel(channel),
                        })
                        .await;
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    debug!("Received PushLog response for request {:?}", request_id);
                    if let Some(sender) = self.pending_requests.remove(&request_id) {
                        let _ = sender.send(Ok(response));
                    }
                }
            },

            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                error!("Outbound request {:?} failed: {:?}", request_id, error);
                if let Some(sender) = self.pending_requests.remove(&request_id) {
                    let _ = sender.send(Err(Error::Transport(format!("{:?}", error))));
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
                        let _ = self
                            .event_tx
                            .send(HostEvent::GossipMessage {
                                propagation_source,
                                message_id,
                                topic,
                                message: broadcast,
                            })
                            .await;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to decode gossipsub message from {}: {}",
                            propagation_source, e
                        );
                    }
                }
            }

            gossipsub::Event::Subscribed { peer_id, topic } => {
                debug!("Peer {} subscribed to {}", peer_id, topic);
                let _ = self
                    .event_tx
                    .send(HostEvent::PeerSubscribed {
                        peer_id,
                        topic: topic.to_string(),
                    })
                    .await;
            }

            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                debug!("Peer {} unsubscribed from {}", peer_id, topic);
                let _ = self
                    .event_tx
                    .send(HostEvent::PeerUnsubscribed {
                        peer_id,
                        topic: topic.to_string(),
                    })
                    .await;
            }

            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                debug!("Peer {} does not support gossipsub", peer_id);
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

    #[tokio::test]
    async fn test_host_creation() {
        let result = P2PHost::new();
        assert!(result.is_ok());

        let (host, handle, _events) = result.unwrap();
        let peer_id = host.local_peer_id();
        assert_ne!(peer_id.to_string(), "");

        // Shutdown
        handle.shutdown().await.unwrap();
    }
}
