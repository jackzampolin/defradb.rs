//! P2P Host implementation for DefraDB.
//!
//! This module provides the main P2P host that manages the libp2p swarm,
//! handles peer connections, and coordinates CRDT synchronization.

mod protocols;
mod swarm;
mod two_stream;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use iroh_bitswap::Store;
use libp2p::{
    identity::Keypair, noise, request_response, swarm::behaviour::toggle::Toggle, tcp, yamux,
    Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::behaviour::DefraBehaviour;
use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::{Error, Result};
use crate::message::PushLogReply;
use crate::two_stream::{TwoStreamHandler, TwoStreamRunner};
use crate::QueryId;

use super::command::HostCommand;
use super::event::HostEvent;
use super::handle::P2PHostHandle;
use super::ResponseChannel;

/// Default idle connection timeout.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration options for P2P host construction.
#[derive(Debug, Clone)]
pub struct P2PHostConfig {
    pub enable_pubsub: bool,
    pub enable_relay: bool,
    /// Max protocol message size in bytes. Default: 16 MiB.
    pub max_msg_size: u64,
    /// Max CAR file size in bytes. Default: 64 MiB.
    pub max_car_size: u64,
    /// Stream read timeout in seconds. Default: 30.
    pub stream_timeout: u64,
    /// Max concurrent stream handler tasks. Default: 64.
    pub max_p2p_tasks: usize,
    /// Max established inbound connections. Default: 100.
    pub max_connections_in: u32,
    /// Max established outbound connections. Default: 400.
    pub max_connections_out: u32,
    /// Max connections per peer. Default: 4.
    pub max_connections_per_peer: u32,
    /// Bitswap egress access control. When `Controlled`, served blocks
    /// are filtered per-peer against the shared `ReplicatorRegistry`,
    /// matching Go's `WithPeerBlockRequestFilter` wiring. Default: `Open`.
    pub access_mode: AccessMode,
}

impl Default for P2PHostConfig {
    fn default() -> Self {
        Self {
            enable_pubsub: true,
            enable_relay: false,
            max_msg_size: 16 * 1024 * 1024,
            max_car_size: 64 * 1024 * 1024,
            stream_timeout: 30,
            max_p2p_tasks: 64,
            max_connections_in: 100,
            max_connections_out: 400,
            max_connections_per_peer: 4,
            access_mode: AccessMode::Open,
        }
    }
}

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
    pub(super) two_stream_event_rx: mpsc::Receiver<crate::two_stream::TwoStreamEvent>,
    /// Tracked spawned tasks for graceful shutdown
    pub(super) spawned_tasks: tokio::task::JoinSet<()>,
    /// Bitswap query abort handles for cancellation support
    pub(super) bitswap_queries: HashMap<QueryId, tokio::task::AbortHandle>,
    /// Per-peer addresses learned from connections and identify protocol.
    /// Used by ActivePeers to return full multiaddrs (Go-compatible).
    pub(super) peer_addrs: HashMap<PeerId, Multiaddr>,
    /// Optional local DEFRA identity used for Go-compatible identity exchange.
    pub(super) node_identity: Option<Arc<identity::RawIdentity>>,
    /// Verified peer DEFRA DIDs keyed by peer ID.
    pub(super) peer_identities: HashMap<PeerId, identity::Did>,
    /// Topics whose incoming gossipsub messages bypass the PushLog decoder
    /// and flow through `HostEvent::GossipRawMessage` instead. Populated
    /// by `HostCommand::RegisterPubsubRpcTopic` when the coordinator wires
    /// up a `pubsub_rpc::TopicHandler` (#828).
    pub(super) pubsub_rpc_topics: HashSet<String>,
}

impl<S: Store + Clone + Send + Sync + 'static> P2PHost<S> {
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

    /// Create a new P2P host with the given config options.
    pub async fn with_config(
        bitswap_store: S,
        config: P2PHostConfig,
    ) -> Result<(
        Self,
        P2PHostHandle,
        mpsc::Receiver<HostEvent>,
        Arc<ReplicatorRegistry>,
    )> {
        let keypair = Keypair::generate_ed25519();
        Self::with_keypair_and_config(keypair, bitswap_store, config).await
    }

    /// Create a new P2P host with the given keypair and blockstore.
    ///
    /// Uses default config: pubsub enabled, relay disabled.
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
        Self::with_keypair_and_config_and_identity(
            keypair,
            bitswap_store,
            P2PHostConfig::default(),
            None,
        )
        .await
    }

    /// Create a new P2P host with the given keypair, blockstore, and config options.
    ///
    /// # Arguments
    ///
    /// * `keypair` - The identity keypair for this node
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    /// * `config` - P2P host configuration (pubsub, relay toggles)
    pub async fn with_keypair_and_config(
        keypair: Keypair,
        bitswap_store: S,
        config: P2PHostConfig,
    ) -> Result<(
        Self,
        P2PHostHandle,
        mpsc::Receiver<HostEvent>,
        Arc<ReplicatorRegistry>,
    )> {
        Self::with_keypair_and_config_and_identity(keypair, bitswap_store, config, None).await
    }

    /// Create a new P2P host with the given keypair, blockstore, config, and optional DEFRA identity.
    pub async fn with_keypair_and_config_and_identity(
        keypair: Keypair,
        bitswap_store: S,
        config: P2PHostConfig,
        node_identity: Option<Arc<identity::RawIdentity>>,
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

        // Replicator registry is constructed up-front because the Bitswap
        // access filter (installed at behaviour construction time) needs
        // the same Arc that SyncCoordinator will later populate.
        let replicators = Arc::new(ReplicatorRegistry::new());

        // Pass keypair and blockstore to behaviour for message signing and block exchange
        let mut behaviour = DefraBehaviour::new(
            local_peer_id,
            local_public_key,
            keypair.clone(),
            bitswap_store,
            config.access_mode,
            Arc::clone(&replicators),
            config.enable_pubsub,
            &config,
        )
        .await?;

        // Enable TCP port reuse to match Go-libp2p behavior.
        // Go-libp2p reuses the listen port for outgoing connections, so the
        // remote side sees the listen address (not an ephemeral port).
        // This is critical for ActivePeers to return correct addresses.
        let tcp_config = tcp::Config::default().port_reuse(true);

        // Two builder paths required: libp2p's SwarmBuilder uses a typestate
        // pattern where `.with_relay_client()` transitions to a different builder
        // type (different `.with_behaviour()` closure signature), so it cannot be
        // called conditionally on a single builder chain.
        let swarm = if config.enable_relay {
            SwarmBuilder::with_existing_identity(keypair.clone())
                .with_tokio()
                .with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
                .map_err(|e| Error::Transport(e.to_string()))?
                .with_relay_client(noise::Config::new, yamux::Config::default)
                .map_err(|e| Error::Transport(e.to_string()))?
                .with_behaviour(|_key, relay_client| {
                    behaviour.relay = Toggle::from(Some(relay_client));
                    behaviour
                })
                .expect("behaviour already constructed")
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
                .build()
        } else {
            SwarmBuilder::with_existing_identity(keypair.clone())
                .with_tokio()
                .with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
                .map_err(|e| Error::Transport(e.to_string()))?
                .with_behaviour(|_key| behaviour)
                .expect("behaviour already constructed")
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
                .build()
        };

        let (command_tx, command_rx) = mpsc::channel(256);
        let (event_tx, event_rx) = mpsc::channel(256);

        let handle = P2PHostHandle::new(
            command_tx,
            local_public_key_proto,
            local_peer_id,
            keypair.clone(),
        );

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
        let car_request_streams = control
            .accept(TwoStreamHandler::car_request_protocol())
            .map_err(|_| Error::Behaviour("Failed to register CAR request protocol".into()))?;
        let car_response_streams = control
            .accept(TwoStreamHandler::car_response_protocol())
            .map_err(|_| Error::Behaviour("Failed to register CAR response protocol".into()))?;
        let identity_request_streams = control
            .accept(TwoStreamHandler::identity_request_protocol())
            .map_err(|_| Error::Behaviour("Failed to register identity request protocol".into()))?;
        let identity_response_streams = control
            .accept(TwoStreamHandler::identity_response_protocol())
            .map_err(|_| {
                Error::Behaviour("Failed to register identity response protocol".into())
            })?;

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
            car_request_streams,
            car_response_streams,
            identity_request_streams,
            identity_response_streams,
            two_stream_event_tx,
            config.max_msg_size,
            config.max_car_size,
            config.stream_timeout,
            config.max_p2p_tasks,
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
            node_identity,
            peer_identities: HashMap::new(),
            pubsub_rpc_topics: HashSet::new(),
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

    /// Send a PushLog response through the given channel.
    pub fn send_pushlog_response(&mut self, channel: ResponseChannel, response: PushLogReply) {
        if let Err(resp) = self
            .swarm
            .behaviour_mut()
            .send_pushlog_response(channel.into_inner(), response)
        {
            tracing::error!(
                "Failed to send PushLog response: message_id={}",
                resp.message_id
            );
        }
    }
}
