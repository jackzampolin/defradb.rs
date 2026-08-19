//! P2P Host implementation for DefraDB.
//!
//! This module provides the main P2P host that manages the libp2p swarm,
//! handles peer connections, and coordinates CRDT synchronization.

mod connection_manager;
mod protocols;
mod swarm;
mod two_stream;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use iroh_bitswap::Store;
use libp2p::{
    identity::Keypair,
    multiaddr::Protocol,
    noise, request_response,
    swarm::{
        behaviour::toggle::Toggle,
        dial_opts::{DialOpts, PeerCondition},
        DialError,
    },
    tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tokio::{
    sync::mpsc,
    time::{self, Instant, MissedTickBehavior},
};
use tracing::{debug, info, warn};

use crate::behaviour::DefraBehaviour;
use crate::bitswap::{
    AccessMode, BlockClassifier, DefaultBlockClassifier, LateBoundServeAcp, ReplicatorRegistry,
};
use crate::error::{Error, Result};
use crate::message::PushLogReply;
use crate::two_stream::{TwoStreamHandler, TwoStreamRunner};
use crate::QueryId;

use connection_manager::ActiveConnectionManager;

use super::command::HostCommand;
use super::event::HostEvent;
use super::handle::P2PHostHandle;
use super::ResponseChannel;

/// Default idle connection timeout.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Periodic DHT refresh interval.
const DHT_BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Delay used to coalesce the initial connection burst before bootstrapping.
const DHT_BOOTSTRAP_DEBOUNCE: Duration = Duration::from_millis(100);

/// Go libp2p connection manager grace period before pruning new connections.
const DEFAULT_CONNECTION_MANAGER_GRACE_PERIOD: Duration = Duration::from_secs(20);

/// Frequency for checking whether the active connection set should be pruned.
const CONNECTION_PRUNE_INTERVAL: Duration = Duration::from_secs(5);

/// Go ResourceManager minimum FD cap. See go-libp2p resource-manager defaults.
const GO_RESOURCE_MANAGER_MIN_FD_LIMIT: u64 = 256;

/// Go ResourceManager minimum memory budget.
const GO_RESOURCE_MANAGER_MIN_MEMORY_BYTES: u64 = 128 * 1024 * 1024;

/// Go DefraDB gives libp2p half of system RAM by default.
const GO_RESOURCE_MANAGER_SYSTEM_MEMORY_DIVISOR: u64 = 2;

/// Go's transient ResourceManager scope is capped at 25% of the system scope.
const GO_RESOURCE_MANAGER_TRANSIENT_MEMORY_DIVISOR: u64 = 4;

#[derive(Debug, Clone, Copy)]
struct ResourceManagerRuntimeLimits {
    fd_limit: u64,
    system_memory_budget_bytes: u64,
}

impl ResourceManagerRuntimeLimits {
    fn detect() -> Result<Self> {
        let fd_limit = current_fd_limit()?;
        let system_memory_budget_bytes =
            total_system_memory_bytes() / GO_RESOURCE_MANAGER_SYSTEM_MEMORY_DIVISOR;
        validate_resource_manager_startup_limits(fd_limit, system_memory_budget_bytes)?;

        Ok(Self {
            fd_limit,
            system_memory_budget_bytes,
        })
    }

    fn transient_memory_budget_bytes(&self) -> u64 {
        self.system_memory_budget_bytes / GO_RESOURCE_MANAGER_TRANSIENT_MEMORY_DIVISOR
    }

    fn system_memory_budget_usize(&self) -> usize {
        self.system_memory_budget_bytes
            .try_into()
            .unwrap_or(usize::MAX)
    }
}

fn total_system_memory_bytes() -> u64 {
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()),
    );
    system.total_memory()
}

#[cfg(unix)]
fn current_fd_limit() -> Result<u64> {
    let (soft_limit, _) = rlimit::Resource::NOFILE.get().map_err(|error| {
        Error::InvalidConfig(format!(
            "failed to read NOFILE resource limit for libp2p ResourceManager validation: {error}"
        ))
    })?;
    Ok(soft_limit)
}

#[cfg(windows)]
fn current_fd_limit() -> Result<u64> {
    Ok(u64::from(rlimit::getmaxstdio()))
}

#[cfg(not(any(unix, windows)))]
fn current_fd_limit() -> Result<u64> {
    Ok(GO_RESOURCE_MANAGER_MIN_FD_LIMIT)
}

fn validate_resource_manager_startup_limits(fd_limit: u64, memory_budget_bytes: u64) -> Result<()> {
    if fd_limit < GO_RESOURCE_MANAGER_MIN_FD_LIMIT {
        return Err(Error::InvalidConfig(format!(
            "libp2p ResourceManager requires at least {} file descriptors; current soft limit is {fd_limit}; raise the soft limit with `ulimit -n 1024` or an equivalent service/container limit",
            GO_RESOURCE_MANAGER_MIN_FD_LIMIT
        )));
    }

    if memory_budget_bytes < GO_RESOURCE_MANAGER_MIN_MEMORY_BYTES {
        return Err(Error::InvalidConfig(format!(
            "libp2p ResourceManager requires at least {} MiB memory budget; computed budget is {} MiB",
            GO_RESOURCE_MANAGER_MIN_MEMORY_BYTES / 1024 / 1024,
            memory_budget_bytes / 1024 / 1024
        )));
    }

    Ok(())
}

/// Tracks the one-time initial bootstrap that complements periodic refresh.
///
/// Go wires periodic DHT refresh through `bootstrap.Bootstrap(...)`
/// (`go-p2p/peer.go:149`). Rust keeps that periodic refresh in the host loop
/// and adds a short kickstart so initial peers can exchange routing information
/// without waiting for the longer interval.
#[derive(Debug)]
struct BootstrapScheduler {
    pending: bool,
    has_fired: bool,
    debounce: Duration,
}

impl BootstrapScheduler {
    fn new(debounce: Duration) -> Self {
        Self {
            pending: false,
            has_fired: false,
            debounce,
        }
    }

    fn is_pending(&self) -> bool {
        self.pending
    }

    fn schedule_initial(&mut self, now: Instant) -> Option<Instant> {
        if self.pending || self.has_fired {
            return None;
        }

        self.pending = true;
        Some(now + self.debounce)
    }

    fn mark_fired(&mut self) {
        self.pending = false;
        self.has_fired = true;
    }
}

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
    /// Established connection low watermark. Default: 100.
    pub connection_manager_low_water: u32,
    /// Established connection high watermark. Default: 400.
    pub connection_manager_high_water: u32,
    /// Grace period before new established connections are eligible for pruning. Default: 20s.
    pub connection_manager_grace_period: Duration,
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
            enable_relay: true,
            max_msg_size: 16 * 1024 * 1024,
            max_car_size: 64 * 1024 * 1024,
            stream_timeout: 30,
            max_p2p_tasks: 64,
            connection_manager_low_water: 100,
            connection_manager_high_water: 400,
            connection_manager_grace_period: DEFAULT_CONNECTION_MANAGER_GRACE_PERIOD,
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
    shutdown_requested: bool,
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
    /// Tracks established connections and actively prunes them after the
    /// Go-compatible low/high watermarks are exceeded.
    connection_manager: ActiveConnectionManager,
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
    /// Uses default config: pubsub enabled, relay enabled.
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
        Self::with_keypair_and_config_and_identity_and_serve_gate(
            keypair,
            bitswap_store,
            config,
            node_identity,
            Arc::new(DefaultBlockClassifier),
            Arc::new(LateBoundServeAcp::new()),
        )
        .await
    }

    /// Create a new P2P host with explicit serve-gate metadata and ACP wiring.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_keypair_and_config_and_identity_and_serve_gate(
        keypair: Keypair,
        bitswap_store: S,
        config: P2PHostConfig,
        node_identity: Option<Arc<identity::RawIdentity>>,
        classifier: Arc<dyn BlockClassifier>,
        serve_acp: Arc<LateBoundServeAcp>,
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

        let resource_limits = ResourceManagerRuntimeLimits::detect()?;
        info!(
            fd_limit = resource_limits.fd_limit,
            system_memory_budget_mib = resource_limits.system_memory_budget_bytes / 1024 / 1024,
            transient_memory_budget_mib =
                resource_limits.transient_memory_budget_bytes() / 1024 / 1024,
            "Configured libp2p ResourceManager parity limits"
        );

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
            classifier,
            serve_acp,
            config.enable_pubsub,
            &config,
            resource_limits.system_memory_budget_usize(),
        )
        .await?;

        let tcp_config = tcp::Config::default();

        // Two builder paths required: libp2p's SwarmBuilder uses a typestate
        // pattern where `.with_relay_client()` transitions to a different builder
        // type (different `.with_behaviour()` closure signature), so it cannot be
        // called conditionally on a single builder chain.
        let swarm = if config.enable_relay {
            SwarmBuilder::with_existing_identity(keypair.clone())
                .with_tokio()
                .with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
                .map_err(|e| Error::Transport(e.to_string()))?
                .with_quic()
                .with_dns()
                .map_err(|e| Error::Transport(e.to_string()))?
                .with_websocket(noise::Config::new, yamux::Config::default)
                .await
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
                .with_quic()
                .with_dns()
                .map_err(|e| Error::Transport(e.to_string()))?
                .with_websocket(noise::Config::new, yamux::Config::default)
                .await
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
        let se_query_request_streams = control
            .accept(TwoStreamHandler::se_query_request_protocol())
            .map_err(|_| Error::Behaviour("Failed to register SE query request protocol".into()))?;
        let se_query_response_streams = control
            .accept(TwoStreamHandler::se_query_response_protocol())
            .map_err(|_| {
                Error::Behaviour("Failed to register SE query response protocol".into())
            })?;
        let manage_request_streams = control
            .accept(TwoStreamHandler::manage_request_protocol())
            .map_err(|_| Error::Behaviour("Failed to register manage request protocol".into()))?;
        let manage_response_streams = control
            .accept(TwoStreamHandler::manage_response_protocol())
            .map_err(|_| Error::Behaviour("Failed to register manage response protocol".into()))?;
        let manage_query_request_streams = control
            .accept(TwoStreamHandler::manage_query_request_protocol())
            .map_err(|_| {
                Error::Behaviour("Failed to register manage query request protocol".into())
            })?;
        let manage_query_response_streams = control
            .accept(TwoStreamHandler::manage_query_response_protocol())
            .map_err(|_| {
                Error::Behaviour("Failed to register manage query response protocol".into())
            })?;
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
            se_query_request_streams,
            se_query_response_streams,
            manage_request_streams,
            manage_response_streams,
            manage_query_request_streams,
            manage_query_response_streams,
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
            shutdown_requested: false,
            pending_requests: HashMap::new(),
            replicators: Arc::clone(&replicators),
            two_stream_handler,
            two_stream_event_rx,
            spawned_tasks: tokio::task::JoinSet::new(),
            bitswap_queries: HashMap::new(),
            peer_addrs: HashMap::new(),
            node_identity,
            peer_identities: HashMap::new(),
            connection_manager: ActiveConnectionManager::new(
                config.connection_manager_low_water,
                config.connection_manager_high_water,
                config.connection_manager_grace_period,
            ),
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

    /// Forward an event without preventing the host loop from servicing the
    /// response command needed by an already-admitted handler.
    ///
    /// Both channels are bounded. If the event channel is full, waiting only
    /// on its capacity can deadlock with a coordinator handler waiting for a
    /// command response from this same host. While backpressured, service host
    /// commands until one event slot becomes available.
    pub(super) async fn forward_event(&mut self, event: HostEvent) -> bool {
        let mut event = Some(event);
        loop {
            let event_tx = self.event_tx.clone();
            tokio::select! {
                permit = event_tx.reserve_owned() => {
                    let Ok(permit) = permit else {
                        return false;
                    };
                    permit.send(event.take().expect("event is forwarded exactly once"));
                    return true;
                }
                command = self.command_rx.recv() => {
                    match command {
                        Some(command) => {
                            if !self.handle_command(command).await {
                                self.shutdown_requested = true;
                                return false;
                            }
                        }
                        None => {
                            self.shutdown_requested = true;
                            return false;
                        }
                    }
                }
                result = self.spawned_tasks.join_next(), if !self.spawned_tasks.is_empty() => {
                    Self::report_spawned_task(result);
                }
            }
        }
    }

    fn report_spawned_task(result: Option<std::result::Result<(), tokio::task::JoinError>>) {
        if let Some(Err(error)) = result {
            tracing::warn!(%error, "P2P host task failed");
        }
    }

    /// Run the P2P host event loop.
    ///
    /// This method runs until shutdown is requested.
    pub async fn run(mut self) {
        let mut bootstrap_scheduler = BootstrapScheduler::new(DHT_BOOTSTRAP_DEBOUNCE);
        let mut bootstrap_deadline = Box::pin(time::sleep(DHT_BOOTSTRAP_DEBOUNCE));
        let mut bootstrap_interval = time::interval(DHT_BOOTSTRAP_INTERVAL);
        bootstrap_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        bootstrap_interval.tick().await;
        let mut connection_prune_interval = time::interval(CONNECTION_PRUNE_INTERVAL);
        connection_prune_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        connection_prune_interval.tick().await;

        while !self.shutdown_requested {
            // Biased select: process swarm events before commands.
            // This ensures ConnectionEstablished events (which update peer_addrs)
            // are processed before PeerAddresses commands read them.
            tokio::select! {
                biased;

                event = self.swarm.select_next_some() => {
                    if self.handle_swarm_event(event).await {
                        if let Some(deadline) = bootstrap_scheduler.schedule_initial(Instant::now()) {
                            bootstrap_deadline.as_mut().reset(deadline);
                            debug!(
                                delay_ms = DHT_BOOTSTRAP_DEBOUNCE.as_millis() as u64,
                                "Scheduled initial Kademlia bootstrap"
                            );
                        }
                    }
                    self.prune_connections("swarm event");
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
                result = self.spawned_tasks.join_next(), if !self.spawned_tasks.is_empty() => {
                    Self::report_spawned_task(result);
                }
                _ = &mut bootstrap_deadline, if bootstrap_scheduler.is_pending() => {
                    bootstrap_scheduler.mark_fired();
                    self.run_kademlia_bootstrap("initial connection");
                }
                _ = bootstrap_interval.tick() => {
                    self.run_kademlia_bootstrap("periodic");
                }
                _ = connection_prune_interval.tick() => {
                    self.prune_connections("periodic");
                }
            }
        }

        info!("P2P host shutdown complete");
    }

    fn run_kademlia_bootstrap(&mut self, reason: &'static str) {
        for (network, result) in self.swarm.behaviour_mut().kademlia.bootstrap() {
            match result {
                Ok(query_id) => {
                    debug!(
                        ?query_id,
                        dht = network.as_str(),
                        reason,
                        "Started Kademlia bootstrap"
                    );
                }
                Err(error) => {
                    debug!(
                        ?error,
                        dht = network.as_str(),
                        reason,
                        "Skipped Kademlia bootstrap"
                    );
                }
            }
        }
    }

    fn prune_connections(&mut self, reason: &'static str) {
        for candidate in self.connection_manager.prune_candidates(Instant::now()) {
            // Abort pending duplicate dials as well as closing established connections.
            if self.swarm.disconnect_peer_id(candidate.peer_id).is_ok() {
                debug!(
                    peer_id = %candidate.peer_id,
                    connection_id = %candidate.connection_id,
                    age_secs = candidate.age.as_secs(),
                    reason,
                    "Pruning established P2P connection above high watermark"
                );
            } else {
                self.connection_manager.on_closed(candidate.connection_id);
            }
        }
    }

    /// Dial a peer at the given addresses.
    ///
    /// Naming the peer lets libp2p skip the dial when one is already connected
    /// or in flight, so a repeated connect cannot stack a second connection
    /// (#1449). Like Go's `Connect`, which returns nil once connected, a
    /// skipped dial is success rather than an error.
    pub(super) fn dial_peer(&mut self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
        let dial_addrs: Vec<Multiaddr> = addrs
            .into_iter()
            .map(|addr| {
                if addr.iter().any(|part| matches!(part, Protocol::P2p(_))) {
                    addr
                } else {
                    addr.with(Protocol::P2p(peer_id))
                }
            })
            .collect();
        if dial_addrs.is_empty() {
            return Err(Error::Dial(format!("no addresses to dial peer {peer_id}")));
        }

        // `allocate_new_port` keeps outbound dials on an ephemeral source port.
        // libp2p 0.56 moved that choice from the TCP transport to the dial, so
        // dropping it would restore the persistent `EADDRINUSE` dial failures
        // that #1021 traced to binding dials to the listen port.
        let dial_opts = DialOpts::peer_id(peer_id)
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .addresses(dial_addrs.clone())
            .allocate_new_port()
            .build();
        match self.swarm.dial(dial_opts) {
            Ok(()) => {
                debug!("Dialing peer {} at {:?}", peer_id, dial_addrs);
                Ok(())
            }
            Err(DialError::DialPeerConditionFalse(_)) => {
                debug!("Peer {} already connected or being dialed", peer_id);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to dial {}: {}", peer_id, e);
                Err(Error::Dial(format!("Failed to dial peer {peer_id}: {e}")))
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
            tracing::error!(
                "Failed to send PushLog response: message_id={}",
                resp.message_id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MockBitswapStore;

    #[test]
    fn default_config_enables_relay() {
        assert!(P2PHostConfig::default().enable_relay);
    }

    #[test]
    fn resource_manager_startup_validation_rejects_low_fd_limit() {
        let err = validate_resource_manager_startup_limits(
            GO_RESOURCE_MANAGER_MIN_FD_LIMIT - 1,
            GO_RESOURCE_MANAGER_MIN_MEMORY_BYTES,
        )
        .expect_err("fd limits below the Go minimum must fail startup validation");

        assert!(
            err.to_string().contains("file descriptors"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resource_manager_startup_validation_rejects_low_memory_budget() {
        let err = validate_resource_manager_startup_limits(
            GO_RESOURCE_MANAGER_MIN_FD_LIMIT,
            GO_RESOURCE_MANAGER_MIN_MEMORY_BYTES - 1,
        )
        .expect_err("memory below the Go minimum must fail startup validation");

        assert!(
            err.to_string().contains("memory budget"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resource_manager_startup_validation_accepts_go_minimums() {
        validate_resource_manager_startup_limits(
            GO_RESOURCE_MANAGER_MIN_FD_LIMIT,
            GO_RESOURCE_MANAGER_MIN_MEMORY_BYTES,
        )
        .expect("Go minimum FD and memory limits must be accepted");
    }

    /// `sysinfo::total_memory` reports bytes; a unit or refresh-kind regression
    /// would be orders of magnitude off and skew the Go-parity memory divisor.
    #[test]
    fn total_system_memory_is_reported_in_bytes() {
        let total = total_system_memory_bytes();

        assert!(
            total > 256 * 1024 * 1024,
            "implausibly small system memory, expected bytes: {total}"
        );
        assert!(
            total < 16 * 1024 * 1024 * 1024 * 1024,
            "implausibly large system memory, expected bytes: {total}"
        );
    }

    #[test]
    fn bootstrap_scheduler_runs_once_after_initial_connection_burst() {
        let mut scheduler = BootstrapScheduler::new(Duration::from_secs(30));
        let now = Instant::now();

        let scheduled: Vec<_> = (0..5)
            .filter_map(|offset| scheduler.schedule_initial(now + Duration::from_secs(offset)))
            .collect();

        assert_eq!(scheduled, vec![now + Duration::from_secs(30)]);
        assert!(scheduler.is_pending());

        scheduler.mark_fired();
        assert!(!scheduler.is_pending());
        assert_eq!(
            scheduler.schedule_initial(now + Duration::from_secs(31)),
            None
        );
    }

    #[tokio::test]
    async fn saturated_event_delivery_still_services_host_commands() {
        let (mut host, handle, mut events, _) =
            P2PHost::new(MockBitswapStore::new()).await.unwrap();
        let expected_peer_id = host.local_peer_id();
        let address: Multiaddr = "/memory/1".parse().unwrap();

        let mut filled = 0;
        loop {
            match host
                .event_tx
                .try_send(HostEvent::Listening(address.clone()))
            {
                Ok(()) => filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(error) => panic!("event receiver unexpectedly closed: {error}"),
            }
        }
        assert_eq!(filled, 256, "test must saturate the real host channel");

        let mut command = Box::pin(handle.local_peer_id());
        let mut forward = Box::pin(host.forward_event(HostEvent::Listening(address)));
        let resolved_peer_id = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut command => result.unwrap(),
                _ = &mut forward => panic!("event forwarding cannot finish while the channel is full"),
            }
        })
        .await
        .expect("a full event channel must not block host command handling");
        assert_eq!(resolved_peer_id, expected_peer_id);

        events.recv().await.unwrap();
        assert!(forward.await);
    }
}
