// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Start command implementation

use std::sync::Arc;

use clap::Args;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::{Config, DatastoreType};
use crate::error::{Error, Result};

const DEV_MODE_BANNER: &str = r#"
******************************************
**     DEVELOPMENT MODE IS ENABLED      **
** ------------------------------------ **
**   if this is a production database   **
** disable development mode and restart **
**   or you may risk losing all data    **
******************************************
"#;

/// Arguments for the start command
#[derive(Args, Debug)]
pub struct StartArgs {
    /// List of peers to connect to
    #[arg(long, value_delimiter = ',')]
    pub peers: Option<Vec<String>>,

    /// Specify the maximum number of retries per transaction
    #[arg(long)]
    pub max_txn_retries: Option<u32>,

    /// Specify the datastore to use (supported: badger, memory)
    #[arg(long)]
    pub store: Option<String>,

    /// Specify the datastore value log file size (in bytes)
    #[arg(long)]
    pub valuelogfilesize: Option<u64>,

    /// Listen addresses for the p2p network (formatted as a libp2p MultiAddr)
    #[arg(long, value_delimiter = ',')]
    pub p2paddr: Option<Vec<String>>,

    /// Disable the peer-to-peer network synchronization system
    #[arg(long)]
    pub no_p2p: Option<bool>,

    /// List of origins to allow for CORS requests
    #[arg(long, value_delimiter = ',')]
    pub allowed_origins: Option<Vec<String>>,

    /// Path to the public key for TLS
    #[arg(long)]
    pub pubkeypath: Option<String>,

    /// Path to the private key for TLS
    #[arg(long)]
    pub privkeypath: Option<String>,

    /// Enables development mode features
    #[arg(long)]
    pub development: Option<bool>,

    /// Skip generating an encryption key. Encryption at rest will be disabled.
    #[arg(long)]
    pub no_encryption: Option<bool>,

    /// Disable telemetry reporting
    #[arg(long)]
    pub no_telemetry: Option<bool>,

    /// Disable signing of commits
    #[arg(long)]
    pub no_signing: Option<bool>,

    /// Default key type to generate new node identity
    #[arg(long)]
    pub default_key_type: Option<String>,

    /// Skip generating a searchable encryption key
    #[arg(long)]
    pub no_searchable_encryption: Option<bool>,

    /// Hex formatted private key used to authenticate with ACP
    #[arg(short = 'i', long)]
    pub identity: Option<String>,

    /// Retry intervals for the replicator (comma-separated seconds)
    #[arg(long, value_delimiter = ',')]
    pub replicator_retry_intervals: Option<Vec<u32>>,
}

impl StartArgs {
    /// Execute the start command
    pub async fn execute(self, mut config: Config) -> Result<()> {
        // Apply start-specific flags to config
        self.apply_to_config(&mut config);

        // Create config if it doesn't exist
        config.create_if_missing()?;

        // Show development mode banner
        if config.development {
            eprintln!("{}", DEV_MODE_BANNER);
        }

        // Start the node
        let node = Node::new(config).await?;
        node.run().await
    }

    /// Apply start command flags to config
    fn apply_to_config(&self, config: &mut Config) {
        if let Some(ref peers) = self.peers {
            config.net.peers = peers.clone();
        }
        if let Some(retries) = self.max_txn_retries {
            config.datastore.max_txn_retries = retries;
        }
        if let Some(ref store) = self.store {
            if let Ok(s) = store.parse() {
                config.datastore.store = s;
            }
        }
        if let Some(size) = self.valuelogfilesize {
            config.datastore.valuelogfilesize = size;
        }
        if let Some(ref addrs) = self.p2paddr {
            config.net.p2p_addresses = addrs.clone();
        }
        if let Some(no_p2p) = self.no_p2p {
            config.net.p2p_disabled = no_p2p;
        }
        if let Some(ref origins) = self.allowed_origins {
            config.api.allowed_origins = origins.clone();
        }
        if let Some(ref path) = self.pubkeypath {
            config.api.pubkey_path = path.clone();
        }
        if let Some(ref path) = self.privkeypath {
            config.api.privkey_path = path.clone();
        }
        if let Some(dev) = self.development {
            config.development = dev;
        }
        if let Some(no_enc) = self.no_encryption {
            config.datastore.no_encryption = no_enc;
        }
        if let Some(no_tel) = self.no_telemetry {
            config.telemetry_disabled = no_tel;
        }
        if let Some(no_sign) = self.no_signing {
            config.datastore.no_signing = no_sign;
        }
        if let Some(ref key_type) = self.default_key_type {
            config.datastore.default_key_type = key_type.clone();
        }
        if let Some(no_se) = self.no_searchable_encryption {
            config.datastore.no_searchable_encryption = no_se;
        }
        if let Some(ref intervals) = self.replicator_retry_intervals {
            config.replicator_retry_intervals = intervals.clone();
        }
    }
}

/// DefraDB Node
struct Node {
    config: Config,
    _store: Arc<dyn storage::Store + Send + Sync>,
    p2p_handle: Option<p2p::P2PHostHandle>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: mpsc::Receiver<()>,
}

impl Node {
    /// Create a new node
    async fn new(config: Config) -> Result<Self> {
        info!("Initializing DefraDB node");
        info!("Root directory: {}", config.rootdir.display());
        info!("Data directory: {}", config.data_path().display());

        // Initialize storage
        let store: Arc<dyn storage::Store + Send + Sync> = match config.datastore.store {
            DatastoreType::Memory => {
                info!("Using in-memory datastore");
                Arc::new(storage::MemoryStore::new())
            }
            DatastoreType::Badger => {
                info!(
                    "Using RocksDB datastore at {}",
                    config.data_path().display()
                );
                let store = storage::RocksDBStore::open(config.data_path())?;
                Arc::new(store)
            }
        };

        // Initialize P2P (if not disabled)
        let p2p_handle = if !config.net.p2p_disabled {
            info!("Initializing P2P network");
            let (host, handle, mut events) = p2p::P2PHost::new().map_err(Error::P2P)?;

            // Start listening on configured addresses
            for addr_str in &config.net.p2p_addresses {
                let addr: p2p::Multiaddr = addr_str
                    .parse()
                    .map_err(|e| Error::InvalidMultiaddr(format!("{}: {}", addr_str, e)))?;

                handle.listen(addr.clone()).await.map_err(Error::P2P)?;
                info!("P2P listening on {}", addr);
            }

            // Spawn the host event loop
            tokio::spawn(host.run());

            // Spawn event handler
            tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    match event {
                        p2p::HostEvent::PeerConnected(peer) => {
                            info!("Peer connected: {}", peer);
                        }
                        p2p::HostEvent::PeerDisconnected(peer) => {
                            info!("Peer disconnected: {}", peer);
                        }
                        p2p::HostEvent::PeerDiscovered(peer) => {
                            info!("Peer discovered: {}", peer);
                        }
                        p2p::HostEvent::Listening(addr) => {
                            info!("Now listening on: {}", addr);
                        }
                        p2p::HostEvent::GossipMessage {
                            propagation_source,
                            topic,
                            ..
                        } => {
                            info!(
                                "Received gossip message on {} from {}",
                                topic, propagation_source
                            );
                        }
                        _ => {}
                    }
                }
            });

            // Log bootstrap peers (connection will be handled by mDNS discovery)
            if !config.net.peers.is_empty() {
                info!("Bootstrap peers configured: {:?}", config.net.peers);
                info!(
                    "Note: Direct peer connection requires peer ID; mDNS will discover local peers"
                );
            }

            // Get and display peer ID
            if let Ok(peer_id) = handle.local_peer_id().await {
                info!("Local peer ID: {}", peer_id);
            }

            Some(handle)
        } else {
            info!("P2P networking disabled");
            None
        };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            config,
            _store: store,
            p2p_handle,
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// Run the node until shutdown
    async fn run(mut self) -> Result<()> {
        info!("DefraDB node started");
        info!("API endpoint: http://{}", self.config.api.address);

        // Set up signal handling
        let shutdown_tx = self.shutdown_tx.clone();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigint =
                signal(SignalKind::interrupt()).map_err(|e| Error::Signal(e.to_string()))?;
            let mut sigterm =
                signal(SignalKind::terminate()).map_err(|e| Error::Signal(e.to_string()))?;

            tokio::spawn(async move {
                tokio::select! {
                    _ = sigint.recv() => {
                        info!("Received SIGINT");
                    }
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM");
                    }
                }
                let _ = shutdown_tx.send(()).await;
            });
        }

        #[cfg(not(unix))]
        {
            tokio::spawn(async move {
                if let Ok(()) = tokio::signal::ctrl_c().await {
                    info!("Received Ctrl+C");
                    let _ = shutdown_tx.send(()).await;
                }
            });
        }

        // Wait for shutdown signal
        self.shutdown_rx.recv().await;

        info!("Shutting down DefraDB node...");

        // Shutdown P2P
        if let Some(handle) = &self.p2p_handle {
            if let Err(e) = handle.shutdown().await {
                error!("Error shutting down P2P: {}", e);
            }
        }

        info!("DefraDB node shutdown complete");
        Ok(())
    }
}
