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
use tracing::{error, info, warn};

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
        self.apply_to_config(&mut config)?;

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
    ///
    /// Returns an error if any flag value fails to parse.
    fn apply_to_config(&self, config: &mut Config) -> Result<()> {
        if let Some(ref peers) = self.peers {
            config.net.peers = peers.clone();
        }
        if let Some(retries) = self.max_txn_retries {
            config.datastore.max_txn_retries = retries;
        }
        if let Some(ref store) = self.store {
            config.datastore.store = store.parse()?;
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn default_start_args() -> StartArgs {
        StartArgs {
            peers: None,
            max_txn_retries: None,
            store: None,
            valuelogfilesize: None,
            p2paddr: None,
            no_p2p: None,
            allowed_origins: None,
            pubkeypath: None,
            privkeypath: None,
            development: None,
            no_encryption: None,
            no_telemetry: None,
            no_signing: None,
            default_key_type: None,
            no_searchable_encryption: None,
            identity: None,
            replicator_retry_intervals: None,
        }
    }

    #[test]
    fn test_apply_to_config_invalid_store_returns_error() {
        let mut config = Config::default();
        let mut args = default_start_args();
        args.store = Some("postgres".to_string());

        let result = args.apply_to_config(&mut config);
        assert!(matches!(result, Err(Error::InvalidDatastore(s)) if s == "postgres"));
    }

    #[test]
    fn test_apply_to_config_valid_store_succeeds() {
        let mut config = Config::default();
        let mut args = default_start_args();
        args.store = Some("memory".to_string());

        let result = args.apply_to_config(&mut config);
        assert!(result.is_ok());
        assert_eq!(config.datastore.store, DatastoreType::Memory);
    }

    #[test]
    fn test_apply_to_config_badger_store_succeeds() {
        let mut config = Config::default();
        config.datastore.store = DatastoreType::Memory; // Start with non-default
        let mut args = default_start_args();
        args.store = Some("badger".to_string());

        let result = args.apply_to_config(&mut config);
        assert!(result.is_ok());
        assert_eq!(config.datastore.store, DatastoreType::Badger);
    }

    #[test]
    fn test_apply_to_config_rocksdb_alias_succeeds() {
        let mut config = Config::default();
        let mut args = default_start_args();
        args.store = Some("rocksdb".to_string());

        let result = args.apply_to_config(&mut config);
        assert!(result.is_ok());
        // rocksdb is an alias for badger in Rust implementation
        assert_eq!(config.datastore.store, DatastoreType::Badger);
    }

    #[test]
    fn test_apply_to_config_all_flags() {
        let mut config = Config::default();
        let args = StartArgs {
            peers: Some(vec!["peer1".to_string(), "peer2".to_string()]),
            max_txn_retries: Some(10),
            store: Some("memory".to_string()),
            valuelogfilesize: Some(2 << 30),
            p2paddr: Some(vec!["/ip4/0.0.0.0/tcp/4001".to_string()]),
            no_p2p: Some(true),
            allowed_origins: Some(vec!["http://localhost:3000".to_string()]),
            pubkeypath: Some("/path/to/pub.key".to_string()),
            privkeypath: Some("/path/to/priv.key".to_string()),
            development: Some(true),
            no_encryption: Some(true),
            no_telemetry: Some(true),
            no_signing: Some(true),
            default_key_type: Some("ed25519".to_string()),
            no_searchable_encryption: Some(true),
            identity: None, // identity is handled separately, not in apply_to_config
            replicator_retry_intervals: Some(vec![10, 20, 30]),
        };

        let result = args.apply_to_config(&mut config);
        assert!(result.is_ok());

        assert_eq!(config.net.peers, vec!["peer1", "peer2"]);
        assert_eq!(config.datastore.max_txn_retries, 10);
        assert_eq!(config.datastore.store, DatastoreType::Memory);
        assert_eq!(config.datastore.valuelogfilesize, 2 << 30);
        assert_eq!(config.net.p2p_addresses, vec!["/ip4/0.0.0.0/tcp/4001"]);
        assert!(config.net.p2p_disabled);
        assert_eq!(config.api.allowed_origins, vec!["http://localhost:3000"]);
        assert_eq!(config.api.pubkey_path, "/path/to/pub.key");
        assert_eq!(config.api.privkey_path, "/path/to/priv.key");
        assert!(config.development);
        assert!(config.datastore.no_encryption);
        assert!(config.telemetry_disabled);
        assert!(config.datastore.no_signing);
        assert_eq!(config.datastore.default_key_type, "ed25519");
        assert!(config.datastore.no_searchable_encryption);
        assert_eq!(config.replicator_retry_intervals, vec![10, 20, 30]);
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
                        other => {
                            tracing::debug!("Unhandled P2P event: {:?}", other);
                        }
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
            match handle.local_peer_id().await {
                Ok(peer_id) => info!("Local peer ID: {}", peer_id),
                Err(e) => error!("Failed to get local peer ID: {}", e),
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
        // Note: We log but don't return errors here because:
        // 1. The user's intent (stop the node) is still fulfilled
        // 2. The node is already stopping - failing would just add noise
        // 3. P2P cleanup issues don't affect data integrity
        if let Some(handle) = &self.p2p_handle {
            if let Err(e) = handle.shutdown().await {
                warn!("P2P shutdown encountered an issue: {}", e);
            }
        }

        info!("DefraDB node shutdown complete");
        Ok(())
    }
}
