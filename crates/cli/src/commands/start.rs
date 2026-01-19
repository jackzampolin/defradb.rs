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

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Args;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::{Config, DatastoreType};
use crate::error::{Error, Result};
use identity::Identity;

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

    /// Specify the datastore to use (supported: badger, rocksdb, memory)
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

    /// Hex formatted private key used to authenticate with ACP.
    ///
    /// The key should be a 64-byte hex string (128 hex characters) for Ed25519,
    /// or a 32-byte hex string (64 hex characters) for secp256k1.
    ///
    /// Example: defra start --identity 0x<hex-encoded-private-key>
    #[arg(short = 'i', long)]
    pub identity: Option<String>,

    /// Key type for the identity (ed25519 or secp256k1).
    /// Only used if --identity is provided.
    #[arg(long, default_value = "ed25519")]
    pub identity_key_type: Option<String>,

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

        // Parse user identity from --identity flag if provided
        let user_identity = self.parse_user_identity()?;

        // Start the node
        let node = Node::new(config, user_identity).await?;
        node.run().await
    }

    /// Parse the user identity from the --identity flag.
    ///
    /// The identity flag should contain a hex-encoded private key.
    /// Supported formats:
    /// - Ed25519: 64-byte key (128 hex chars) or 32-byte seed (64 hex chars)
    /// - secp256k1: 32-byte key (64 hex chars)
    fn parse_user_identity(&self) -> Result<Option<std::sync::Arc<identity::RawIdentity>>> {
        let hex_key = match &self.identity {
            Some(key) => key,
            None => return Ok(None),
        };

        // Remove 0x prefix if present
        let hex_str = hex_key.strip_prefix("0x").unwrap_or(hex_key);

        // Decode hex to bytes
        let key_bytes = hex::decode(hex_str).map_err(|e| {
            Error::InvalidIdentity(format!("invalid hex in --identity flag: {}", e))
        })?;

        // Determine key type
        let key_type_str = self.identity_key_type.as_deref().unwrap_or("ed25519");
        let key_type: identity::IdentityKeyType = key_type_str.parse().map_err(|_| {
            Error::InvalidIdentity(format!(
                "invalid --identity-key-type '{}': expected 'ed25519' or 'secp256k1'",
                key_type_str
            ))
        })?;

        // Create identity from bytes
        let raw_identity = identity::RawIdentity::from_identity_key_type(key_type, &key_bytes)?;

        let did = raw_identity.did()?;
        info!("User identity DID: {}", did);

        Ok(Some(std::sync::Arc::new(raw_identity)))
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
            identity_key_type: None,
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
            identity: None, // identity is handled in Node::new, not apply_to_config
            identity_key_type: None,
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
    p2p_handle: Option<p2p::P2PHostHandle>,
    http_server: Option<defra_http::Server>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: mpsc::Receiver<()>,
    /// User identity from --identity flag (for ACP authentication).
    /// Stored for future use in request context injection.
    #[allow(dead_code)]
    user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
}

impl Node {
    /// Create a new node
    async fn new(
        config: Config,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
    ) -> Result<Self> {
        info!("Initializing DefraDB node");
        info!("Root directory: {}", config.rootdir.display());
        info!("Data directory: {}", config.data_path().display());

        // Initialize peer keypair from keyring (if P2P enabled and keyring not disabled)
        let peer_keypair = if !config.net.p2p_disabled && !config.keyring.disabled {
            Some(Self::init_peer_key(&config)?)
        } else if !config.net.p2p_disabled {
            info!("Keyring disabled, using ephemeral peer identity");
            None
        } else {
            None
        };

        // Initialize storage, database, and set up P2P and HTTP server
        let (p2p_handle, http_server) = match config.datastore.store {
            DatastoreType::Memory => {
                info!("Using in-memory datastore");
                let store = Arc::new(storage::MemoryStore::new());
                // Use in-memory ACP store for memory datastore
                let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
                info!("Using in-memory ACP store");
                Self::init_store_and_server(
                    store,
                    &config,
                    peer_keypair,
                    user_identity.clone(),
                    acp_store,
                )
                .await?
            }
            DatastoreType::Badger => {
                info!(
                    "Using RocksDB datastore at {}",
                    config.data_path().display()
                );
                let store = Arc::new(storage::RocksDBStore::open(config.data_path())?);
                // Use persistent ACP store at <rootdir>/local_document_acp
                let acp_path = config.rootdir.join("local_document_acp");
                info!("Using persistent ACP store at {}", acp_path.display());
                let acp_store: Arc<dyn acp::AcpStore> =
                    Arc::new(acp::PersistentAcpStore::open(&acp_path).map_err(|e| {
                        Error::Storage(storage::Error::Other(format!(
                            "failed to open ACP store: {}",
                            e
                        )))
                    })?);
                Self::init_store_and_server(
                    store,
                    &config,
                    peer_keypair,
                    user_identity.clone(),
                    acp_store,
                )
                .await?
            }
        };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            config,
            p2p_handle,
            http_server: Some(http_server),
            shutdown_tx,
            shutdown_rx,
            user_identity,
        })
    }

    /// Initialize or load the peer key from keyring.
    ///
    /// If a peer key exists in the keyring, it is loaded and converted to a libp2p Keypair.
    /// If no peer key exists, a new Ed25519 key is generated and stored in the keyring.
    fn init_peer_key(config: &Config) -> Result<p2p::Keypair> {
        use crate::config::KeyringBackend;
        use keyring::{FileKeyring, Keyring, SystemKeyring, PEER_KEY};

        let kr: Box<dyn Keyring> = match config.keyring.backend {
            KeyringBackend::File => {
                let path = if config.keyring.path.starts_with('/') {
                    std::path::PathBuf::from(&config.keyring.path)
                } else {
                    config.rootdir.join(&config.keyring.path)
                };
                let secret =
                    keyring::load_secret_from_env().map_err(|e| Error::Keyring(e.to_string()))?;
                Box::new(
                    FileKeyring::open(&path, secret).map_err(|e| Error::Keyring(e.to_string()))?,
                )
            }
            KeyringBackend::System => Box::new(SystemKeyring::open(&config.keyring.namespace)),
        };

        // Try to load existing peer key
        match kr.get(PEER_KEY) {
            Ok(key_bytes) => {
                info!("Loaded existing peer key from keyring");
                Self::derive_and_log_identity_did(&key_bytes)?;
                Self::keypair_from_ed25519_bytes(&key_bytes)
            }
            Err(keyring::Error::NotFound(_)) => {
                info!("Generating new peer key");
                use crypto::Key;
                let private_key = crypto::generate_ed25519()
                    .map_err(|e| Error::Keyring(format!("failed to generate peer key: {}", e)))?;
                let key_bytes = private_key.raw();

                // Store in keyring
                kr.set(PEER_KEY, &key_bytes)
                    .map_err(|e| Error::Keyring(e.to_string()))?;

                Self::derive_and_log_identity_did(&key_bytes)?;
                Self::keypair_from_ed25519_bytes(&key_bytes)
            }
            Err(e) => Err(Error::Keyring(e.to_string())),
        }
    }

    /// Derive and log the node's DID from peer key bytes.
    ///
    /// Creates a RawIdentity from the key bytes, derives its DID, and logs it.
    ///
    /// # Errors
    ///
    /// Returns an error if identity creation or DID derivation fails, which
    /// indicates corrupted key material or a crypto library failure.
    fn derive_and_log_identity_did(key_bytes: &[u8]) -> Result<()> {
        use identity::{Identity, IdentityKeyType, RawIdentity};

        let identity = RawIdentity::from_identity_key_type(IdentityKeyType::Ed25519, key_bytes)?;
        let did = identity
            .did()
            .map_err(|e| Error::Keyring(format!("failed to derive DID: {}", e)))?;
        info!("Node identity DID: {}", did);
        Ok(())
    }

    /// Convert Ed25519 key bytes to libp2p Keypair.
    ///
    /// Ed25519 keys are stored as 64 bytes: 32-byte seed + 32-byte public key.
    /// libp2p expects the 32-byte seed to derive the keypair.
    fn keypair_from_ed25519_bytes(key_bytes: &[u8]) -> Result<p2p::Keypair> {
        use libp2p::identity::ed25519;

        if key_bytes.len() != 64 {
            return Err(Error::Keyring(format!(
                "invalid peer key length: expected 64 bytes, got {}",
                key_bytes.len()
            )));
        }

        // Ed25519 key format: 32-byte seed + 32-byte public key
        // libp2p needs the seed (first 32 bytes) to derive the keypair
        let seed: [u8; 32] = key_bytes[..32]
            .try_into()
            .map_err(|_| Error::Keyring("invalid key format".to_string()))?;

        let secret_key = ed25519::SecretKey::try_from_bytes(seed)
            .map_err(|e| Error::Keyring(format!("invalid Ed25519 key: {}", e)))?;

        Ok(p2p::Keypair::from(ed25519::Keypair::from(secret_key)))
    }

    /// Initialize store, database, P2P, and HTTP server.
    ///
    /// This function creates the database, loads collections, sets up the query
    /// runner with proper transaction support, and returns the HTTP server.
    async fn init_store_and_server<S>(
        store: Arc<S>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
        acp_store: Arc<dyn acp::AcpStore>,
    ) -> Result<(Option<p2p::P2PHostHandle>, defra_http::Server)>
    where
        S: storage::corekv::Store + 'static,
    {
        // Extract DID from user identity for query runner (before consuming it)
        let user_did = match &user_identity {
            Some(identity) => match identity.did() {
                Ok(did) => Some(did),
                Err(e) => {
                    warn!("Failed to extract DID from user identity: {}", e);
                    None
                }
            },
            None => None,
        };

        // Build database options with optional user identity
        let mut db_options = db::DbOptions::new();
        if let Some(identity) = user_identity {
            db_options = db_options.with_node_identity_arc(identity);
            info!("Database configured with user identity");
        }

        // Open database and load collections from storage
        let database = Arc::new(
            db::DB::open_from_arc_with_options(store.clone(), db_options)
                .await
                .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?,
        );

        let collection_count = database
            .list_collections()
            .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?
            .len();
        info!("Loaded {} collection schema(s)", collection_count);

        // Set up P2P if enabled
        let p2p = if config.net.p2p_disabled {
            None
        } else {
            info!("Initializing P2P network");
            let blockstore = Arc::new(blockstore::DefraBlockstore::new(store, false));
            let bitswap_store = p2p::BitswapStoreAdapter::new(blockstore);
            Some(Self::start_p2p(config, bitswap_store, peer_keypair).await?)
        };

        // Create HTTP server with database-backed query runner
        let http_server = {
            let api_address: SocketAddr =
                config
                    .api
                    .address
                    .parse()
                    .map_err(|e: std::net::AddrParseError| {
                        Error::InvalidApiAddress(config.api.address.clone(), e.to_string())
                    })?;

            let server_config = defra_http::ServerConfig {
                address: api_address,
                allowed_origins: config.api.allowed_origins.clone(),
            };

            // Create auto-committing fetcher for non-transactional queries
            let fetcher = db::AutoCommitFetcher::new(database.clone());

            // Create auto-committing mutator for non-transactional mutations
            let mutator = std::sync::Arc::new(db::AutoCommitMutator::new(database.clone()));

            // Create transaction registry for explicit transaction support
            let registry = db::DbTransactionRegistry::new(database.clone());

            // Get collection schemas for the query runner
            let collection_names = database
                .list_collections()
                .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?;

            let mut collections: Vec<schema::CollectionVersion> = Vec::new();
            for name in &collection_names {
                match database.get_collection(name) {
                    Ok(Some(c)) => collections.push(c.schema().clone()),
                    Ok(None) => {
                        warn!("Collection '{}' listed but not found", name);
                    }
                    Err(e) => {
                        return Err(Error::Storage(storage::Error::Other(format!(
                            "failed to load collection '{}': {}",
                            name, e
                        ))));
                    }
                }
            }

            // Create LocalDocumentACP with the provided store
            let document_acp: Arc<dyn acp::DocumentACP> =
                Arc::new(acp::LocalDocumentACP::new(acp_store));
            info!("Document ACP configured");

            // Create query runner with transaction, mutation, and ACP support
            let mut query_runner = query::QueryRunner::with_registry(fetcher, collections, registry)
                .with_mutator(mutator)
                .with_acp(document_acp);

            // Wire default identity for ACP permission checks (from --identity CLI flag)
            if let Some(did) = user_did {
                info!("Query runner configured with default identity for ACP");
                query_runner = query_runner.with_default_identity(did);
            }

            let runner = Arc::new(query_runner);

            // Create REST operations that wrap the query runner
            let rest_ops = query::rest::RestOperationsImpl::new(Arc::clone(&runner));

            // Create HTTP server with REST endpoints enabled
            // Cast the Arc<QueryRunner> to Arc<dyn QueryExecutor> for the server
            let executor: Arc<dyn query::executor::QueryExecutor> = runner;
            let server = defra_http::Server::from_arc_with_config(executor, server_config)
                .with_rest(rest_ops);

            info!(
                "HTTP server configured on {} with REST endpoints enabled",
                api_address
            );
            server
        };

        Ok((p2p, http_server))
    }

    /// Start P2P networking with the given bitswap store and optional keypair.
    ///
    /// If a keypair is provided, it will be used for the P2P identity.
    /// Otherwise, an ephemeral keypair will be generated.
    async fn start_p2p<S: p2p::BitswapStore<Params = libipld::DefaultParams>>(
        config: &Config,
        bitswap_store: S,
        keypair: Option<p2p::Keypair>,
    ) -> Result<p2p::P2PHostHandle> {
        let (host, handle, mut events, _replicators) = match keypair {
            Some(kp) => p2p::P2PHost::with_keypair(kp, bitswap_store).map_err(Error::P2P)?,
            None => p2p::P2PHost::new(bitswap_store).map_err(Error::P2P)?,
        };

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
            info!("Note: Direct peer connection requires peer ID; mDNS will discover local peers");
        }

        // Get and display peer ID
        match handle.local_peer_id().await {
            Ok(peer_id) => info!("Local peer ID: {}", peer_id),
            Err(e) => error!("Failed to get local peer ID: {}", e),
        }

        Ok(handle)
    }

    /// Run the node until shutdown
    async fn run(mut self) -> Result<()> {
        info!("DefraDB node started");
        info!("API endpoint: http://{}", self.config.api.address);

        // Start HTTP server
        let http_task: Option<JoinHandle<()>> = if let Some(server) = self.http_server.take() {
            info!("Starting HTTP server on {}", self.config.api.address);
            Some(tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    error!("HTTP server error: {}", e);
                }
            }))
        } else {
            None
        };

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
                if let Err(e) = shutdown_tx.send(()).await {
                    error!("Failed to send shutdown signal: {}", e);
                }
            });
        }

        #[cfg(not(unix))]
        {
            tokio::spawn(async move {
                match tokio::signal::ctrl_c().await {
                    Ok(()) => {
                        info!("Received Ctrl+C");
                        if let Err(e) = shutdown_tx.send(()).await {
                            error!("Failed to send shutdown signal: {}", e);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to listen for Ctrl+C signal: {}. Node may not respond to interrupt signals.",
                            e
                        );
                    }
                }
            });
        }

        // Wait for shutdown signal OR HTTP server crash
        let mut http_task = http_task;
        let http_crashed = match &mut http_task {
            Some(task) => {
                tokio::select! {
                    _ = self.shutdown_rx.recv() => false,
                    result = task => {
                        match result {
                            Ok(()) => {
                                error!("HTTP server exited unexpectedly");
                            }
                            Err(e) if e.is_panic() => {
                                error!("HTTP server panicked: {}", e);
                            }
                            Err(e) => {
                                error!("HTTP server task failed: {}", e);
                            }
                        }
                        true
                    }
                }
            }
            None => {
                self.shutdown_rx.recv().await;
                false
            }
        };

        if http_crashed {
            info!("Initiating shutdown due to HTTP server failure...");
        } else {
            info!("Shutting down DefraDB node...");
            // Only abort if we're shutting down normally (not due to crash)
            if let Some(task) = http_task {
                info!("Stopping HTTP server...");
                task.abort();
                match tokio::time::timeout(std::time::Duration::from_secs(1), task).await {
                    Ok(_) => info!("HTTP server stopped"),
                    Err(_) => warn!(
                        timeout_secs = 1,
                        "HTTP server shutdown timed out - server was forcefully terminated. \
                         This may occur if requests were still in flight."
                    ),
                }
            }
        }

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

#[cfg(test)]
mod http_integration_tests {
    use super::*;
    use std::time::Duration;

    /// Wait for server to be ready by polling health endpoint with retries.
    async fn wait_for_server(api_url: &str, max_attempts: u32) {
        let client = reqwest::Client::new();
        for attempt in 1..=max_attempts {
            match client
                .get(format!("{}/health-check", api_url))
                .timeout(Duration::from_millis(100))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return,
                _ => {
                    if attempt < max_attempts {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
        panic!(
            "Server at {} failed to become ready after {} attempts",
            api_url, max_attempts
        );
    }

    /// Create a test config with random port and P2P disabled
    fn test_config(port: u16, temp_dir: &std::path::Path) -> Config {
        Config {
            rootdir: temp_dir.to_path_buf(),
            log: crate::config::LogConfig::default(),
            api: crate::config::ApiConfig {
                address: format!("127.0.0.1:{}", port),
                allowed_origins: vec![],
                pubkey_path: String::new(),
                privkey_path: String::new(),
            },
            datastore: crate::config::DatastoreConfig {
                store: DatastoreType::Memory,
                path: String::new(),
                max_txn_retries: 5,
                valuelogfilesize: 1 << 30,
                no_encryption: true,
                no_signing: true,
                no_searchable_encryption: true,
                default_key_type: "ed25519".to_string(),
            },
            net: crate::config::NetConfig {
                p2p_disabled: true, // Disable P2P for HTTP-only tests
                p2p_addresses: vec![],
                peers: vec![],
                pubsub_enabled: false,
                relay_enabled: false,
            },
            keyring: crate::config::KeyringConfig::default(),
            development: false,
            secret_file: String::new(),
            telemetry_disabled: true,
            replicator_retry_intervals: vec![],
        }
    }

    #[tokio::test]
    async fn test_http_server_starts_and_serves_health_check() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let config = test_config(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);

        // Create node
        let node = Node::new(config, None).await.unwrap();

        // Get shutdown sender before moving node
        let shutdown_tx = node.shutdown_tx.clone();

        // Spawn node in background
        let node_handle = tokio::spawn(async move { node.run().await });

        // Wait for server to be ready
        wait_for_server(&api_url, 20).await;

        // Test health check endpoint
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/health-check", api_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to connect to health check");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body = response.text().await.unwrap();
        assert_eq!(body, "Healthy");

        // Shutdown
        shutdown_tx.send(()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), node_handle)
            .await
            .expect("Node shutdown timed out")
            .expect("Node task panicked");
        assert!(result.is_ok(), "Node shutdown failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_http_server_serves_version_endpoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let config = test_config(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);

        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });

        wait_for_server(&api_url, 20).await;

        // Test version endpoint
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/api/v0/version", api_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to connect to version endpoint");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        assert!(
            body.get("version").is_some(),
            "Response should contain version"
        );
        assert!(
            body.get("commit").is_some(),
            "Response should contain commit"
        );

        // Shutdown
        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    #[tokio::test]
    async fn test_http_server_serves_graphql_endpoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let config = test_config(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);

        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });

        wait_for_server(&api_url, 20).await;

        // Test GraphQL endpoint - expect error since no schema is loaded
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(r#"{"query": "{ users { name } }"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to connect to graphql endpoint");

        // Should return 200 OK even with errors (GraphQL spec)
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        // With empty schema, we expect an error about collection not found
        assert!(
            body.get("errors").is_some() || body.get("data").is_some(),
            "Response should be valid GraphQL response"
        );

        // Shutdown
        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    #[tokio::test]
    async fn test_http_server_schema_endpoint_returns_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let config = test_config(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);

        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });

        wait_for_server(&api_url, 20).await;

        // Test schema endpoint - should return empty or minimal schema
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/api/v0/schema", api_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to connect to schema endpoint");

        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Shutdown
        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// Create a test config with RocksDB backend
    fn test_config_rocksdb(port: u16, temp_dir: &std::path::Path) -> Config {
        Config {
            rootdir: temp_dir.to_path_buf(),
            log: crate::config::LogConfig::default(),
            api: crate::config::ApiConfig {
                address: format!("127.0.0.1:{}", port),
                allowed_origins: vec![],
                pubkey_path: String::new(),
                privkey_path: String::new(),
            },
            datastore: crate::config::DatastoreConfig {
                store: DatastoreType::Badger, // Use RocksDB backend
                path: String::new(),
                max_txn_retries: 5,
                valuelogfilesize: 1 << 30,
                no_encryption: true,
                no_signing: true,
                no_searchable_encryption: true,
                default_key_type: "ed25519".to_string(),
            },
            net: crate::config::NetConfig {
                p2p_disabled: true,
                p2p_addresses: vec![],
                peers: vec![],
                pubsub_enabled: false,
                relay_enabled: false,
            },
            keyring: crate::config::KeyringConfig::default(),
            development: false,
            secret_file: String::new(),
            telemetry_disabled: true,
            replicator_retry_intervals: vec![],
        }
    }

    #[tokio::test]
    async fn test_http_server_with_rocksdb_backend() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);

        // Create node with RocksDB backend
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });

        wait_for_server(&api_url, 20).await;

        // Test health check works with RocksDB
        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/health-check", api_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to connect to health check");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "Healthy");

        // Test schema endpoint - should be empty for fresh database
        let response = client
            .get(format!("{}/api/v0/schema", api_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to connect to schema endpoint");

        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Shutdown
        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// End-to-end test: pre-seed database with documents, query via HTTP
    #[tokio::test]
    async fn test_http_graphql_returns_documents_from_database() {
        use document::NormalValue;
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        // Use the same path that Node will use (rootdir when datastore.path is empty)
        let data_path = temp_dir.path();

        // Phase 1: Pre-seed database with collection and documents
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);

            // Create Users collection
            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                    FieldDescription::new("3", "age", FieldKind::int()),
                ],
            );
            database.create_collection(schema).await.unwrap();

            // Insert test documents
            let collection = database.get_collection("Users").unwrap().unwrap();
            let txn = database.new_txn(false).await.unwrap();

            let mut doc1 = document::Document::new();
            doc1.set("name", NormalValue::String("Alice".to_string()));
            doc1.set("age", NormalValue::Int(30));
            doc1.generate_and_set_doc_id().unwrap();
            collection.create(&txn, &doc1).await.unwrap();

            let mut doc2 = document::Document::new();
            doc2.set("name", NormalValue::String("Bob".to_string()));
            doc2.set("age", NormalValue::Int(25));
            doc2.generate_and_set_doc_id().unwrap();
            collection.create(&txn, &doc2).await.unwrap();

            txn.commit().await.unwrap();
            database.close().await.unwrap();
        }

        // Phase 2: Start server and query via HTTP
        let config = Config {
            rootdir: temp_dir.path().to_path_buf(),
            log: crate::config::LogConfig::default(),
            api: crate::config::ApiConfig {
                address: format!("127.0.0.1:{}", port),
                allowed_origins: vec![],
                pubkey_path: String::new(),
                privkey_path: String::new(),
            },
            datastore: crate::config::DatastoreConfig {
                store: DatastoreType::Badger, // Uses RocksDB
                path: String::new(),
                max_txn_retries: 5,
                valuelogfilesize: 1 << 30,
                no_encryption: true,
                no_signing: true,
                no_searchable_encryption: true,
                default_key_type: "ed25519".to_string(),
            },
            net: crate::config::NetConfig {
                p2p_disabled: true,
                p2p_addresses: vec![],
                peers: vec![],
                pubsub_enabled: false,
                relay_enabled: false,
            },
            keyring: crate::config::KeyringConfig::default(),
            development: false,
            secret_file: String::new(),
            telemetry_disabled: true,
            replicator_retry_intervals: vec![],
        };

        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });

        wait_for_server(&api_url, 20).await;

        // Query documents via GraphQL
        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(r#"{"query": "{ Users { name age } }"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to query graphql endpoint");

        assert_eq!(response.status(), reqwest::StatusCode::OK);

        let body: serde_json::Value = response.json().await.unwrap();

        // Verify we got data back (not just errors)
        let data = body.get("data").expect("Response should have data field");
        let users = data.get("Users").expect("Data should have Users field");
        let users_array = users.as_array().expect("Users should be an array");

        assert_eq!(users_array.len(), 2, "Should have 2 users");

        // Verify document contents
        let names: Vec<&str> = users_array
            .iter()
            .filter_map(|u| u.get("name").and_then(|n| n.as_str()))
            .collect();
        assert!(names.contains(&"Alice"), "Should contain Alice");
        assert!(names.contains(&"Bob"), "Should contain Bob");

        // Shutdown
        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    // =========================================================================
    // Integration tests for Issue #67: GraphQL Mutations
    // =========================================================================

    /// Test creating a document via GraphQL mutation through HTTP
    #[tokio::test]
    async fn test_http_graphql_create_mutation() {
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let data_path = temp_dir.path();

        // Phase 1: Pre-seed database with collection (no documents)
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);

            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                    FieldDescription::new("3", "age", FieldKind::int()),
                ],
            );
            database.create_collection(schema).await.unwrap();
            database.close().await.unwrap();
        }

        // Phase 2: Start server and create document via mutation
        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Create a document via mutation
        let create_mutation = r#"{
            "query": "mutation { create_Users(input: {name: \"Charlie\", age: 35}) { _docID name age } }"
        }"#;

        let response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(create_mutation)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("Failed to execute create mutation");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();

        // Verify mutation succeeded
        // Note: Response uses collection name "Users" as key, not "create_Users"
        let data = body.get("data").expect("Response should have data field");
        let created_array = data
            .get("Users")
            .and_then(|u| u.as_array())
            .expect("Data should have Users array");
        assert_eq!(created_array.len(), 1, "Should have created 1 document");
        let created = &created_array[0];
        assert_eq!(
            created.get("name").and_then(|n| n.as_str()),
            Some("Charlie")
        );
        assert_eq!(created.get("age").and_then(|n| n.as_i64()), Some(35));
        let doc_id = created
            .get("_docID")
            .and_then(|d| d.as_str())
            .expect("Should return _docID");
        assert!(doc_id.starts_with("bae-"), "DocID should start with bae-");

        // Verify document persisted by querying it back
        let query_response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(r#"{"query": "{ Users { name age } }"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to query users");

        let query_body: serde_json::Value = query_response.json().await.unwrap();
        let users = query_body["data"]["Users"]
            .as_array()
            .expect("Should have Users array");
        assert_eq!(users.len(), 1, "Should have exactly 1 user");
        assert_eq!(users[0]["name"].as_str(), Some("Charlie"));

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// Test updating a document via GraphQL mutation through HTTP
    #[tokio::test]
    async fn test_http_graphql_update_mutation() {
        use document::NormalValue;
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let data_path = temp_dir.path();

        // Phase 1: Pre-seed database with a document
        let doc_id: String;
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);

            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                    FieldDescription::new("3", "age", FieldKind::int()),
                ],
            );
            database.create_collection(schema).await.unwrap();

            let collection = database.get_collection("Users").unwrap().unwrap();
            let txn = database.new_txn(false).await.unwrap();

            let mut doc = document::Document::new();
            doc.set("name", NormalValue::String("Diana".to_string()));
            doc.set("age", NormalValue::Int(28));
            doc.generate_and_set_doc_id().unwrap();
            doc_id = doc.id().unwrap().to_string();
            collection.create(&txn, &doc).await.unwrap();

            txn.commit().await.unwrap();
            database.close().await.unwrap();
        }

        // Phase 2: Start server and update the document
        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Update the document
        let update_mutation = format!(
            r#"{{"query": "mutation {{ update_Users(docIDs: [\"{}\"], input: {{age: 29}}) {{ _docID name age }} }}"}}"#,
            doc_id
        );

        let response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(update_mutation)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("Failed to execute update mutation");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();

        // Debug: print full response
        println!(
            "Update mutation response: {}",
            serde_json::to_string_pretty(&body).unwrap()
        );

        // Verify mutation succeeded
        // Note: Response uses collection name "Users" as key, not "update_Users"
        let data = body.get("data").expect("Response should have data field");
        let updated = data
            .get("Users")
            .and_then(|u| u.as_array())
            .expect("Users should be an array");
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0]["age"].as_i64(), Some(29));
        assert_eq!(updated[0]["name"].as_str(), Some("Diana"));

        // Verify persistence
        let query_response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(r#"{"query": "{ Users { name age } }"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        let query_body: serde_json::Value = query_response.json().await.unwrap();
        let users = query_body["data"]["Users"].as_array().unwrap();
        assert_eq!(users[0]["age"].as_i64(), Some(29));

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// Test deleting a document via GraphQL mutation through HTTP
    #[tokio::test]
    async fn test_http_graphql_delete_mutation() {
        use document::NormalValue;
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let data_path = temp_dir.path();

        // Phase 1: Pre-seed database with two documents
        let doc_id_to_delete: String;
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);

            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                    FieldDescription::new("3", "age", FieldKind::int()),
                ],
            );
            database.create_collection(schema).await.unwrap();

            let collection = database.get_collection("Users").unwrap().unwrap();
            let txn = database.new_txn(false).await.unwrap();

            let mut doc1 = document::Document::new();
            doc1.set("name", NormalValue::String("Eve".to_string()));
            doc1.set("age", NormalValue::Int(22));
            doc1.generate_and_set_doc_id().unwrap();
            doc_id_to_delete = doc1.id().unwrap().to_string();
            collection.create(&txn, &doc1).await.unwrap();

            let mut doc2 = document::Document::new();
            doc2.set("name", NormalValue::String("Frank".to_string()));
            doc2.set("age", NormalValue::Int(40));
            doc2.generate_and_set_doc_id().unwrap();
            collection.create(&txn, &doc2).await.unwrap();

            txn.commit().await.unwrap();
            database.close().await.unwrap();
        }

        // Phase 2: Start server and delete one document
        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Delete the first document
        let delete_mutation = format!(
            r#"{{"query": "mutation {{ delete_Users(docIDs: [\"{}\"]) {{ _docID }} }}"}}"#,
            doc_id_to_delete
        );

        let response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(delete_mutation)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("Failed to execute delete mutation");

        assert_eq!(response.status(), reqwest::StatusCode::OK);

        // Verify only one document remains
        let query_response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(r#"{"query": "{ Users { name } }"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        let query_body: serde_json::Value = query_response.json().await.unwrap();
        let users = query_body["data"]["Users"].as_array().unwrap();
        assert_eq!(users.len(), 1, "Should have only 1 user after deletion");
        assert_eq!(users[0]["name"].as_str(), Some("Frank"));

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    // =========================================================================
    // Integration tests for Issue #68: Transaction HTTP Endpoints
    // =========================================================================

    /// Test transaction begin endpoint
    #[tokio::test]
    async fn test_http_transaction_begin() {
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let data_path = temp_dir.path();

        // Pre-seed database with collection
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);
            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                ],
            );
            database.create_collection(schema).await.unwrap();
            database.close().await.unwrap();
        }

        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Begin a transaction
        let response = client
            .post(format!("{}/api/v0/tx/begin", api_url))
            .header("content-type", "application/json")
            .body(r#"{"readonly": false}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("Failed to begin transaction");

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let txn_id = body
            .get("txn_id")
            .and_then(|t| t.as_str())
            .expect("Should return txn_id");
        assert!(!txn_id.is_empty(), "Transaction ID should not be empty");

        // Begin a read-only transaction
        let response = client
            .post(format!("{}/api/v0/tx/begin", api_url))
            .header("content-type", "application/json")
            .body(r#"{"readonly": true}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let readonly_txn_id = body.get("txn_id").and_then(|t| t.as_str()).unwrap();
        assert_ne!(
            txn_id, readonly_txn_id,
            "Should get different transaction IDs"
        );

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// Test full transaction lifecycle: begin, query, commit
    #[tokio::test]
    async fn test_http_transaction_commit_flow() {
        use document::NormalValue;
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let data_path = temp_dir.path();

        // Pre-seed database with collection and document
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);
            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                    FieldDescription::new("3", "age", FieldKind::int()),
                ],
            );
            database.create_collection(schema).await.unwrap();

            let collection = database.get_collection("Users").unwrap().unwrap();
            let txn = database.new_txn(false).await.unwrap();
            let mut doc = document::Document::new();
            doc.set("name", NormalValue::String("Grace".to_string()));
            doc.set("age", NormalValue::Int(33));
            doc.generate_and_set_doc_id().unwrap();
            collection.create(&txn, &doc).await.unwrap();
            txn.commit().await.unwrap();
            database.close().await.unwrap();
        }

        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Step 1: Begin a transaction
        let begin_response = client
            .post(format!("{}/api/v0/tx/begin", api_url))
            .header("content-type", "application/json")
            .body(r#"{"readonly": true}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        let begin_body: serde_json::Value = begin_response.json().await.unwrap();
        let txn_id = begin_body["txn_id"].as_str().unwrap();

        // Step 2: Query within the transaction
        let query_body = format!(
            r#"{{"query": "{{ Users {{ name age }} }}", "txn_id": "{}"}}"#,
            txn_id
        );
        let query_response = client
            .post(format!("{}/api/v0/graphql", api_url))
            .header("content-type", "application/json")
            .body(query_body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        assert_eq!(query_response.status(), reqwest::StatusCode::OK);
        let query_result: serde_json::Value = query_response.json().await.unwrap();
        let users = query_result["data"]["Users"].as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0]["name"].as_str(), Some("Grace"));

        // Step 3: Commit the transaction
        let commit_body = format!(r#"{{"txn_id": "{}"}}"#, txn_id);
        let commit_response = client
            .post(format!("{}/api/v0/tx/commit", api_url))
            .header("content-type", "application/json")
            .body(commit_body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        assert_eq!(commit_response.status(), reqwest::StatusCode::OK);
        let commit_body: serde_json::Value = commit_response.json().await.unwrap();
        assert_eq!(commit_body["status"].as_str(), Some("committed"));

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// Test transaction rollback
    #[tokio::test]
    async fn test_http_transaction_rollback() {
        use schema::{CollectionVersion, FieldDescription, FieldKind};

        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");
        let data_path = temp_dir.path();

        // Pre-seed database with collection
        {
            let store = storage::RocksDBStore::open(data_path).unwrap();
            let database = db::DB::new(store);
            let schema = CollectionVersion::new(
                "Users",
                "v1",
                "col-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "name", FieldKind::string()),
                ],
            );
            database.create_collection(schema).await.unwrap();
            database.close().await.unwrap();
        }

        let config = test_config_rocksdb(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Begin a transaction
        let begin_response = client
            .post(format!("{}/api/v0/tx/begin", api_url))
            .header("content-type", "application/json")
            .body(r#"{"readonly": false}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        let begin_body: serde_json::Value = begin_response.json().await.unwrap();
        let txn_id = begin_body["txn_id"].as_str().unwrap();

        // Rollback the transaction
        let rollback_body = format!(r#"{{"txn_id": "{}"}}"#, txn_id);
        let rollback_response = client
            .post(format!("{}/api/v0/tx/rollback", api_url))
            .header("content-type", "application/json")
            .body(rollback_body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        assert_eq!(rollback_response.status(), reqwest::StatusCode::OK);
        let rollback_result: serde_json::Value = rollback_response.json().await.unwrap();
        assert_eq!(rollback_result["status"].as_str(), Some("rolled_back"));

        // Verify the transaction is no longer valid (double rollback should fail)
        let double_rollback = client
            .post(format!("{}/api/v0/tx/rollback", api_url))
            .header("content-type", "application/json")
            .body(format!(r#"{{"txn_id": "{}"}}"#, txn_id))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        assert_eq!(double_rollback.status(), reqwest::StatusCode::BAD_REQUEST);

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }

    /// Test invalid transaction ID returns error
    #[tokio::test]
    async fn test_http_transaction_invalid_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = portpicker::pick_unused_port().expect("No free ports");

        let config = test_config(port, temp_dir.path());
        let api_url = format!("http://127.0.0.1:{}", port);
        let node = Node::new(config, None).await.unwrap();
        let shutdown_tx = node.shutdown_tx.clone();

        let node_handle = tokio::spawn(async move { node.run().await });
        wait_for_server(&api_url, 20).await;

        let client = reqwest::Client::new();

        // Try to commit a non-existent transaction
        let response = client
            .post(format!("{}/api/v0/tx/commit", api_url))
            .header("content-type", "application/json")
            .body(r#"{"txn_id": "nonexistent-txn-id"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

        shutdown_tx.send(()).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), node_handle).await;
    }
}
