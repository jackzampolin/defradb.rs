//! Node struct and constructor

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::config::{Config, DatastoreType};
use crate::error::Result;
#[cfg(feature = "fjall")]
use storage::backends::FjallStoreOptions;
#[cfg(feature = "redb")]
use storage::backends::RedbStoreOptions;
#[cfg(feature = "rocksdb")]
use storage::backends::RocksDbStoreOptions;

/// Tracks spawned P2P background tasks for graceful shutdown.
pub(super) struct P2PTasks {
    /// Coordinator-owned background replication shutdown.
    pub coordinator: p2p::sync::SyncShutdownHandle,
    /// P2P host event loop task
    pub host_task: JoinHandle<()>,
    /// Replication loop task (processes incoming blocks)
    pub replication_task: JoinHandle<()>,
    /// Host event handler task (processes P2P events through coordinator)
    pub event_handler_task: Option<JoinHandle<()>>,
    /// Records push failures for retry
    pub failure_recorder_task: JoinHandle<()>,
    /// Periodically retries failed doc pushes with exponential backoff
    pub retry_loop_task: JoinHandle<()>,
}

/// DefraDB Node
/// Node manages the DefraDB server lifecycle.
#[doc(hidden)]
pub struct Node {
    pub(super) config: Config,
    pub(super) p2p_handle: Option<p2p::P2PHostHandle>,
    pub(super) p2p_tasks: Option<P2PTasks>,
    pub(super) downsample_task: Option<JoinHandle<()>>,
    pub(super) txn_cleanup_task: Option<JoinHandle<()>>,
    pub(super) http_server: Option<defra_http::Server>,
    pub(super) pg_server: Option<pg_compat::PgServer>,
    /// Shutdown signal sender (for tests)
    #[doc(hidden)]
    pub shutdown_tx: mpsc::Sender<()>,
    pub(super) shutdown_rx: mpsc::Receiver<()>,
    /// User identity from --identity flag (for ACP authentication).
    /// Stored for future use in request context injection.
    #[allow(dead_code)]
    user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
}

impl Node {
    /// Load the keyring `encryption-key`, generating and persisting a new
    /// 32-byte AES-256 key on first use (Go's `getOrCreateEncryptionKey`).
    ///
    /// Errors if the keyring is disabled or the stored key is not 32 bytes.
    fn load_or_create_encryption_key(config: &Config) -> Result<[u8; 32]> {
        use crate::error::Error;
        use keyring::ENCRYPTION_KEY;

        if config.keyring.disabled {
            return Err(Error::Keyring(
                "at-rest encryption requires a keyring, but the keyring is disabled".into(),
            ));
        }

        let kr = crate::commands::open_keyring(config)?;
        let key_bytes: Vec<u8> = match kr.get(ENCRYPTION_KEY) {
            Ok(bytes) => bytes.to_vec(),
            Err(keyring::Error::NotFound(_)) => {
                info!("Generating new at-rest encryption key");
                let generated = crypto::generate_aes256().map_err(|e| {
                    Error::Keyring(format!("failed to generate encryption key: {}", e))
                })?;
                kr.set(ENCRYPTION_KEY, &generated)
                    .map_err(|e| Error::Keyring(e.to_string()))?;
                generated
            }
            Err(e) => return Err(Error::Keyring(e.to_string())),
        };

        <[u8; 32]>::try_from(key_bytes.as_slice()).map_err(|_| {
            Error::Keyring(format!(
                "keyring encryption-key must be 32 bytes, got {}",
                key_bytes.len()
            ))
        })
    }

    /// Load the keyring `searchable-encryption-key`, generating and persisting
    /// a new 32-byte AES-256 key on first use (Go's
    /// `getOrCreateSearchableEncryptionKey`, cli/start.go).
    ///
    /// Returns `Ok(None)` when SE is disabled (`--no-searchable-encryption`) or
    /// the keyring is disabled (`--no-keyring`) -- mirrors Go, which only loads
    /// the key when a keyring exists and SE is not disabled.
    fn load_or_create_searchable_encryption_key(config: &Config) -> Result<Option<[u8; 32]>> {
        use crate::error::Error;
        use keyring::SEARCHABLE_ENCRYPTION_KEY;

        if config.datastore.no_searchable_encryption || config.keyring.disabled {
            return Ok(None);
        }

        let kr = crate::commands::open_keyring(config)?;
        let key_bytes: Vec<u8> = match kr.get(SEARCHABLE_ENCRYPTION_KEY) {
            Ok(bytes) => bytes.to_vec(),
            Err(keyring::Error::NotFound(_)) => {
                info!("Generating new searchable encryption key");
                let generated = crypto::generate_aes256().map_err(|e| {
                    Error::Keyring(format!(
                        "failed to generate searchable encryption key: {}",
                        e
                    ))
                })?;
                kr.set(SEARCHABLE_ENCRYPTION_KEY, &generated)
                    .map_err(|e| Error::Keyring(e.to_string()))?;
                generated
            }
            Err(e) => return Err(Error::Keyring(e.to_string())),
        };

        let key = <[u8; 32]>::try_from(key_bytes.as_slice()).map_err(|_| {
            Error::Keyring(format!(
                "keyring searchable-encryption-key must be 32 bytes, got {}",
                key_bytes.len()
            ))
        })?;
        Ok(Some(key))
    }

    /// Build the persistent ACP/Zanzibar stores from a shared store and start
    /// the server. The store may be a bare backend or an [`EncryptedStore`];
    /// both DB and ACP share the same instance so they encrypt uniformly.
    #[allow(clippy::type_complexity)]
    async fn init_persistent_store_and_server<S>(
        store: Arc<S>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
        node_identity_did: Option<String>,
        se_key: Option<[u8; 32]>,
    ) -> Result<(
        Option<p2p::P2PHostHandle>,
        Option<P2PTasks>,
        Option<JoinHandle<()>>,
        Option<JoinHandle<()>>,
        defra_http::Server,
        Option<pg_compat::PgServer>,
    )>
    where
        S: storage::corekv::Store + 'static,
    {
        info!("Using unified ACP store (namespace isolated in main database)");
        let acp_store: Arc<dyn acp::AcpStore> =
            Arc::new(acp::PersistentAcpStore::from_store(store.clone()));
        let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
            Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
        Self::init_store_and_server(
            store,
            config,
            peer_keypair,
            user_identity,
            acp_store,
            zanzibar_store,
            node_identity_did,
            se_key,
        )
        .await
    }

    /// Create a new node
    #[doc(hidden)]
    pub async fn new(
        config: Config,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
    ) -> Result<Self> {
        info!("Initializing DefraDB node");
        info!("Root directory: {}", config.rootdir.display());
        info!("Data directory: {}", config.data_path().display());

        // Initialize peer keypair from keyring (if P2P enabled and keyring not disabled)
        let (peer_keypair, node_identity_did) =
            if !config.net.p2p_disabled && !config.keyring.disabled {
                let (kp, did) = Self::init_peer_key(&config)?;
                (Some(kp), Some(did))
            } else if !config.net.p2p_disabled {
                info!("Keyring disabled, using ephemeral peer identity");
                (None, None)
            } else {
                (None, None)
            };

        // Load the cluster-shared searchable-encryption key from the keyring
        // (Go's getOrCreateSearchableEncryptionKey). `None` when SE or the
        // keyring is disabled. Fed into the P2P broadcast/merge SE path so a
        // `defra start` node produces and verifies SE artifacts.
        let se_key = Self::load_or_create_searchable_encryption_key(&config)?;
        if se_key.is_some() {
            info!("Searchable encryption key loaded from keyring");
        }

        // Initialize storage, database, and set up P2P and HTTP server
        let (p2p_handle, p2p_tasks, downsample_task, txn_cleanup_task, http_server, pg_server) =
            match config.datastore.store {
                DatastoreType::Memory => {
                    info!("Using in-memory datastore");
                    // Use in-memory ACP store for memory datastore
                    let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
                    let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                        Arc::new(acp::MemoryZanzibarStore::new());
                    info!("Using in-memory ACP store");
                    let backend = storage::MemoryStore::new();
                    if config.datastore.at_rest_encryption {
                        info!("At-rest encryption enabled (value-only, AES-256-GCM)");
                        let key = Self::load_or_create_encryption_key(&config)?;
                        let store =
                            Arc::new(storage::encrypted_store::EncryptedStore::new(backend, key));
                        Self::init_store_and_server(
                            store,
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            acp_store,
                            zanzibar_store,
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    } else {
                        Self::init_store_and_server(
                            Arc::new(backend),
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            acp_store,
                            zanzibar_store,
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    }
                }
                #[cfg(feature = "redb")]
                DatastoreType::Redb => {
                    info!("Using Redb datastore at {}", config.data_path().display());
                    let opts = RedbStoreOptions::new()
                        .with_durability(config.datastore.durability)
                        .with_cache_size(256 * 1024 * 1024);
                    let backend = storage::RedbStore::open_with_options(config.data_path(), opts)?;
                    if config.datastore.at_rest_encryption {
                        info!("At-rest encryption enabled (value-only, AES-256-GCM)");
                        let key = Self::load_or_create_encryption_key(&config)?;
                        let store =
                            Arc::new(storage::encrypted_store::EncryptedStore::new(backend, key));
                        Self::init_persistent_store_and_server(
                            store,
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    } else {
                        Self::init_persistent_store_and_server(
                            Arc::new(backend),
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    }
                }
                #[cfg(not(feature = "redb"))]
                DatastoreType::Redb => {
                    return Err(crate::error::Error::InvalidDatastore(
                        "redb backend not enabled. Rebuild with --features redb".into(),
                    ));
                }
                #[cfg(feature = "fjall")]
                DatastoreType::Fjall => {
                    info!("Using Fjall datastore at {}", config.data_path().display());
                    let opts =
                        FjallStoreOptions::new().with_durability(config.datastore.durability);
                    let backend = storage::FjallStore::open_with_options(config.data_path(), opts)?;
                    if config.datastore.at_rest_encryption {
                        info!("At-rest encryption enabled (value-only, AES-256-GCM)");
                        let key = Self::load_or_create_encryption_key(&config)?;
                        let store =
                            Arc::new(storage::encrypted_store::EncryptedStore::new(backend, key));
                        Self::init_persistent_store_and_server(
                            store,
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    } else {
                        Self::init_persistent_store_and_server(
                            Arc::new(backend),
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    }
                }
                #[cfg(not(feature = "fjall"))]
                DatastoreType::Fjall => {
                    return Err(crate::error::Error::InvalidDatastore(
                        "fjall backend not enabled. Rebuild with --features fjall".into(),
                    ));
                }
                #[cfg(feature = "rocksdb")]
                DatastoreType::RocksDb => {
                    info!(
                        "Using RocksDB datastore at {}",
                        config.data_path().display()
                    );
                    let opts = RocksDbStoreOptions::from_env()
                        .with_durability(config.datastore.durability);
                    let backend =
                        storage::RocksDbStore::open_with_options(config.data_path(), opts)?;
                    if config.datastore.at_rest_encryption {
                        info!("At-rest encryption enabled (value-only, AES-256-GCM)");
                        let key = Self::load_or_create_encryption_key(&config)?;
                        let store =
                            Arc::new(storage::encrypted_store::EncryptedStore::new(backend, key));
                        Self::init_persistent_store_and_server(
                            store,
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    } else {
                        Self::init_persistent_store_and_server(
                            Arc::new(backend),
                            &config,
                            peer_keypair,
                            user_identity.clone(),
                            node_identity_did.clone(),
                            se_key,
                        )
                        .await?
                    }
                }
                #[cfg(not(feature = "rocksdb"))]
                DatastoreType::RocksDb => {
                    return Err(crate::error::Error::InvalidDatastore(
                        "rocksdb backend not enabled. Rebuild with --features rocksdb".into(),
                    ));
                }
            };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            config,
            p2p_handle,
            p2p_tasks,
            downsample_task,
            txn_cleanup_task,
            http_server: Some(http_server),
            pg_server,
            shutdown_tx,
            shutdown_rx,
            user_identity,
        })
    }
}
