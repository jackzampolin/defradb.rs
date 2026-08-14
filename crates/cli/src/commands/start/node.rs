//! Node struct and constructor

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{Config, DatastoreType};
use crate::error::Result;
#[cfg(feature = "fjall")]
use storage::backends::FjallStoreOptions;
#[cfg(feature = "lark")]
use storage::backends::LarkStoreOptions;
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

/// Servers and background tasks produced by store/server initialization.
pub(super) struct ServerSetup {
    pub p2p_handle: Option<p2p::P2PHostHandle>,
    pub p2p_tasks: Option<P2PTasks>,
    pub downsample_task: Option<JoinHandle<()>>,
    pub txn_cleanup_task: Option<JoinHandle<()>>,
    pub http_server: defra_http::Server,
    #[cfg(feature = "postgres")]
    pub pg_server: Option<pg_compat::PgServer>,
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
    #[cfg(feature = "postgres")]
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

    /// Wrap a concrete backend in a type-erased [`storage::DynStore`],
    /// applying at-rest encryption when configured. The erasure keeps the
    /// whole node stack at a single `S = DynStore` instantiation.
    pub(crate) fn wrap_store<S>(config: &Config, backend: S) -> Result<storage::DynStore>
    where
        S: storage::corekv::Store + 'static,
    {
        if config.datastore.at_rest_encryption {
            info!("At-rest encryption enabled (value-only, AES-256-GCM)");
            let key = Self::load_or_create_encryption_key(config)?;
            Ok(storage::DynStore::new(Arc::new(
                storage::encrypted_store::EncryptedStore::new(backend, key),
            )))
        } else {
            Ok(storage::DynStore::new(Arc::new(backend)))
        }
    }

    /// Build the persistent ACP/Zanzibar stores from a shared store and start
    /// the server. The store may wrap a bare backend or an [`EncryptedStore`];
    /// both DB and ACP share the same instance so they encrypt uniformly.
    async fn init_persistent_store_and_server(
        store: Arc<storage::DynStore>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
        node_identity_did: Option<String>,
        se_key: Option<[u8; 32]>,
    ) -> Result<ServerSetup> {
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

        if let Some(message) =
            valuelogfilesize_warning(config.datastore.store, config.datastore.valuelogfilesize)
        {
            warn!("{message}");
        }

        // Initialize storage, database, and set up P2P and HTTP server
        let servers = match config.datastore.store {
            DatastoreType::Memory => {
                info!("Using in-memory datastore");
                // Use in-memory ACP store for memory datastore
                let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
                let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                    Arc::new(acp::MemoryZanzibarStore::new());
                info!("Using in-memory ACP store");
                let backend = storage::MemoryStore::new();
                let store = Self::wrap_store(&config, backend)?;
                Self::init_store_and_server(
                    Arc::new(store),
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
            #[cfg(feature = "redb")]
            DatastoreType::Redb => {
                info!("Using Redb datastore at {}", config.data_path().display());
                let opts = RedbStoreOptions::new()
                    .with_durability(config.datastore.durability)
                    .with_cache_size(256 * 1024 * 1024);
                let backend = storage::RedbStore::open_with_options(config.data_path(), opts)?;
                let store = Self::wrap_store(&config, backend)?;
                Self::init_persistent_store_and_server(
                    Arc::new(store),
                    &config,
                    peer_keypair,
                    user_identity.clone(),
                    node_identity_did.clone(),
                    se_key,
                )
                .await?
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
                let opts = FjallStoreOptions::new().with_durability(config.datastore.durability);
                let backend = storage::FjallStore::open_with_options(config.data_path(), opts)?;
                let store = Self::wrap_store(&config, backend)?;
                Self::init_persistent_store_and_server(
                    Arc::new(store),
                    &config,
                    peer_keypair,
                    user_identity.clone(),
                    node_identity_did.clone(),
                    se_key,
                )
                .await?
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
                let opts = rocksdb_options(&config);
                let backend = storage::RocksDbStore::open_with_options(config.data_path(), opts)?;
                let store = Self::wrap_store(&config, backend)?;
                Self::init_persistent_store_and_server(
                    Arc::new(store),
                    &config,
                    peer_keypair,
                    user_identity.clone(),
                    node_identity_did.clone(),
                    se_key,
                )
                .await?
            }
            #[cfg(not(feature = "rocksdb"))]
            DatastoreType::RocksDb => {
                return Err(crate::error::Error::InvalidDatastore(
                    "rocksdb backend not enabled. Rebuild with --features rocksdb".into(),
                ));
            }
            #[cfg(feature = "lark")]
            DatastoreType::Lark => {
                info!("Using Lark datastore at {}", config.data_path().display());
                let opts = lark_options(&config);
                let backend = storage::LarkStore::open_with_options(config.data_path(), opts)?;
                let store = Self::wrap_store(&config, backend)?;
                Self::init_persistent_store_and_server(
                    Arc::new(store),
                    &config,
                    peer_keypair,
                    user_identity.clone(),
                    node_identity_did.clone(),
                    se_key,
                )
                .await?
            }
            #[cfg(not(feature = "lark"))]
            DatastoreType::Lark => {
                return Err(crate::error::Error::InvalidDatastore(
                    "lark backend not enabled. Rebuild with --features lark".into(),
                ));
            }
        };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            config,
            p2p_handle: servers.p2p_handle,
            p2p_tasks: servers.p2p_tasks,
            downsample_task: servers.downsample_task,
            txn_cleanup_task: servers.txn_cleanup_task,
            http_server: Some(servers.http_server),
            #[cfg(feature = "postgres")]
            pg_server: servers.pg_server,
            shutdown_tx,
            shutdown_rx,
            user_identity,
        })
    }
}

/// Whether a backend has a knob `--valuelogfilesize` can map onto.
///
/// Go wires the flag to Badger's `ValueLogFileSize` (`node/store_badger.go:34`),
/// and its config key is namespaced `datastore.badger.valuelogfilesize`. Rust
/// has no Badger backend, so the flag maps onto the nearest equivalent -- the
/// compaction output target file size -- on the two backends that have one.
fn supports_valuelogfilesize(store: DatastoreType) -> bool {
    matches!(store, DatastoreType::Lark | DatastoreType::RocksDb)
}

/// Build lark options from config.
///
/// `--valuelogfilesize` is applied after `from_env` so the CLI flag wins over
/// `LARK_TARGET_FILE_MB`, and only when explicitly set -- leaving it unset must
/// preserve lark's own 64 MiB default rather than imposing Go's Badger-derived
/// 1 GiB, which would be a 16x change for every operator who never set it.
#[cfg(feature = "lark")]
fn lark_options(config: &Config) -> LarkStoreOptions {
    lark_options_from(LarkStoreOptions::from_env(), config)
}

/// Apply config over an already-resolved base, so precedence is testable
/// without mutating process environment.
#[cfg(feature = "lark")]
fn lark_options_from(base: LarkStoreOptions, config: &Config) -> LarkStoreOptions {
    let mut opts = base.with_durability(config.datastore.durability);
    if let Some(bytes) = config.datastore.valuelogfilesize {
        opts = opts.with_target_file_size(bytes);
    }
    opts
}

/// Build rocksdb options from config. Same precedence rule as [`lark_options`],
/// against `ROCKS_TARGET_FILE_MB`.
#[cfg(feature = "rocksdb")]
fn rocksdb_options(config: &Config) -> RocksDbStoreOptions {
    rocksdb_options_from(RocksDbStoreOptions::from_env(), config)
}

/// See [`lark_options_from`].
#[cfg(feature = "rocksdb")]
fn rocksdb_options_from(base: RocksDbStoreOptions, config: &Config) -> RocksDbStoreOptions {
    let mut opts = base.with_durability(config.datastore.durability);
    if let Some(bytes) = config.datastore.valuelogfilesize {
        opts = opts.with_target_file_size_base(bytes);
    }
    opts
}

/// Message to log when the flag was set but the selected backend cannot honour
/// it. Accepting an option and silently doing nothing is the defect this fix
/// exists to close, so the no-op is announced rather than hidden.
fn valuelogfilesize_warning(store: DatastoreType, value: Option<u64>) -> Option<String> {
    match value {
        Some(bytes) if !supports_valuelogfilesize(store) => Some(format!(
            "--valuelogfilesize={bytes} has no effect on the {store:?} backend \
             and is being ignored; it applies to the lark and rocksdb backends only"
        )),
        _ => None,
    }
}

#[cfg(test)]
mod valuelogfilesize_tests {
    use super::*;

    #[test]
    fn an_ignored_flag_is_reported_rather_than_silently_dropped() {
        assert!(
            valuelogfilesize_warning(DatastoreType::Redb, Some(1 << 20)).is_some(),
            "silently ignoring the flag reproduces the defect being fixed"
        );
    }

    #[test]
    fn no_warning_when_the_backend_honours_it() {
        assert!(valuelogfilesize_warning(DatastoreType::Lark, Some(1 << 20)).is_none());
    }

    fn config_with(store: DatastoreType, size: Option<u64>) -> Config {
        let mut config = Config::default();
        config.datastore.store = store;
        config.datastore.valuelogfilesize = size;
        config
    }

    /// The wiring the issue was about: the value must actually reach the
    /// backend's options, not merely land in the config struct.
    #[cfg(feature = "lark")]
    #[test]
    fn lark_receives_an_explicitly_set_value() {
        let opts = lark_options(&config_with(DatastoreType::Lark, Some(4 * 1024 * 1024)));
        assert_eq!(opts.target_file_size(), 4 * 1024 * 1024);
    }

    /// An unset flag must leave whatever the environment resolved to alone --
    /// applying Go's 1 GiB here would be a 16x change for operators who never
    /// set it. Asserted against the resolved base rather than a hardcoded
    /// constant, so this does not break when the storage crate retunes.
    #[cfg(feature = "lark")]
    #[test]
    fn lark_keeps_the_resolved_default_when_unset() {
        let base = LarkStoreOptions::from_env();
        let expected = base.target_file_size();
        let opts = lark_options_from(base, &config_with(DatastoreType::Lark, None));
        assert_eq!(opts.target_file_size(), expected);
    }

    /// Settled precedence: CLI flag beats `LARK_TARGET_FILE_MB`. Expressed by
    /// handing in a base that already carries an env-derived value.
    #[cfg(feature = "lark")]
    #[test]
    fn lark_cli_flag_beats_the_env_var() {
        let from_env = LarkStoreOptions::new().with_target_file_size(8 * 1024 * 1024);
        let opts = lark_options_from(
            from_env,
            &config_with(DatastoreType::Lark, Some(4 * 1024 * 1024)),
        );
        assert_eq!(
            opts.target_file_size(),
            4 * 1024 * 1024,
            "the CLI flag must win over the environment"
        );
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_receives_an_explicitly_set_value() {
        let opts = rocksdb_options(&config_with(DatastoreType::RocksDb, Some(4 * 1024 * 1024)));
        assert_eq!(opts.target_file_size_base(), 4 * 1024 * 1024);
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_keeps_the_resolved_default_when_unset() {
        let base = RocksDbStoreOptions::from_env();
        let expected = base.target_file_size_base();
        let opts = rocksdb_options_from(base, &config_with(DatastoreType::RocksDb, None));
        assert_eq!(opts.target_file_size_base(), expected);
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn rocksdb_cli_flag_beats_the_env_var() {
        let from_env = RocksDbStoreOptions::new().with_target_file_size_base(8 * 1024 * 1024);
        let opts = rocksdb_options_from(
            from_env,
            &config_with(DatastoreType::RocksDb, Some(4 * 1024 * 1024)),
        );
        assert_eq!(opts.target_file_size_base(), 4 * 1024 * 1024);
    }

    #[test]
    fn no_warning_when_the_flag_was_never_set() {
        assert!(valuelogfilesize_warning(DatastoreType::Redb, None).is_none());
        assert!(valuelogfilesize_warning(DatastoreType::Memory, None).is_none());
    }
}
