//! Node struct and constructor

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{Config, DatastoreType};
use crate::error::Result;

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
                let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
                let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                    Arc::new(acp::MemoryZanzibarStore::new());
                info!("Using in-memory ACP store");
                let backend = storage::RegolithStore::in_memory()?;
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
            DatastoreType::Regolith => {
                info!(
                    "Using regolith datastore at {}",
                    config.data_path().display()
                );
                let opts = storage::RegolithStoreOptions::default()
                    .with_durability(config.datastore.durability);
                let backend = storage::RegolithStore::open_with_options(config.data_path(), opts)?;
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

/// Options for the regolith store, with the value-log-file-size flag
/// applied when the operator set it.
///
/// Go wires `--valuelogfilesize` to Badger's `ValueLogFileSize`. regolith's
/// nearest equivalent is the compaction output target, so that is where it
/// lands.
fn regolith_options(config: &Config) -> storage::RegolithStoreOptions {
    let mut opts =
        storage::RegolithStoreOptions::default().with_durability(config.datastore.durability);
    if let Some(bytes) = config.datastore.valuelogfilesize {
        opts.engine.target_file_size = bytes;
    }
    opts
}

/// Message to log when the flag was set but cannot be honoured. Accepting
/// an option and silently doing nothing is the defect this exists to
/// close, so the no-op is announced rather than hidden.
fn valuelogfilesize_warning(store: DatastoreType, value: Option<u64>) -> Option<String> {
    match value {
        Some(bytes) if store == DatastoreType::Memory => Some(format!(
            "--valuelogfilesize={bytes} has no effect on an in-memory datastore \
             and is being ignored; it sets the on-disk compaction target"
        )),
        _ => None,
    }
}

#[cfg(test)]
mod valuelogfilesize_tests {
    use super::*;

    fn config_with(store: DatastoreType, size: Option<u64>) -> Config {
        let mut config = Config::default();
        config.datastore.store = store;
        config.datastore.valuelogfilesize = size;
        config
    }

    #[test]
    fn an_ignored_flag_is_reported_rather_than_silently_dropped() {
        assert!(
            valuelogfilesize_warning(DatastoreType::Memory, Some(1 << 20)).is_some(),
            "silently ignoring the flag reproduces the defect being fixed"
        );
    }

    #[test]
    fn no_warning_when_the_store_honours_it() {
        assert!(valuelogfilesize_warning(DatastoreType::Regolith, Some(1 << 20)).is_none());
    }

    /// The wiring the issue was about: the value must reach the engine's
    /// options, not merely land in the config struct.
    #[test]
    fn an_explicitly_set_value_reaches_the_engine() {
        let opts = regolith_options(&config_with(DatastoreType::Regolith, Some(4 * 1024 * 1024)));
        assert_eq!(opts.engine.target_file_size, 4 * 1024 * 1024);
    }

    /// An unset flag must leave the profile's own value alone. Asserted
    /// against that value rather than a constant, so this does not break
    /// when regolith retunes.
    #[test]
    fn an_unset_flag_keeps_the_profile_default() {
        let expected = storage::RegolithStoreOptions::default()
            .engine
            .target_file_size;
        let opts = regolith_options(&config_with(DatastoreType::Regolith, None));
        assert_eq!(opts.engine.target_file_size, expected);
    }
}
