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

        // Initialize storage, database, and set up P2P and HTTP server
        let (p2p_handle, p2p_tasks, downsample_task, http_server, pg_server) =
            match config.datastore.store {
                DatastoreType::Memory => {
                    info!("Using in-memory datastore");
                    let store = Arc::new(storage::MemoryStore::new());
                    // Use in-memory ACP store for memory datastore
                    let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
                    let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                        Arc::new(acp::MemoryZanzibarStore::new());
                    info!("Using in-memory ACP store");
                    Self::init_store_and_server(
                        store,
                        &config,
                        peer_keypair,
                        user_identity.clone(),
                        acp_store,
                        zanzibar_store,
                        node_identity_did.clone(),
                    )
                    .await?
                }
                #[cfg(feature = "redb")]
                DatastoreType::Redb => {
                    info!("Using Redb datastore at {}", config.data_path().display());
                    let opts = RedbStoreOptions::new()
                        .with_durability(config.datastore.durability)
                        .with_cache_size(256 * 1024 * 1024);
                    let store = Arc::new(storage::RedbStore::open_with_options(
                        config.data_path(),
                        opts,
                    )?);
                    // Use unified ACP store backed by main database with namespace isolation
                    info!("Using unified ACP store (namespace isolated in main database)");
                    let acp_store: Arc<dyn acp::AcpStore> =
                        Arc::new(acp::PersistentAcpStore::from_store(store.clone()));
                    let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                        Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
                    Self::init_store_and_server(
                        store,
                        &config,
                        peer_keypair,
                        user_identity.clone(),
                        acp_store,
                        zanzibar_store,
                        node_identity_did.clone(),
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
                    let opts =
                        FjallStoreOptions::new().with_durability(config.datastore.durability);
                    let store = Arc::new(storage::FjallStore::open_with_options(
                        config.data_path(),
                        opts,
                    )?);
                    info!("Using unified ACP store (namespace isolated in main database)");
                    let acp_store: Arc<dyn acp::AcpStore> =
                        Arc::new(acp::PersistentAcpStore::from_store(store.clone()));
                    let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                        Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
                    Self::init_store_and_server(
                        store,
                        &config,
                        peer_keypair,
                        user_identity.clone(),
                        acp_store,
                        zanzibar_store,
                        node_identity_did.clone(),
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
                    let opts = RocksDbStoreOptions::from_env()
                        .with_durability(config.datastore.durability);
                    let store = Arc::new(storage::RocksDbStore::open_with_options(
                        config.data_path(),
                        opts,
                    )?);
                    info!("Using unified ACP store (namespace isolated in main database)");
                    let acp_store: Arc<dyn acp::AcpStore> =
                        Arc::new(acp::PersistentAcpStore::from_store(store.clone()));
                    let zanzibar_store: Arc<dyn acp::ZanzibarStore> =
                        Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
                    Self::init_store_and_server(
                        store,
                        &config,
                        peer_keypair,
                        user_identity.clone(),
                        acp_store,
                        zanzibar_store,
                        node_identity_did.clone(),
                    )
                    .await?
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
            http_server: Some(http_server),
            pg_server,
            shutdown_tx,
            shutdown_rx,
            user_identity,
        })
    }
}
