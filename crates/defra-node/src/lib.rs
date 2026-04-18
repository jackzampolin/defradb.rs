//! Reusable embedded DefraDB node builder.
//!
//! Wraps defradb.rs library crates behind a clean builder API so that
//! downstream binaries can embed a DefraDB instance without duplicating
//! wiring code.
//!
//! P2P uses IROH (QUIC-native) transport for peer-to-peer replication.

mod benchmark_data_gen;
mod benchmark_queries;
mod benchmark_stats;
#[doc(hidden)]
pub mod benchmark_support;
pub mod coding_search;
pub mod config;
mod db_impls;
pub mod dense_search;
mod node_acp;
pub mod search_chunks;
pub mod version;

use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "p2p")]
use std::sync::Mutex;

#[cfg(feature = "p2p")]
use p2p::P2PTransport;

#[cfg(feature = "p2p")]
type WireDocumentAcpCallback = Box<dyn FnOnce(Arc<dyn acp::DocumentACP>, bool)>;

pub use coding_search::{
    CodingHybridSearchHit, CodingHybridSearchRequest, CodingHybridSearchResponse,
    CodingSearchTarget,
};
#[cfg(feature = "http")]
pub use config::HttpConfig;
#[cfg(feature = "p2p")]
pub use config::P2PConfig;
pub use config::{DocumentAcpConfig, SourceHubConfig};
pub use dense_search::{DenseHybridSearchHit, DenseHybridSearchRequest, DenseHybridSearchResponse};
pub use events::EventName;
pub use lens::{LensConfig, LensModule, TransformId};
pub use query::{QueryExecutor, QueryRequest, QueryResponse};
pub use schema::CollectionVersion;

/// Type-erased schema operations so we can store DB<S> without leaking the Store generic.
#[async_trait::async_trait]
trait SchemaOps: Send + Sync {
    async fn add_schema(&self, sdl: &str) -> anyhow::Result<()>;
    async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()>;
    async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> anyhow::Result<CollectionVersion>;
    async fn set_active_collection_version(&self, version_id: &str) -> anyhow::Result<()>;
    async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId>;
    fn list_collections(&self) -> anyhow::Result<Vec<String>>;
    fn get_collection(&self, name: &str) -> anyhow::Result<Option<CollectionVersion>>;
    async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> anyhow::Result<Option<CollectionVersion>>;
    async fn get_all_collection_versions(&self) -> anyhow::Result<Vec<CollectionVersion>>;
}

/// An embedded DefraDB node with query execution and event subscription.
pub struct EmbeddedNode {
    runner: Arc<dyn QueryExecutor>,
    event_bus: Arc<dyn events::Bus>,
    schema_ops: Arc<dyn SchemaOps>,
    embedding_config: db::EmbeddingClientConfig,
    #[cfg(feature = "p2p")]
    p2p_ops: Option<Arc<dyn defra_http::P2POperations>>,
    #[cfg(feature = "p2p")]
    p2p_lifecycle: Option<P2PLifecycle>,
}

#[cfg(feature = "p2p")]
struct P2PLifecycle {
    inner: Mutex<Option<P2PLifecycleInner>>,
}

#[cfg(feature = "p2p")]
struct P2PLifecycleInner {
    transport: p2p::iroh::IrohTransport,
    endpoint_task: tokio::task::JoinHandle<()>,
    replication_task: tokio::task::JoinHandle<()>,
    event_handler_task: tokio::task::JoinHandle<()>,
    failure_recorder_task: tokio::task::JoinHandle<()>,
    retry_loop_task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "p2p")]
impl P2PLifecycle {
    fn new(inner: P2PLifecycleInner) -> Self {
        Self {
            inner: Mutex::new(Some(inner)),
        }
    }

    async fn shutdown(&self) {
        let inner = match self.inner.lock() {
            Ok(mut guard) => guard.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };

        if let Some(inner) = inner {
            inner.shutdown().await;
        }
    }
}

#[cfg(feature = "p2p")]
impl P2PLifecycleInner {
    async fn shutdown(self) {
        abort_background_task("iroh retry loop", self.retry_loop_task).await;

        if let Err(error) = self.transport.shutdown().await {
            tracing::debug!(%error, "Iroh transport shutdown returned an error");
        }

        await_endpoint_task(self.endpoint_task).await;
        abort_background_task("iroh event handler", self.event_handler_task).await;
        abort_background_task("iroh replication loop", self.replication_task).await;
        abort_background_task("iroh failure recorder", self.failure_recorder_task).await;
    }
}

#[cfg(feature = "p2p")]
async fn await_endpoint_task(mut task: tokio::task::JoinHandle<()>) {
    match tokio::time::timeout(std::time::Duration::from_secs(5), &mut task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {
            tracing::debug!("Iroh endpoint task was already cancelled");
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "Iroh endpoint task failed during shutdown");
        }
        Err(_) => {
            tracing::warn!("Iroh endpoint task did not stop after graceful shutdown; aborting");
            task.abort();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
        }
    }
}

#[cfg(feature = "p2p")]
async fn abort_background_task(task_name: &'static str, task: tokio::task::JoinHandle<()>) {
    task.abort();
    match tokio::time::timeout(std::time::Duration::from_secs(1), task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => {
            tracing::debug!(task = task_name, %error, "P2P background task failed during shutdown");
        }
        Err(_) => {
            tracing::debug!(
                task = task_name,
                "P2P background task did not stop after abort"
            );
        }
    }
}

impl EmbeddedNode {
    /// Start building a new embedded node.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    /// Execute a GraphQL query or mutation.
    pub async fn execute(&self, query_str: &str) -> QueryResponse {
        self.runner.execute(QueryRequest::new(query_str)).await
    }

    /// Add a schema from a GraphQL SDL type definition.
    pub async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        self.schema_ops.add_schema(sdl).await
    }

    /// Create a materialized view from a source query and target SDL.
    ///
    /// `source_query` format: `"SourceType { field1 field2 ... }"`
    /// `target_sdl` is the SDL for the view collection (may include directives
    /// like `@downsample` that are forward-declared for future defradb.rs support).
    pub async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()> {
        self.schema_ops.add_view(source_query, target_sdl).await
    }

    /// Apply a JSON Patch (RFC 6902) to an existing collection's schema.
    ///
    /// Returns the updated [`CollectionVersion`] (with a new `version_id`). The
    /// prior version is deactivated and the patched version is activated, unless
    /// the patch is a metadata-only or in-place change (see [`db::DB::patch_collection`]).
    ///
    /// `collection_name` may be a collection name, version ID, or variant; the
    /// underlying implementation falls back to version-ID lookup if name lookup fails.
    pub async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> anyhow::Result<CollectionVersion> {
        self.schema_ops
            .patch_collection(collection_name, patch)
            .await
    }

    /// Activate a specific collection version by its `version_id`.
    ///
    /// Deactivates sibling versions of the same collection and updates the
    /// collection-name pointer to resolve to this version. If migrations are
    /// registered, documents are reindexed through them.
    pub async fn set_active_collection_version(&self, version_id: &str) -> anyhow::Result<()> {
        self.schema_ops
            .set_active_collection_version(version_id)
            .await
    }

    /// Register a Lens migration between two collection versions.
    ///
    /// Returns the content-addressed [`TransformId`] of the stored transform.
    /// Placeholder versions are created if the source or destination are not
    /// yet materialized, allowing migrations to be registered ahead of patches.
    pub async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId> {
        self.schema_ops.set_migration(config).await
    }

    /// List the names of every active collection known to the node.
    ///
    /// Useful for idempotent schema-bootstrap flows that need to decide whether
    /// to call [`Self::add_schema`] (create) or [`Self::patch_collection`] (evolve).
    pub fn list_collections(&self) -> anyhow::Result<Vec<String>> {
        self.schema_ops.list_collections()
    }

    /// Fetch the active schema definition for a collection by name.
    ///
    /// Returns `Ok(None)` if no active collection with that name exists.
    pub fn get_collection(&self, name: &str) -> anyhow::Result<Option<CollectionVersion>> {
        self.schema_ops.get_collection(name)
    }

    /// Fetch a collection schema by its version ID, including inactive versions.
    ///
    /// Searches both the in-memory cache (active versions) and the underlying
    /// systemstore (all stored versions), so callers can inspect the history
    /// of a patched collection.
    pub async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> anyhow::Result<Option<CollectionVersion>> {
        self.schema_ops
            .get_collection_by_version_id(version_id)
            .await
    }

    /// Return every collection version known to the node, active and inactive.
    pub async fn get_all_collection_versions(&self) -> anyhow::Result<Vec<CollectionVersion>> {
        self.schema_ops.get_all_collection_versions().await
    }

    /// Subscribe to DefraDB events.
    pub fn subscribe(&self, event_names: &[EventName]) -> events::Subscription {
        self.event_bus.subscribe(event_names)
    }

    /// Access the underlying query executor for advanced use.
    pub fn runner(&self) -> &Arc<dyn QueryExecutor> {
        &self.runner
    }

    /// Access the event bus directly.
    pub fn event_bus(&self) -> &Arc<dyn events::Bus> {
        &self.event_bus
    }

    /// Access the resolved node-level embedding runtime config.
    pub fn embedding_config(&self) -> &db::EmbeddingClientConfig {
        &self.embedding_config
    }

    /// Access P2P operations (if P2P is enabled and configured).
    #[cfg(feature = "p2p")]
    pub fn p2p(&self) -> Option<&dyn defra_http::P2POperations> {
        self.p2p_ops.as_deref()
    }

    /// Cloneable P2P operations handle for background tasks.
    #[cfg(feature = "p2p")]
    pub fn p2p_arc(&self) -> Option<Arc<dyn defra_http::P2POperations>> {
        self.p2p_ops.as_ref().map(Arc::clone)
    }

    /// Gracefully stop background services owned by this embedded node.
    pub async fn shutdown(&self) {
        #[cfg(feature = "p2p")]
        if let Some(lifecycle) = &self.p2p_lifecycle {
            lifecycle.shutdown().await;
        }
    }
}

/// Selects which persistent storage backend to use when `data_path` is set.
///
/// Defaults to `Redb` for backwards compatibility.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub enum StorageBackend {
    /// Pure-Rust embedded database. Loads the full dataset into memory on open,
    /// which can be slow for very large stores (10 GB+).
    #[default]
    Redb,
    /// RocksDB LSM-tree backend. Constant-time open regardless of dataset size,
    /// but requires the `rocksdb` feature to be enabled at compile time.
    RocksDb,
}

/// Builder for constructing an `EmbeddedNode`.
#[derive(Default)]
pub struct NodeBuilder {
    data_path: Option<PathBuf>,
    storage_backend: StorageBackend,
    embedding_url: Option<String>,
    embedding_model: Option<String>,
    embedding_api_key: Option<String>,
    document_acp: DocumentAcpConfig,
    #[cfg(feature = "http")]
    http_config: Option<HttpConfig>,
    #[cfg(feature = "p2p")]
    p2p_config: Option<P2PConfig>,
}

impl NodeBuilder {
    /// Set the data directory for persistent storage.
    /// If not set, uses in-memory storage.
    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    /// Select the persistent storage backend (default: `Redb`).
    ///
    /// Has no effect when `data_path` is not set (in-memory mode).
    /// Using `StorageBackend::RocksDb` requires the `rocksdb` feature.
    pub fn with_storage_backend(mut self, backend: StorageBackend) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Set the fallback OpenAI-compatible embedding base URL.
    pub fn with_embedding_url(mut self, url: impl Into<String>) -> Self {
        self.embedding_url = Some(url.into());
        self
    }

    /// Set the fallback embedding model name used when the schema leaves it empty.
    pub fn with_embedding_model(mut self, model: impl Into<String>) -> Self {
        self.embedding_model = Some(model.into());
        self
    }

    /// Set the resolved embedding API key value used for Authorization headers.
    pub fn with_embedding_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.embedding_api_key = Some(api_key.into());
        self
    }

    /// Configure the node to use SourceHub-backed document ACP.
    pub fn with_sourcehub(mut self, config: SourceHubConfig) -> Self {
        self.document_acp = DocumentAcpConfig::SourceHub(config);
        self
    }

    /// Enable the HTTP GraphQL server.
    #[cfg(feature = "http")]
    pub fn with_http(mut self, config: HttpConfig) -> Self {
        self.http_config = Some(config);
        self
    }

    /// Enable P2P networking for replication.
    #[cfg(feature = "p2p")]
    pub fn with_p2p(mut self, config: P2PConfig) -> Self {
        self.p2p_config = Some(config);
        self
    }

    /// Build and start the embedded DefraDB node.
    pub async fn build(self) -> anyhow::Result<EmbeddedNode> {
        // 1. Event bus
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        let db_options = {
            let mut options = db::DbOptions::default();
            if let Some(url) = self.embedding_url.as_ref() {
                options = options.with_embedding_url(url.clone());
            }
            if let Some(model) = self.embedding_model.as_ref() {
                options = options.with_embedding_model(model.clone());
            }
            if let Some(api_key) = self.embedding_api_key.as_ref() {
                options = options.with_embedding_api_key(api_key.clone());
            }
            options
        };

        // 2. Extract configs before moving self
        #[cfg(feature = "http")]
        let http_config = self.http_config;
        #[cfg(feature = "p2p")]
        let p2p_config = self.p2p_config;

        // 3. Storage backend + database
        let node = if let Some(path) = self.data_path {
            tokio::fs::create_dir_all(&path).await?;

            match self.storage_backend {
                StorageBackend::Redb => {
                    tracing::info!(
                        storage_backend = "redb",
                        data_path = %path.display(),
                        "embedded node starting"
                    );
                    let store = Arc::new(
                        storage::RedbStore::open(path.to_str().ok_or_else(|| {
                            anyhow::anyhow!("data_path contains non-UTF8 characters")
                        })?)
                        .map_err(|e| anyhow::anyhow!("failed to open redb store: {}", e))?,
                    );

                    let acp_store: Arc<dyn acp::AcpStore> =
                        Arc::new(acp::PersistentAcpStore::from_store(store.clone()));

                    Self::build_with_store(
                        store,
                        acp_store,
                        self.document_acp.clone(),
                        db_options.clone(),
                        event_bus,
                        #[cfg(feature = "p2p")]
                        p2p_config,
                    )
                    .await?
                }
                #[cfg(feature = "rocksdb")]
                StorageBackend::RocksDb => {
                    tracing::info!(
                        storage_backend = "rocksdb",
                        data_path = %path.display(),
                        "embedded node starting"
                    );
                    let store = Arc::new(
                        storage::RocksDbStore::open(&path)
                            .map_err(|e| anyhow::anyhow!("failed to open rocksdb store: {}", e))?,
                    );

                    let acp_store: Arc<dyn acp::AcpStore> =
                        Arc::new(acp::PersistentAcpStore::from_store(store.clone()));

                    Self::build_with_store(
                        store,
                        acp_store,
                        self.document_acp.clone(),
                        db_options.clone(),
                        event_bus,
                        #[cfg(feature = "p2p")]
                        p2p_config,
                    )
                    .await?
                }
                #[cfg(not(feature = "rocksdb"))]
                StorageBackend::RocksDb => {
                    return Err(anyhow::anyhow!(
                        "RocksDB backend requested but the `rocksdb` feature is not enabled. \
                         Rebuild with `--features rocksdb`."
                    ));
                }
            }
        } else {
            tracing::info!(
                storage_backend = "memory",
                "embedded node starting (ephemeral, no data_path)"
            );
            let store = Arc::new(storage::MemoryStore::new());
            let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());

            Self::build_with_store(
                store,
                acp_store,
                self.document_acp,
                db_options,
                event_bus,
                #[cfg(feature = "p2p")]
                p2p_config,
            )
            .await?
        };

        // 4. Spawn HTTP server if configured
        #[cfg(feature = "http")]
        if let Some(http_cfg) = http_config {
            let server_config = defra_http::ServerConfig {
                address: http_cfg.address,
                ..Default::default()
            };
            let server =
                defra_http::Server::from_arc_with_config(node.runner.clone(), server_config)
                    .with_event_bus_arc(node.event_bus.clone());

            #[cfg(feature = "p2p")]
            let server = if let Some(p2p) = node.p2p_ops.as_ref() {
                server.with_p2p_arc(Arc::clone(p2p))
            } else {
                server
            };

            let addr = http_cfg.address;
            let extra_routes = http_cfg.extra_routes;
            tokio::spawn(async move {
                let router_result = server.router();
                let run_result = async {
                    let mut router = router_result?;
                    if let Some(extra) = extra_routes {
                        router = router.merge(extra);
                    }

                    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
                        let hint = match e.kind() {
                            std::io::ErrorKind::AddrInUse => "port is already in use",
                            std::io::ErrorKind::PermissionDenied => {
                                "permission denied (try port > 1024)"
                            }
                            std::io::ErrorKind::AddrNotAvailable => {
                                "address not available on this host"
                            }
                            _ => "check network configuration",
                        };
                        anyhow::anyhow!("failed to bind to {}: {} ({})", addr, e, hint)
                    })?;

                    axum::serve(listener, router)
                        .await
                        .map_err(|e| anyhow::anyhow!("server error: {}", e))?;
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                if let Err(e) = run_result {
                    tracing::error!(error = %e, address = %addr, "HTTP server exited with error");
                }
            });
            tracing::info!(address = %addr, "HTTP server started");
        }

        Ok(node)
    }

    async fn build_with_store<S: storage::corekv::Store + 'static>(
        store: Arc<S>,
        acp_store: Arc<dyn acp::AcpStore>,
        document_acp_config: DocumentAcpConfig,
        db_options: db::DbOptions,
        event_bus: Arc<dyn events::Bus>,
        #[cfg(feature = "p2p")] p2p_config: Option<P2PConfig>,
    ) -> anyhow::Result<EmbeddedNode> {
        let embedding_config = db_options.embedding_config();

        // Open database
        let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
            .await
            .map_err(|e| anyhow::anyhow!("failed to open database: {}", e))?;

        // Wire event bus so mutations publish events
        database.set_event_bus(event_bus.clone());
        let database = Arc::new(database);

        // P2P setup (affects mutator choice)
        #[cfg(feature = "p2p")]
        let mut p2p_result = if let Some(p2p_cfg) = p2p_config {
            Some(
                Self::setup_p2p(store.clone(), database.clone(), event_bus.clone(), &p2p_cfg)
                    .await?,
            )
        } else {
            None
        };

        // Choose mutator: BroadcastMutator if P2P, AutoCommitMutator otherwise
        #[cfg(feature = "p2p")]
        let mutator: Arc<dyn query::DocMutator> = if let Some(ref p2p) = p2p_result {
            p2p.mutator.clone()
        } else {
            Arc::new(db::AutoCommitMutator::new(database.clone()))
        };
        #[cfg(not(feature = "p2p"))]
        let mutator: Arc<dyn query::DocMutator> =
            Arc::new(db::AutoCommitMutator::new(database.clone()));

        // Query runner components
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
        let provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());
        let registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));
        let (document_acp, _strict_replicated_doc_access) =
            node_acp::create_document_acp(acp_store, &document_acp_config).await?;

        #[cfg(feature = "p2p")]
        if let Some(wire_document_acp) = p2p_result
            .as_mut()
            .and_then(|result| result.wire_document_acp.take())
        {
            wire_document_acp(document_acp.clone(), _strict_replicated_doc_access);
        }

        // Assemble query runner
        let query_runner =
            query::QueryRunner::with_arc_registry_and_provider(fetcher, provider, registry)
                .with_mutator(mutator)
                .with_acp(document_acp)
                .with_lens_store(database.lens_store().clone());

        let runner: Arc<dyn QueryExecutor> = Arc::new(query_runner);
        let schema_ops: Arc<dyn SchemaOps> = Arc::new(database.clone());

        #[cfg(feature = "p2p")]
        let (p2p_ops, p2p_lifecycle) = match p2p_result {
            Some(result) => (Some(result.ops), result.lifecycle),
            None => (None, None),
        };

        Ok(EmbeddedNode {
            runner,
            event_bus,
            schema_ops,
            embedding_config,
            #[cfg(feature = "p2p")]
            p2p_ops,
            #[cfg(feature = "p2p")]
            p2p_lifecycle,
        })
    }

    #[cfg(feature = "p2p")]
    async fn setup_p2p<S: storage::corekv::Store + 'static>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        event_bus: Arc<dyn events::Bus>,
        config: &P2PConfig,
    ) -> anyhow::Result<P2PSetupResult> {
        // 1. Load or generate secret key for stable node identity
        let secret_key =
            p2p::iroh::load_or_generate_secret_key(config.secret_key_path.as_deref()).await?;

        // 2. Configure and spawn IROH endpoint with pinned port + optional bind address
        let iroh_config = p2p::iroh::IrohEndpointConfig {
            secret_key: secret_key.clone(),
            relay_mode: config.relay_mode.clone(),
            discovery: config.discovery.clone(),
            bind_port: Some(config.port),
            bind_addr: config.bind_addr,
        };
        let (command_tx, iroh_events, replicator_registry, endpoint_task) =
            p2p::iroh::spawn_endpoint(iroh_config)
                .await
                .map_err(|e| anyhow::anyhow!("IROH endpoint spawn failed: {}", e))?;

        // 3. Create IROH transport facade
        let transport = p2p::iroh::IrohTransport::new(command_tx, secret_key);

        // 5. Blockstore for sync coordinator + merge handler
        let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));

        // 6. Collection store (persists subscriptions)
        let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
            Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));

        // 7. SyncCoordinator (transport-generic -- same constructor, different type param)
        let sync_config = p2p::sync::SyncConfig {
            max_concurrent_dag_fetches: config.max_concurrent_dag_fetches,
            max_concurrent_push_tasks: config.max_concurrent_push_tasks,
            rate_limit_burst: config.rate_limit_burst,
            rate_limit_rate: config.rate_limit_rate,
            ..Default::default()
        };
        let (mut coordinator, sync_events) = p2p::sync::SyncCoordinator::with_access_control(
            transport.clone(),
            sync_blockstore.clone(),
            sync_config,
            p2p::AccessMode::Controlled,
            replicator_registry,
            collection_store,
        )
        .await
        .map_err(|e| anyhow::anyhow!("SyncCoordinator creation failed: {}", e))?;

        // Failure channel (required by replication loop)
        let failure_rx = db_merge::attach_failure_channel(&mut coordinator, 1024);
        let failure_recorder_task = spawn_failure_recorder(store.clone(), failure_rx);

        let coordinator = Arc::new(coordinator);

        if config.load_persisted_collections {
            db_merge::load_persisted_collections(&coordinator)
                .await
                .ok();
        } else {
            tracing::info!("skipping persisted P2P collection subscriptions");
        }

        // 8. Merge handler
        let replication = db_merge::create_replication_stack(
            database.clone(),
            sync_blockstore.clone(),
            coordinator.clone(),
        );
        let merge_handler_for_loop = replication.merge_handler.clone();
        let broadcast_mutator = replication.broadcast_mutator.clone();
        let merge_handler_for_acp = replication.merge_handler.clone();

        // 9. Replication loop (transport-generic)
        let coord_for_repl = coordinator.clone();
        let replication_task = tokio::spawn(async move {
            p2p::sync::ReplicationLoop::run(
                coord_for_repl,
                sync_events,
                merge_handler_for_loop,
                p2p::sync::ReplicationConfig::default(),
            )
            .await;
        });

        // 10. IROH event handler (events are already TransportEvent -- no conversion needed)
        let coord_for_events = coordinator.clone();
        let event_handler_task = tokio::spawn(async move {
            Self::run_event_handler(iroh_events, coord_for_events).await;
        });
        let retry_loop_task =
            spawn_iroh_retry_loop(store.clone(), database.clone(), transport.clone());

        let doc_pusher_impl = Arc::new(defra_p2p_adapter::DbTransportDocPusher::new(
            database.clone(),
            transport.clone(),
        ));
        let doc_pusher_for_acp = doc_pusher_impl.clone();
        let doc_pusher: Arc<dyn defra_p2p_adapter::TransportDocPusher> = doc_pusher_impl;
        let version_syncer = Some(defra_p2p_adapter::DbTransportVersionSyncer::new_arc(
            sync_blockstore,
            replication.merge_handler_inner.clone(),
            database.clone(),
            transport.clone(),
        ));

        // 11. BroadcastMutator (replaces AutoCommitMutator)
        let broadcast_mutator_for_acp = broadcast_mutator.clone();
        let mutator: Arc<dyn query::DocMutator> = broadcast_mutator;

        let peer_id = transport.local_peer_id().to_string();
        tracing::info!(peer_id = %peer_id, "P2P started (IROH/QUIC)");
        let ops: Arc<dyn defra_http::P2POperations> =
            Arc::new(defra_p2p_adapter::IrohP2PAdapter::with_full_context(
                transport.clone(),
                coordinator,
                doc_pusher,
                event_bus,
                version_syncer,
            ));

        Ok(P2PSetupResult {
            ops,
            lifecycle: Some(P2PLifecycle::new(P2PLifecycleInner {
                transport,
                endpoint_task,
                replication_task,
                event_handler_task,
                failure_recorder_task,
                retry_loop_task,
            })),
            mutator,
            wire_document_acp: Some(Box::new(move |acp, strict| {
                merge_handler_for_acp.set_document_acp(acp.clone());
                merge_handler_for_acp.set_strict_replicated_doc_access(strict);
                doc_pusher_for_acp.set_document_acp(acp.clone());
                broadcast_mutator_for_acp.set_document_acp(acp);
            })),
        })
    }

    #[cfg(feature = "p2p")]
    async fn run_event_handler<B: blockstore::Blockstore + Send + Sync + 'static>(
        mut events: tokio::sync::mpsc::Receiver<
            p2p::TransportEvent<<p2p::iroh::IrohTransport as P2PTransport>::ResponseToken>,
        >,
        coordinator: Arc<p2p::sync::SyncCoordinator<B, p2p::iroh::IrohTransport>>,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        while let Some(event) = events.recv().await {
            if event.requires_inline_ordering() {
                if let Err(e) = coordinator.handle_transport_event(event).await {
                    if e.is_rate_limited() {
                        tracing::debug!(error = %e, "P2P rate-limited");
                    } else if e.is_retriable() {
                        tracing::warn!(error = %e, "P2P transport event failed after retries");
                    } else {
                        tracing::error!(error = %e, "P2P event handler error");
                    }
                }
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coord = coordinator.clone();
            tokio::spawn(async move {
                if let Err(e) = coord.handle_transport_event(event).await {
                    if e.is_rate_limited() {
                        tracing::debug!(error = %e, "P2P rate-limited");
                    } else if e.is_retriable() {
                        tracing::warn!(error = %e, "P2P transport event failed after retries");
                    } else {
                        tracing::error!(error = %e, "P2P event handler error");
                    }
                }
                drop(permit);
            });
        }
    }
}

#[cfg(feature = "p2p")]
fn spawn_failure_recorder<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    mut failure_rx: tokio::sync::mpsc::Receiver<p2p::sync::PushFailure>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(failure) = failure_rx.recv().await {
            tracing::warn!(
                peer_id = %failure.peer_id,
                doc_id = %failure.doc_id,
                collection_id = %failure.collection_id,
                "P2P push to replicator failed"
            );

            let peerstore = storage::stores::Peerstore::new(store.clone());
            let retry_info = storage::stores::RetryInfo::new_initial();
            let info_bytes = match retry_info.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::warn!(error = %error, "failed to serialize retry info");
                    continue;
                }
            };

            if let Err(error) = peerstore
                .record_push_failure(
                    &failure.peer_id.to_string(),
                    &failure.doc_id,
                    &failure.collection_id,
                    &info_bytes,
                )
                .await
            {
                tracing::warn!(error = %error, "failed to record push failure");
            }
        }
    })
}

#[cfg(feature = "p2p")]
fn spawn_iroh_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    transport: p2p::iroh::IrohTransport,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let peers = match peerstore.get_all_retry_peers().await {
                Ok(peers) => peers,
                Err(error) => {
                    tracing::debug!(error = %error, "failed to load retry peers");
                    continue;
                }
            };

            for (peer_id_str, info_bytes) in peers {
                let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                    Ok(info) => info,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                        continue;
                    }
                };
                if !retry_info.is_due() {
                    continue;
                }

                let peer_id = p2p::transport::PeerId::new(peer_id_str.clone());
                // Iroh request-response can reconnect on demand, so don't
                // suppress retries based on the peer-map's current
                // connected_peers snapshot.

                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                    Ok(docs) => docs,
                    Err(error) => {
                        tracing::debug!(peer_id = %peer_id_str, error = %error, "failed to load retry docs");
                        continue;
                    }
                };
                if docs.is_empty() {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                    continue;
                }

                let mut all_succeeded = true;
                for (doc_id, collection_id) in &docs {
                    match db_merge::retry_doc_via_transport(
                        &transport,
                        database.as_ref(),
                        None,
                        &peer_id,
                        doc_id,
                        collection_id,
                    )
                    .await
                    {
                        Ok(()) => {
                            let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                        }
                        Err(error) => {
                            tracing::warn!(
                                doc_id = %doc_id,
                                peer_id = %peer_id,
                                error = %error,
                                "retry push failed"
                            );
                            all_succeeded = false;
                        }
                    }
                }

                if all_succeeded {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                } else {
                    retry_info.bump();
                    if let Ok(bytes) = retry_info.to_bytes() {
                        let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                    }
                }
            }
        }
    })
}

/// Internal result from P2P setup, carrying the type-erased ops and mutator.
#[cfg(feature = "p2p")]
struct P2PSetupResult {
    ops: Arc<dyn defra_http::P2POperations>,
    lifecycle: Option<P2PLifecycle>,
    mutator: Arc<dyn query::DocMutator>,
    wire_document_acp: Option<WireDocumentAcpCallback>,
}

#[cfg(all(test, feature = "http"))]
mod tests {
    use axum::{routing::get, Router};

    use super::HttpConfig;

    #[test]
    fn http_config_accepts_extra_routes() {
        let config = HttpConfig::new(9182)
            .with_extra_routes(Router::new().route("/healthz", get(|| async { "ok" })));

        assert_eq!(config.address.port(), 9182);
        assert!(config.extra_routes.is_some());
    }
}

#[cfg(all(test, feature = "p2p"))]
mod p2p_tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Once;
    use std::time::{Duration, Instant};

    use serde_json::Value as JsonValue;

    use super::{EmbeddedNode, P2PConfig};

    fn init_tracing() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let filter = tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive(
                    "iroh_quinn_proto::connection=error"
                        .parse()
                        .expect("valid tracing directive"),
                )
                .add_directive(
                    "noq_proto::connection=error"
                        .parse()
                        .expect("valid tracing directive"),
                );
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_test_writer()
                .try_init();
        });
    }

    fn test_p2p_config() -> P2PConfig {
        P2PConfig {
            port: 0,
            bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
            discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
            secret_key_path: None,
            load_persisted_collections: false,
            max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
            max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
            rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
        }
    }

    async fn wait_for_listen_addr(node: &EmbeddedNode) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let addrs = node
                .p2p()
                .expect("P2P should be enabled")
                .listen_addresses()
                .await;
            if let Some(addr) = addrs.first() {
                return addr.clone();
            }
            assert!(
                Instant::now() < deadline,
                "node never exposed a P2P listen address"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_for_connected_peer(node: &EmbeddedNode) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let peers = node
                .p2p()
                .expect("P2P should be enabled")
                .connected_peers()
                .await
                .expect("connected_peers should succeed");
            if !peers.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node never reported a connected peer"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn collection_len(data: &JsonValue, collection: &str) -> usize {
        data.get(collection)
            .and_then(|v| v.as_array())
            .map(|docs| docs.len())
            .unwrap_or(0)
    }

    async fn wait_for_collection_len(node: &EmbeddedNode, collection: &str, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let response = node
                .execute(&format!("query {{ {collection} {{ _docID name age }} }}"))
                .await;
            assert!(
                response.errors.is_empty(),
                "query returned errors: {:?}",
                response.errors
            );

            let len = response
                .data
                .as_ref()
                .map(|data| collection_len(data, collection))
                .unwrap_or(0);
            if len >= expected {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "collection {collection} never reached {expected} docs; last response: {:?}",
                response.data
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    #[tokio::test]
    async fn live_replicator_pushes_post_config_writes() {
        init_tracing();

        let node0 = EmbeddedNode::builder()
            .with_p2p(test_p2p_config())
            .build()
            .await
            .expect("build node0");
        let node1 = EmbeddedNode::builder()
            .with_p2p(test_p2p_config())
            .build()
            .await
            .expect("build node1");

        node0
            .add_schema("type User { name: String age: Int }")
            .await
            .expect("schema on node0");
        node1
            .add_schema("type User { name: String age: Int }")
            .await
            .expect("schema on node1");

        let addr1 = wait_for_listen_addr(&node1).await;

        let p2p0 = node0.p2p().expect("node0 p2p");
        let p2p1 = node1.p2p().expect("node1 p2p");

        p2p0.connect_peer(&addr1)
            .await
            .expect("connect node0 -> node1");
        wait_for_connected_peer(&node0).await;
        wait_for_connected_peer(&node1).await;

        p2p0.subscribe_collection("User")
            .await
            .expect("subscribe node0 User");
        p2p1.subscribe_collection("User")
            .await
            .expect("subscribe node1 User");

        p2p0.set_replicator(&addr1, vec!["User".to_string()])
            .await
            .expect("set replicator node0 -> node1");

        let response = node0
            .execute(
                r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID name age } }"#,
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "mutation returned errors: {:?}",
            response.errors
        );

        wait_for_collection_len(&node1, "User", 1).await;

        node0.shutdown().await;
        node1.shutdown().await;
    }
}
