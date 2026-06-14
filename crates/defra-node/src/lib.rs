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
use std::time::Duration;

use defra_core::signing::SigningConfig;
use identity::{Identity as _, IdentityKeyType, RawIdentity};
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
pub use query::QueryLimits;
pub use query::{QueryExecutor, QueryRequest, QueryResponse};
pub use schema::CollectionVersion;
#[cfg(feature = "otel")]
pub use telemetry::{TelemetryConfig, TelemetryHandle};

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
    node_identity_did: Option<String>,
    #[cfg(feature = "http")]
    txn_cleanup_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(feature = "p2p")]
    p2p_ops: Option<Arc<dyn defra_http::P2POperations>>,
    #[cfg(feature = "p2p")]
    p2p_lifecycle: Option<P2PLifecycle>,
    #[cfg(feature = "otel")]
    telemetry: std::sync::Mutex<Option<TelemetryHandle>>,
}

#[cfg(feature = "p2p")]
struct P2PLifecycle {
    inner: Mutex<Option<P2PLifecycleInner>>,
}

#[cfg(feature = "p2p")]
struct P2PLifecycleInner {
    transport: p2p::iroh::IrohTransport,
    coordinator: p2p::sync::SyncShutdownHandle,
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

#[cfg(feature = "http")]
#[derive(Clone, Copy)]
struct TransactionCleanupConfig {
    max_idle_age: Duration,
    sweep_interval: Duration,
}

#[cfg(feature = "p2p")]
impl P2PLifecycleInner {
    async fn shutdown(self) {
        let shutdown_started = std::time::Instant::now();
        let Self {
            transport,
            coordinator,
            endpoint_task,
            replication_task,
            event_handler_task,
            failure_recorder_task,
            retry_loop_task,
        } = self;

        let retry_started = std::time::Instant::now();
        abort_background_task("iroh retry loop", retry_loop_task).await;
        tracing::warn!(
            elapsed_ms = retry_started.elapsed().as_millis(),
            "P2P shutdown: retry loop stopped"
        );

        let coordinator_started = std::time::Instant::now();
        coordinator.shutdown().await;
        tracing::warn!(
            elapsed_ms = coordinator_started.elapsed().as_millis(),
            "P2P shutdown: coordinator stopped"
        );

        let transport_started = std::time::Instant::now();
        if let Err(error) = transport.shutdown().await {
            tracing::debug!(%error, "Iroh transport shutdown returned an error");
        }
        tracing::warn!(
            elapsed_ms = transport_started.elapsed().as_millis(),
            "P2P shutdown: transport stop requested"
        );

        drop(transport);
        drop(coordinator);

        let event_handler_started = std::time::Instant::now();
        abort_background_task("iroh event handler", event_handler_task).await;
        tracing::warn!(
            elapsed_ms = event_handler_started.elapsed().as_millis(),
            "P2P shutdown: event handler stopped"
        );

        let replication_started = std::time::Instant::now();
        abort_background_task("iroh replication loop", replication_task).await;
        tracing::warn!(
            elapsed_ms = replication_started.elapsed().as_millis(),
            "P2P shutdown: replication loop stopped"
        );

        let failure_started = std::time::Instant::now();
        abort_background_task("iroh failure recorder", failure_recorder_task).await;
        tracing::warn!(
            elapsed_ms = failure_started.elapsed().as_millis(),
            "P2P shutdown: failure recorder stopped"
        );

        let endpoint_started = std::time::Instant::now();
        await_endpoint_task(endpoint_task).await;
        tracing::warn!(
            elapsed_ms = endpoint_started.elapsed().as_millis(),
            total_elapsed_ms = shutdown_started.elapsed().as_millis(),
            "P2P shutdown: endpoint task stopped"
        );
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
        let request = QueryRequest::new(query_str);
        let Some(node_identity_did) = self.node_identity_did.as_deref() else {
            return self.runner.execute(request).await;
        };

        let signing_config = defra_core::signing::resolve_signing_config_with_flag(
            None,
            Some(node_identity_did),
            true,
        );
        let Some(signing_config) = signing_config else {
            return self.runner.execute(request).await;
        };

        execute_with_signing_context(
            self.runner.clone(),
            request,
            signing_config,
            node_identity_did.to_string(),
        )
        .await
    }

    /// Run `op` with the node's own identity installed as the ambient acting
    /// identity for the duration of the future, so DB-layer NAC checks resolve
    /// to the node itself (which holds full access). No-op when no node identity
    /// is configured. Uses the task_local so it survives `.await` on the
    /// direct-async schema paths.
    async fn as_node_identity<F, T>(&self, op: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        defra_core::current_identity::with_scoped_identity(self.node_identity_did.clone(), op).await
    }

    /// DID used as the embedded node identity for signing, when configured.
    pub fn node_identity_did(&self) -> Option<&str> {
        self.node_identity_did.as_deref()
    }

    /// Add a schema from a GraphQL SDL type definition.
    pub async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        self.as_node_identity(self.schema_ops.add_schema(sdl)).await
    }

    /// Create a materialized view from a source query and target SDL.
    ///
    /// `source_query` format: `"SourceType { field1 field2 ... }"`
    /// `target_sdl` is the SDL for the view collection (may include directives
    /// like `@downsample` that are forward-declared for future defradb.rs support).
    pub async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()> {
        self.as_node_identity(self.schema_ops.add_view(source_query, target_sdl))
            .await
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
        self.as_node_identity(self.schema_ops.patch_collection(collection_name, patch))
            .await
    }

    /// Activate a specific collection version by its `version_id`.
    ///
    /// Deactivates sibling versions of the same collection and updates the
    /// collection-name pointer to resolve to this version. If migrations are
    /// registered, documents are reindexed through them.
    pub async fn set_active_collection_version(&self, version_id: &str) -> anyhow::Result<()> {
        self.as_node_identity(self.schema_ops.set_active_collection_version(version_id))
            .await
    }

    /// Register a Lens migration between two collection versions.
    ///
    /// Returns the content-addressed [`TransformId`] of the stored transform.
    /// Placeholder versions are created if the source or destination are not
    /// yet materialized, allowing migrations to be registered ahead of patches.
    pub async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId> {
        self.as_node_identity(self.schema_ops.set_migration(config))
            .await
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
    ///
    /// **The node should not be used after this call.** When the `otel`
    /// feature is on, `shutdown` calls `TelemetryHandle::shutdown` which
    /// flushes and disables the global tracer provider. Subsequent calls
    /// that emit spans (e.g. `execute`, `add_schema`) go to a no-op
    /// tracer — they still work functionally, but observability is gone
    /// with no error surfaced. Drop the node after shutdown completes.
    pub async fn shutdown(&self) {
        #[cfg(feature = "http")]
        if let Some(task) = self.txn_cleanup_task.lock().await.take() {
            task.abort();
            let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        }

        #[cfg(feature = "p2p")]
        if let Some(lifecycle) = &self.p2p_lifecycle {
            lifecycle.shutdown().await;
        }

        // Flush buffered spans / metrics. Done after P2P shutdown so trailing
        // shutdown spans are still captured. Go DefraDB never calls provider
        // shutdown — we don't repeat that bug.
        //
        // `handle.shutdown()` synchronously joins the SDK batch-exporter
        // thread (up to ~5 s, double with metrics), so run it on a blocking
        // thread rather than stalling this Tokio worker / reactor.
        #[cfg(feature = "otel")]
        {
            let handle = match self.telemetry.lock() {
                Ok(mut guard) => guard.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(handle) = handle {
                let _ = tokio::task::spawn_blocking(move || handle.shutdown()).await;
            }
        }
    }
}

async fn execute_with_signing_context(
    executor: Arc<dyn QueryExecutor>,
    request: QueryRequest,
    signing_config: SigningConfig,
    node_did: String,
) -> QueryResponse {
    let handle = tokio::runtime::Handle::current();
    let batch_session_key = Some(signing_config.public_key_hex.clone());

    match tokio::task::spawn_blocking(move || {
        defra_core::signing::set_signing_config(Some(signing_config));
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        // Install the node identity as the ambient acting identity so DB-layer
        // NAC checks resolve to the node itself. The pool thread is reused, so
        // the guard clears it when the closure ends to prevent cross-request leak.
        let _id_guard = defra_core::current_identity::scoped_current_identity(Some(node_did));
        handle.block_on(async { executor.execute(request).await })
    })
    .await
    {
        Ok(response) => response,
        Err(join_error) => {
            QueryResponse::error(format!("query execution task failed: {join_error}"))
        }
    }
}

fn resolve_registered_node_identity(did: &str) -> anyhow::Result<SigningConfig> {
    let config = defra_core::signing::get_identity(did).ok_or_else(|| {
        anyhow::anyhow!("node identity DID {did} is not registered in the DefraDB signing registry")
    })?;
    if !config.has_local_private_key() && !config.has_remote_signer() {
        anyhow::bail!(
            "node identity DID {did} is registered without local key bytes or a remote signer"
        );
    }
    Ok(config)
}

fn local_raw_identity_from_registered_config(
    did: &str,
    config: &SigningConfig,
) -> anyhow::Result<Option<RawIdentity>> {
    if !config.has_local_private_key() {
        return Ok(None);
    }

    let key_type = identity_key_type_from_signing_key_type(config.key_type)?;
    let identity = RawIdentity::from_identity_key_type(key_type, &config.private_key_bytes)
        .map_err(|error| {
            anyhow::anyhow!("failed to load registered node identity {did}: {error}")
        })?;
    let derived_did = identity
        .did()
        .map_err(|error| anyhow::anyhow!("failed to derive registered node identity DID: {error}"))?
        .to_string();
    if derived_did != did {
        anyhow::bail!(
            "registered node identity DID mismatch: expected {did}, derived {derived_did}"
        );
    }
    Ok(Some(identity))
}

fn identity_key_type_from_signing_key_type(
    key_type: defra_core::signing::SigningKeyType,
) -> anyhow::Result<IdentityKeyType> {
    match key_type {
        defra_core::signing::SigningKeyType::Ed25519 => Ok(IdentityKeyType::Ed25519),
        defra_core::signing::SigningKeyType::Secp256k1 => Ok(IdentityKeyType::Secp256k1),
        defra_core::signing::SigningKeyType::Secp256r1 => Ok(IdentityKeyType::Secp256r1),
        unsupported => anyhow::bail!("unsupported registered node identity key type {unsupported}"),
    }
}

fn timeout_secs(timeout: Duration) -> u64 {
    if timeout.is_zero() {
        0
    } else {
        timeout
            .as_secs()
            .saturating_add(u64::from(timeout.subsec_nanos() > 0))
    }
}

/// Selects which persistent storage backend to use when `data_path` is set.
///
/// Defaults to `Redb` for backwards compatibility.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub enum StorageBackend {
    /// Pure-Rust LSM-tree backend.
    Lark,
    /// Pure-Rust embedded database using redb.
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
    storage_durability: storage::backends::DurabilityMode,
    embedding_url: Option<String>,
    embedding_model: Option<String>,
    embedding_api_key: Option<String>,
    document_acp: DocumentAcpConfig,
    node_identity_did: Option<String>,
    node_acp_enabled: bool,
    at_rest_encryption_key: Option<[u8; 32]>,
    query_timeout: Option<Duration>,
    query_limits: QueryLimits,
    #[cfg(feature = "http")]
    http_config: Option<HttpConfig>,
    #[cfg(feature = "p2p")]
    p2p_config: Option<P2PConfig>,
    #[cfg(feature = "otel")]
    telemetry_handle: Option<TelemetryHandle>,
}

struct StoreBuildArgs {
    acp_store: Arc<dyn acp::AcpStore>,
    document_acp_config: DocumentAcpConfig,
    db_options: db::DbOptions,
    event_bus: Arc<dyn events::Bus>,
    node_identity_did: Option<String>,
    node_acp_enabled: bool,
    query_timeout: Option<Duration>,
    query_limits: QueryLimits,
    #[cfg(feature = "http")]
    transaction_cleanup_config: Option<TransactionCleanupConfig>,
    #[cfg(feature = "p2p")]
    p2p_config: Option<P2PConfig>,
    #[cfg(feature = "otel")]
    telemetry_handle: Option<TelemetryHandle>,
}

struct PersistentStoreBuildArgs {
    document_acp_config: DocumentAcpConfig,
    db_options: db::DbOptions,
    event_bus: Arc<dyn events::Bus>,
    node_identity_did: Option<String>,
    node_acp_enabled: bool,
    query_timeout: Option<Duration>,
    query_limits: QueryLimits,
    #[cfg(feature = "http")]
    transaction_cleanup_config: Option<TransactionCleanupConfig>,
    #[cfg(feature = "p2p")]
    p2p_config: Option<P2PConfig>,
    #[cfg(feature = "otel")]
    telemetry_handle: Option<TelemetryHandle>,
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
    /// Using a backend requires that backend feature to be enabled.
    pub fn with_storage_backend(mut self, backend: StorageBackend) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Set the persistent storage durability mode (default: `Immediate`).
    ///
    /// `Immediate` fsyncs acknowledged commits. `Eventual` can improve write
    /// throughput, but OS crashes may lose acknowledged writes.
    pub fn with_storage_durability(
        mut self,
        durability: storage::backends::DurabilityMode,
    ) -> Self {
        self.storage_durability = durability;
        self
    }

    /// Enable transparent at-rest value encryption for the persistent storage backend.
    pub fn with_at_rest_encryption_key(mut self, key: [u8; 32]) -> Self {
        self.at_rest_encryption_key = Some(key);
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

    /// Use an identity already registered in DefraDB's process-local signing registry.
    ///
    /// The caller must register the signer before calling [`NodeBuilder::build`].
    /// Registered identities may be backed by exportable private key bytes or by a
    /// remote signer such as a host Secure Enclave adapter.
    pub fn with_node_identity_did(mut self, did: impl Into<String>) -> Self {
        self.node_identity_did = Some(did.into());
        self
    }

    /// Enable Node Access Control (NAC) with the configured node identity as owner.
    ///
    /// NAC is disabled by default. When enabled, DB-layer node operations are
    /// gated by the NAC policy; the node's own identity always retains full
    /// access (it is the owner). Requires [`NodeBuilder::with_node_identity_did`]
    /// to be set with an identity backed by local key bytes, so the node DID can
    /// own the policy — [`NodeBuilder::build`] fails otherwise.
    pub fn with_node_acp_enabled(mut self) -> Self {
        self.node_acp_enabled = true;
        self
    }

    /// Set the query execution timeout.
    ///
    /// `Duration::ZERO` disables query execution timeouts.
    pub fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    /// Set GraphQL parsing and filter evaluation limits.
    pub fn with_query_limits(mut self, limits: QueryLimits) -> Self {
        self.query_limits = limits;
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

    /// Hand a pre-built telemetry handle to the node so it owns shutdown.
    ///
    /// The caller is responsible for calling [`telemetry::init`] and
    /// composing the bridge layer onto their `tracing` subscriber — this
    /// method just transfers ownership of the lifecycle handle so the node
    /// flushes providers when it shuts down. Because the handle is moved,
    /// calling this twice is a compile error rather than a silent overwrite.
    ///
    /// For a one-call ergonomic version, callers can write
    /// `.with_telemetry(telemetry::init(cfg)?.0)` — `init` requires a Tokio
    /// runtime and returns `(TelemetryHandle, SdkTracer)`.
    #[cfg(feature = "otel")]
    pub fn with_telemetry(mut self, handle: TelemetryHandle) -> Self {
        self.telemetry_handle = Some(handle);
        self
    }

    /// Build and start the embedded DefraDB node.
    pub async fn build(self) -> anyhow::Result<EmbeddedNode> {
        let node_identity_did = self.node_identity_did.clone();
        let node_identity_config = node_identity_did
            .as_deref()
            .map(resolve_registered_node_identity)
            .transpose()?;

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
            if let (Some(did), Some(config)) =
                (node_identity_did.as_deref(), node_identity_config.as_ref())
            {
                if let Some(identity) = local_raw_identity_from_registered_config(did, config)? {
                    options = options.with_node_identity(identity);
                }
            }
            options
        };

        // 2. Extract configs before moving self
        #[cfg(feature = "http")]
        let http_config = self.http_config;
        #[cfg(feature = "http")]
        let transaction_cleanup_config = match http_config.as_ref() {
            Some(config) if config.transaction_idle_timeout.is_zero() => None,
            Some(config) => {
                if config.transaction_cleanup_interval.is_zero() {
                    anyhow::bail!(
                        "transaction cleanup interval must be non-zero when idle cleanup is enabled"
                    );
                }

                Some(TransactionCleanupConfig {
                    max_idle_age: config.transaction_idle_timeout,
                    sweep_interval: config.transaction_cleanup_interval,
                })
            }
            None => None,
        };
        #[cfg(feature = "p2p")]
        let p2p_config = self.p2p_config;
        let query_timeout = self.query_timeout;
        let query_limits = self.query_limits;
        let node_acp_enabled = self.node_acp_enabled;

        // Telemetry handle (if any) was moved into the builder via
        // `with_telemetry`. Threaded through `StoreBuildArgs` to the
        // `EmbeddedNode` construction site so its Drop / explicit
        // `shutdown()` runs at the right point.
        #[cfg(feature = "otel")]
        let telemetry_handle: Option<TelemetryHandle> = self.telemetry_handle;

        // 3. Storage backend + database
        let node = if let Some(path) = self.data_path {
            tokio::fs::create_dir_all(&path).await?;

            let persistent_args = PersistentStoreBuildArgs {
                document_acp_config: self.document_acp.clone(),
                db_options: db_options.clone(),
                event_bus,
                node_identity_did: node_identity_did.clone(),
                node_acp_enabled,
                query_timeout,
                query_limits,
                #[cfg(feature = "http")]
                transaction_cleanup_config,
                #[cfg(feature = "p2p")]
                p2p_config,
                #[cfg(feature = "otel")]
                telemetry_handle,
            };

            match self.storage_backend {
                #[cfg(feature = "lark")]
                StorageBackend::Lark => {
                    tracing::info!(
                        storage_backend = "lark",
                        data_path = %path.display(),
                        "embedded node starting"
                    );
                    let opts =
                        storage::LarkStoreOptions::new().with_durability(self.storage_durability);
                    let store = storage::LarkStore::open_with_options(&path, opts)
                        .map_err(|e| anyhow::anyhow!("failed to open lark store: {}", e))?;

                    Self::build_with_persistent_store(
                        store,
                        self.at_rest_encryption_key,
                        persistent_args,
                    )
                    .await?
                }
                #[cfg(not(feature = "lark"))]
                StorageBackend::Lark => {
                    return Err(anyhow::anyhow!(
                        "Lark backend requested but the `lark` feature is not enabled. \
                         Rebuild with `--features lark`."
                    ));
                }
                #[cfg(feature = "redb")]
                StorageBackend::Redb => {
                    tracing::info!(
                        storage_backend = "redb",
                        data_path = %path.display(),
                        "embedded node starting"
                    );
                    let opts =
                        storage::RedbStoreOptions::new().with_durability(self.storage_durability);
                    let store = storage::RedbStore::open_with_options(
                        path.to_str().ok_or_else(|| {
                            anyhow::anyhow!("data_path contains non-UTF8 characters")
                        })?,
                        opts,
                    )
                    .map_err(|e| anyhow::anyhow!("failed to open redb store: {}", e))?;

                    Self::build_with_persistent_store(
                        store,
                        self.at_rest_encryption_key,
                        persistent_args,
                    )
                    .await?
                }
                #[cfg(not(feature = "redb"))]
                StorageBackend::Redb => {
                    return Err(anyhow::anyhow!(
                        "Redb backend requested but the `redb` feature is not enabled. \
                         Rebuild with `--features redb`."
                    ));
                }
                #[cfg(feature = "rocksdb")]
                StorageBackend::RocksDb => {
                    tracing::info!(
                        storage_backend = "rocksdb",
                        data_path = %path.display(),
                        "embedded node starting"
                    );
                    let opts = storage::RocksDbStoreOptions::new()
                        .with_durability(self.storage_durability);
                    let store = storage::RocksDbStore::open_with_options(&path, opts)
                        .map_err(|e| anyhow::anyhow!("failed to open rocksdb store: {}", e))?;

                    Self::build_with_persistent_store(
                        store,
                        self.at_rest_encryption_key,
                        persistent_args,
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
                StoreBuildArgs {
                    acp_store,
                    document_acp_config: self.document_acp,
                    db_options,
                    event_bus,
                    node_identity_did: node_identity_did.clone(),
                    node_acp_enabled,
                    query_timeout,
                    query_limits,
                    #[cfg(feature = "http")]
                    transaction_cleanup_config,
                    #[cfg(feature = "p2p")]
                    p2p_config,
                    #[cfg(feature = "otel")]
                    telemetry_handle,
                },
            )
            .await?
        };

        // 4. Spawn HTTP server if configured
        #[cfg(feature = "http")]
        if let Some(http_cfg) = http_config {
            let server_config = defra_http::ServerConfig {
                address: http_cfg.address,
                request_timeout: timeout_secs(http_cfg.request_timeout),
                query_limits,
                ..Default::default()
            };
            let server =
                defra_http::Server::from_arc_with_config(node.runner.clone(), server_config)
                    .with_event_bus_arc(node.event_bus.clone());

            let server = if let Some(did) = node_identity_did.as_ref() {
                server.with_node_identity_did(did.clone())
            } else {
                server
            };

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

    async fn build_with_persistent_store<S: storage::corekv::Store + 'static>(
        store: S,
        at_rest_encryption_key: Option<[u8; 32]>,
        args: PersistentStoreBuildArgs,
    ) -> anyhow::Result<EmbeddedNode> {
        if let Some(key) = at_rest_encryption_key {
            tracing::info!("at-rest encryption enabled (value-only, AES-256-GCM)");
            let store = Arc::new(storage::encrypted_store::EncryptedStore::new(store, key));
            Self::build_with_persistent_store_arc(store, args).await
        } else {
            Self::build_with_persistent_store_arc(Arc::new(store), args).await
        }
    }

    async fn build_with_persistent_store_arc<S: storage::corekv::Store + 'static>(
        store: Arc<S>,
        args: PersistentStoreBuildArgs,
    ) -> anyhow::Result<EmbeddedNode> {
        let PersistentStoreBuildArgs {
            document_acp_config,
            db_options,
            event_bus,
            node_identity_did,
            node_acp_enabled,
            query_timeout,
            query_limits,
            #[cfg(feature = "http")]
            transaction_cleanup_config,
            #[cfg(feature = "p2p")]
            p2p_config,
            #[cfg(feature = "otel")]
            telemetry_handle,
        } = args;

        let acp_store: Arc<dyn acp::AcpStore> =
            Arc::new(acp::PersistentAcpStore::from_store(store.clone()));

        Self::build_with_store(
            store,
            StoreBuildArgs {
                acp_store,
                document_acp_config,
                db_options,
                event_bus,
                node_identity_did,
                node_acp_enabled,
                query_timeout,
                query_limits,
                #[cfg(feature = "http")]
                transaction_cleanup_config,
                #[cfg(feature = "p2p")]
                p2p_config,
                #[cfg(feature = "otel")]
                telemetry_handle,
            },
        )
        .await
    }

    async fn build_with_store<S: storage::corekv::Store + 'static>(
        store: Arc<S>,
        args: StoreBuildArgs,
    ) -> anyhow::Result<EmbeddedNode> {
        let StoreBuildArgs {
            acp_store,
            document_acp_config,
            db_options,
            event_bus,
            node_identity_did,
            node_acp_enabled,
            query_timeout,
            query_limits,
            #[cfg(feature = "http")]
            transaction_cleanup_config,
            #[cfg(feature = "p2p")]
            p2p_config,
            #[cfg(feature = "otel")]
            telemetry_handle,
        } = args;

        let embedding_config = db_options.embedding_config();

        // Open database
        let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
            .await
            .map_err(|e| anyhow::anyhow!("failed to open database: {}", e))?;

        // Wire event bus so mutations publish events
        database.set_event_bus(event_bus.clone());
        let database = Arc::new(database);

        // Node Access Control: only installed when explicitly enabled. The node
        // identity owns the policy, so DB-layer node operations run by the node
        // itself are always allowed (the node-DID bypass in check_node_access).
        if node_acp_enabled {
            let owner = node_identity_did
                .as_deref()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "node ACP requires a node identity; set one with with_node_identity_did()"
                    )
                })
                .and_then(|did| {
                    identity::Did::new(did)
                        .map_err(|e| anyhow::anyhow!("invalid node identity DID for node ACP: {e}"))
                })?;
            if database.node_did().as_ref() != Some(&owner) {
                anyhow::bail!(
                    "node ACP requires the node identity to be backed by local key bytes so the \
                     node DID can own the policy"
                );
            }
            let nac_store = Arc::new(acp::PersistentZanzibarStore::from_store(store.clone()));
            let nac_config = db::NacConfig::new().with_enabled().with_dev_mode();
            let nac_manager = Arc::new(db::NacManager::new(nac_store, nac_config));
            nac_manager
                .initialize(Some(&owner))
                .await
                .map_err(|e| anyhow::anyhow!("failed to enable node ACP: {e}"))?;
            database.set_nac_manager(nac_manager);
        }

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

        #[cfg(feature = "p2p")]
        let txn_broadcaster: Option<Arc<dyn db::event_emission::TxnBroadcaster>> =
            p2p_result.as_ref().map(|r| r.txn_broadcaster.clone());
        #[cfg(not(feature = "p2p"))]
        let txn_broadcaster: Option<Arc<dyn db::event_emission::TxnBroadcaster>> = None;

        let registry = Arc::new(match txn_broadcaster {
            Some(b) => db::DbTransactionRegistry::with_broadcaster(database.clone(), b),
            None => db::DbTransactionRegistry::new(database.clone()),
        });

        #[cfg(feature = "http")]
        let txn_cleanup_task = transaction_cleanup_config.map(|cleanup| {
            tracing::info!(
                max_idle_age_secs = cleanup.max_idle_age.as_secs(),
                sweep_interval_secs = cleanup.sweep_interval.as_secs(),
                "Transaction idle cleanup worker enabled"
            );
            registry.start_stale_transaction_cleanup(cleanup.max_idle_age, cleanup.sweep_interval)
        });

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
                .with_lens_store(database.lens_store().clone())
                .with_query_limits(query_limits);
        let query_runner = if let Some(timeout) = query_timeout {
            query_runner.with_query_timeout(timeout_secs(timeout))
        } else {
            query_runner
        };

        let runner: Arc<dyn QueryExecutor> = Arc::new(query_runner);
        let schema_ops: Arc<dyn SchemaOps> =
            Arc::new(db_impls::DbSchemaOps::new(database.clone(), query_limits));

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
            node_identity_did,
            #[cfg(feature = "http")]
            txn_cleanup_task: tokio::sync::Mutex::new(txn_cleanup_task),
            #[cfg(feature = "p2p")]
            p2p_ops,
            #[cfg(feature = "p2p")]
            p2p_lifecycle,
            #[cfg(feature = "otel")]
            telemetry: std::sync::Mutex::new(telemetry_handle),
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

        let restored_doc_ids =
            restore_iroh_p2p_state(store.clone(), &transport, &coordinator).await;

        let peer_id = transport.local_peer_id().to_string();
        tracing::info!(peer_id = %peer_id, "P2P started (IROH/QUIC)");
        let adapter = defra_p2p_adapter::IrohP2PAdapter::with_full_context(
            transport.clone(),
            coordinator.clone(),
            doc_pusher,
            event_bus,
            version_syncer,
            db::node_access_checker(database.clone()),
        );
        adapter.set_initial_tracked_documents(restored_doc_ids);
        let ops: Arc<dyn defra_http::P2POperations> = Arc::new(adapter);

        Ok(P2PSetupResult {
            ops,
            lifecycle: Some(P2PLifecycle::new(P2PLifecycleInner {
                transport,
                coordinator: coordinator.shutdown_handle(),
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
            txn_broadcaster: replication.txn_broadcaster,
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
async fn restore_iroh_p2p_state<S, B>(
    store: Arc<S>,
    transport: &p2p::iroh::IrohTransport,
    coordinator: &Arc<p2p::sync::IrohSyncCoordinator<B>>,
) -> std::collections::HashSet<String>
where
    S: storage::corekv::Store + 'static,
    B: blockstore::Blockstore + 'static,
{
    let peerstore = storage::stores::Peerstore::new(store);

    match peerstore.list_replicators().await {
        Ok(entries) => {
            for (peer_id_str, data) in entries {
                let replicator = match p2p::ReplicatorInfo::from_bytes(&data) {
                    Ok(replicator) => replicator,
                    Err(error) => {
                        tracing::warn!(
                            peer_id = %peer_id_str,
                            error = %error,
                            "failed to decode persisted P2P replicator"
                        );
                        continue;
                    }
                };
                let peer_id = p2p::transport::PeerId::new(replicator.peer_id_str().to_string());
                if let Err(error) = coordinator
                    .create_replicator(&peer_id, replicator.collections.clone(), false)
                    .await
                {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %error,
                        "failed to restore persisted P2P replicator"
                    );
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P replicators"),
    }

    let mut restored_doc_ids = std::collections::HashSet::new();
    match peerstore.load_documents().await {
        Ok(doc_ids) => {
            for doc_id in doc_ids {
                if let Err(error) = transport
                    .subscribe(p2p::topics::DefraTopic::document(&doc_id))
                    .await
                {
                    tracing::warn!(
                        doc_id = %doc_id,
                        error = %error,
                        "failed to restore P2P document subscription"
                    );
                }
                restored_doc_ids.insert(doc_id);
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to load persisted P2P document subscriptions");
        }
    }

    restored_doc_ids
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
                continue;
            }
            if let Err(error) = defra_p2p_adapter::set_persisted_replicator_status(
                &peerstore,
                &failure.peer_id.to_string(),
                p2p::ReplicatorStatus::Inactive,
            )
            .await
            {
                tracing::warn!(error = %error, "failed to mark replicator inactive");
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
                    let _ = defra_p2p_adapter::set_persisted_replicator_status(
                        &peerstore,
                        &peer_id_str,
                        p2p::ReplicatorStatus::Active,
                    )
                    .await;
                    continue;
                }

                let retry_filters = match peerstore.get_replicator(&peer_id_str).await {
                    Ok(Some(bytes)) => p2p::ReplicatorInfo::from_bytes(&bytes)
                        .map(|info| info.filters)
                        .unwrap_or_default(),
                    _ => p2p::ReplicationFilters::new(),
                };
                let mut all_succeeded = true;
                for (doc_id, collection_id) in &docs {
                    match db_merge::retry_doc_via_transport(
                        &transport,
                        database.as_ref(),
                        None,
                        &peer_id,
                        doc_id,
                        collection_id,
                        &retry_filters,
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
                    let _ = defra_p2p_adapter::set_persisted_replicator_status(
                        &peerstore,
                        &peer_id_str,
                        p2p::ReplicatorStatus::Active,
                    )
                    .await;
                } else {
                    let _ = defra_p2p_adapter::set_persisted_replicator_status(
                        &peerstore,
                        &peer_id_str,
                        p2p::ReplicatorStatus::Inactive,
                    )
                    .await;
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
    txn_broadcaster: Arc<dyn db::event_emission::TxnBroadcaster>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use defra_core::signing::{RemoteSigner, SigningConfig, SigningKeyType};

    use super::EmbeddedNode;

    #[cfg(feature = "http")]
    use axum::{routing::get, Router};

    #[cfg(feature = "http")]
    use super::HttpConfig;

    #[cfg(feature = "http")]
    #[test]
    fn http_config_accepts_extra_routes() {
        use std::time::Duration;

        let config = HttpConfig::new(9182)
            .with_request_timeout(Duration::from_secs(120))
            .with_transaction_idle_timeout(Duration::from_secs(900))
            .with_transaction_cleanup_interval(Duration::from_secs(30))
            .with_extra_routes(Router::new().route("/healthz", get(|| async { "ok" })));

        assert_eq!(config.address.port(), 9182);
        assert_eq!(config.request_timeout, Duration::from_secs(120));
        assert_eq!(config.transaction_idle_timeout, Duration::from_secs(900));
        assert_eq!(config.transaction_cleanup_interval, Duration::from_secs(30));
        assert!(config.extra_routes.is_some());
    }

    #[test]
    fn node_builder_accepts_query_timeout() {
        let timeout = std::time::Duration::from_secs(45);
        let builder = EmbeddedNode::builder().with_query_timeout(timeout);

        assert_eq!(builder.query_timeout, Some(timeout));
    }

    #[test]
    fn timeout_secs_rounds_non_zero_duration_up() {
        assert_eq!(super::timeout_secs(std::time::Duration::ZERO), 0);
        assert_eq!(super::timeout_secs(std::time::Duration::from_millis(1)), 1);
        assert_eq!(super::timeout_secs(std::time::Duration::new(2, 1)), 3);
        assert_eq!(super::timeout_secs(std::time::Duration::from_secs(5)), 5);
    }

    #[tokio::test]
    async fn node_identity_did_requires_registered_signer() {
        defra_core::signing::clear_identity_store();

        let error = match EmbeddedNode::builder()
            .with_node_identity_did("did:key:zMissing")
            .build()
            .await
        {
            Ok(_) => panic!("unregistered node identity must fail"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("is not registered in the DefraDB signing registry"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn node_identity_did_accepts_registered_remote_signer() {
        defra_core::signing::clear_identity_store();

        let did = "did:key:zRegisteredRemote";
        defra_core::signing::store_identity(
            did,
            SigningConfig {
                key_type: SigningKeyType::Secp256r1,
                private_key_bytes: Vec::new(),
                public_key_bytes: vec![2, 3, 4],
                public_key_hex: "020304".to_string(),
                remote_signer: Some(Arc::new(TestRemoteSigner)),
                signing_authorization: None,
            },
        );

        let node = EmbeddedNode::builder()
            .with_node_identity_did(did)
            .build()
            .await
            .expect("registered remote signer should build");

        assert_eq!(node.node_identity_did(), Some(did));
        node.shutdown().await;
        defra_core::signing::clear_identity_store();
    }

    struct TestRemoteSigner;

    impl RemoteSigner for TestRemoteSigner {
        fn sign_sync(
            &self,
            _data: &[u8],
            _authorization: Option<&defra_core::signing::SigningAuthorization>,
        ) -> Result<Vec<u8>, String> {
            Ok(vec![1, 2, 3])
        }
    }
}

#[cfg(all(test, feature = "p2p"))]
mod p2p_tests;
