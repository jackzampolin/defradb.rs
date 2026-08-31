//! Reusable embedded DefraDB node builder.
//!
//! Wraps defradb.rs library crates behind a clean builder API so that
//! downstream binaries can embed a DefraDB instance without duplicating
//! wiring code.
//!
//! P2P uses Iroh/QUIC only. libp2p lives on CLI, FFI, and `embedded`.
//!
//! ## Cargo features
//!
//! - `native` — native host (tokio, event channel). Default-on.
//! - `sourcehub` — on-chain document ACP. Default-on. Omit for local-only ACP.
//! - `wasmtime-runtime` — Lens WASM execution. Default-on. Without it,
//!   [`EmbeddedNode::set_migration`] returns an explicit error.
//! - `p2p` — Iroh/QUIC replication. Implies `native`. Does **not** compile libp2p.
//! - `http` — GraphQL HTTP server.
//! - `otel` — OpenTelemetry exporter.

mod acp_ops;
mod benchmark_data_gen;
mod benchmark_queries;
mod benchmark_stats;
#[doc(hidden)]
pub mod benchmark_support;
pub mod coding_search;
pub mod config;
#[cfg(test)]
mod dac_api_tests;
mod db_impls;
pub mod dense_search;
#[cfg(test)]
mod embedded_query_identity_tests;
mod node_acp;
#[cfg(feature = "p2p")]
mod p2p_runtime;
pub mod search_chunks;
mod signed_query_runtime;
pub mod version;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use defra_core::signing::SigningConfig;
use identity::{Identity as _, IdentityKeyType, RawIdentity};
use rand::Rng;

/// Re-exported so callers can name `Arc<dyn DocumentACP>` (the type
/// [`EmbeddedNode::document_acp`] returns) without depending on `acp` directly.
pub use acp::{DocumentACP, DocumentPermission, Identity as AcpIdentity};
pub use coding_search::{
    CodingHybridSearchHit, CodingHybridSearchRequest, CodingHybridSearchResponse,
    CodingSearchTarget,
};
pub use config::DocumentAcpConfig;
#[cfg(feature = "http")]
pub use config::HttpConfig;
#[cfg(feature = "p2p")]
pub use config::P2PConfig;
#[cfg(feature = "sourcehub")]
pub use config::SourceHubConfig;
pub use dense_search::{DenseHybridSearchHit, DenseHybridSearchRequest, DenseHybridSearchResponse};
pub use events::EventName;
pub use lens::{LensConfig, LensModule, TransformId};
pub use query::QueryLimits;
pub use query::{QueryExecutor, QueryRequest, QueryResponse, TransactionHandle};
pub use schema::CollectionVersion;
pub use telemetry::{ConflictMetricsSnapshot, RetryLayerSnapshot};
#[cfg(feature = "otel")]
pub use telemetry::{TelemetryConfig, TelemetryHandle};

#[cfg(not(target_arch = "wasm32"))]
use signed_query_runtime::{
    execute_with_signing_context, unavailable_node_signer_response, SignedQueryRuntime,
    SIGNED_QUERY_DRAIN_TIMEOUT,
};

/// Retry policy for [`EmbeddedNode::execute_with_retry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecuteRetryPolicy {
    /// Number of retries after the initial attempt.
    pub max_retries: u32,
    /// Backoff before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff between retries.
    pub max_backoff: Duration,
}

impl ExecuteRetryPolicy {
    /// Create a retry policy with bounded exponential backoff.
    pub const fn new(max_retries: u32, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff,
        }
    }
}

impl Default for ExecuteRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(250),
        }
    }
}

/// Type-erased schema operations so we can store DB<S> without leaking the Store generic.
#[async_trait::async_trait]
trait SchemaOps: Send + Sync {
    async fn add_schema(&self, sdl: &str) -> anyhow::Result<Vec<CollectionVersion>>;
    async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()>;
    async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> anyhow::Result<CollectionVersion>;
    async fn set_active_collection_version(&self, version_id: &str) -> anyhow::Result<()>;
    async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId>;
    async fn materialize_collection(&self, collection_name: &str) -> anyhow::Result<usize>;
    fn list_collections(&self) -> anyhow::Result<Vec<String>>;
    fn get_collection(&self, name: &str) -> anyhow::Result<Option<CollectionVersion>>;
    async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> anyhow::Result<Option<CollectionVersion>>;
    async fn get_all_collection_versions(&self) -> anyhow::Result<Vec<CollectionVersion>>;
}

#[cfg(feature = "http")]
struct HttpSchemaOps {
    schema_ops: Arc<dyn SchemaOps>,
}

#[cfg(feature = "http")]
#[async_trait::async_trait]
impl defra_http::router::SchemaOperations for HttpSchemaOps {
    async fn add_schema(&self, sdl: &str) -> Result<Vec<CollectionVersion>, String> {
        self.schema_ops
            .add_schema(sdl)
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait::async_trait]
trait BlockOps: Send + Sync {
    async fn signed_block_bytes(
        &self,
        cid: &str,
        caller_did: Option<&str>,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)>;
    async fn verified_signer_did(&self, cid: &str) -> anyhow::Result<String>;
    async fn verified_signer_did_in_txn(
        &self,
        cid: &str,
        transaction: &TransactionHandle,
    ) -> anyhow::Result<String>;
}

/// An embedded DefraDB node with query execution and event subscription.
pub struct EmbeddedNode {
    runner: Arc<dyn QueryExecutor>,
    event_bus: Arc<dyn events::Bus>,
    schema_ops: Arc<dyn SchemaOps>,
    #[cfg(feature = "http")]
    collection_version_ops: Arc<dyn defra_http::router::CollectionVersionOperations>,
    block_ops: Arc<dyn BlockOps>,
    acp_ops: Arc<dyn acp_ops::AcpOps>,
    document_acp: Arc<dyn acp::DocumentACP>,
    embedding_config: db::EmbeddingClientConfig,
    node_identity_did: Option<String>,
    node_query_identity: Option<identity::Did>,
    #[cfg(not(target_arch = "wasm32"))]
    signed_query_runtime: Option<SignedQueryRuntime>,
    #[cfg(not(target_arch = "wasm32"))]
    transaction_stats: Option<storage::TransactionStatsHandle>,
    #[cfg(feature = "http")]
    txn_cleanup_task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(feature = "p2p")]
    p2p_ops: Option<Arc<dyn defra_http::P2POperations>>,
    #[cfg(feature = "p2p")]
    p2p_lifecycle: Option<p2p_runtime::P2PLifecycle>,
    #[cfg(feature = "otel")]
    telemetry: std::sync::Mutex<Option<TelemetryHandle>>,
}

#[cfg(feature = "http")]
#[derive(Clone, Copy)]
struct TransactionCleanupConfig {
    max_idle_age: Duration,
    sweep_interval: Duration,
}

impl EmbeddedNode {
    /// Start building a new embedded node.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    /// Execute a GraphQL query or mutation.
    ///
    /// A configured node identity is used as the document ACP actor. Prepared
    /// requests with an explicit identity retain that actor instead.
    pub async fn execute(&self, query_str: &str) -> QueryResponse {
        self.execute_request_once(QueryRequest::new(query_str))
            .await
    }

    /// Execute a GraphQL query or mutation, retrying transient transaction conflicts.
    ///
    /// A configured node identity is used as the document ACP actor on every
    /// attempt.
    pub async fn execute_with_retry(
        &self,
        query_str: &str,
        policy: ExecuteRetryPolicy,
    ) -> QueryResponse {
        self.execute_request_with_retry(QueryRequest::new(query_str), policy)
            .await
    }

    /// Execute a prepared query request, retrying transient transaction conflicts.
    ///
    /// Requests without an identity use the configured node identity for
    /// document ACP. An explicit request identity takes precedence.
    pub async fn execute_request_with_retry(
        &self,
        request: QueryRequest,
        policy: ExecuteRetryPolicy,
    ) -> QueryResponse {
        execute_request_with_retry_loop(request, policy, |request| {
            self.execute_request_once(request)
        })
        .await
    }

    async fn execute_request_once(&self, request: QueryRequest) -> QueryResponse {
        self.execute_prepared_request(request, None).await
    }

    async fn execute_prepared_request(
        &self,
        request: QueryRequest,
        txn_handle: Option<TransactionHandle>,
    ) -> QueryResponse {
        let request = self.with_default_query_identity(request);
        let Some(node_identity_did) = self.node_identity_did.as_deref() else {
            return match txn_handle {
                Some(handle) => self.runner.execute_in_txn(request, &handle).await,
                None => self.runner.execute(request).await,
            };
        };

        #[cfg(target_arch = "wasm32")]
        return QueryResponse::error(format!(
            "configured node signing identity {node_identity_did} cannot execute queries on wasm32"
        ));

        #[cfg(not(target_arch = "wasm32"))]
        let signing_config = defra_core::signing::resolve_signing_config_with_flag(
            None,
            Some(node_identity_did),
            true,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let Some(signing_config) = signing_config
        else {
            return unavailable_node_signer_response(node_identity_did);
        };
        #[cfg(not(target_arch = "wasm32"))]
        let Some(signed_query_runtime) = self.signed_query_runtime.as_ref() else {
            return unavailable_node_signer_response(node_identity_did);
        };
        #[cfg(not(target_arch = "wasm32"))]
        let Some(signed_query_permit) = signed_query_runtime.admit() else {
            return QueryResponse::error("signed query runtime is shutting down");
        };

        #[cfg(not(target_arch = "wasm32"))]
        execute_with_signing_context(
            self.runner.clone(),
            request,
            txn_handle,
            signing_config,
            node_identity_did.to_string(),
            signed_query_runtime.handle(),
            signed_query_permit,
        )
        .await
    }

    /// Execute a prepared query request within an existing transaction.
    ///
    /// When the node has a configured identity, the same signing and ambient
    /// identity context used by [`Self::execute`] is applied to this request.
    /// It is also the document ACP actor unless the request supplies one.
    pub async fn execute_request_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        self.execute_prepared_request(request, Some(handle.clone()))
            .await
    }

    fn with_default_query_identity(&self, mut request: QueryRequest) -> QueryRequest {
        if request.identity.is_none() {
            request.identity.clone_from(&self.node_query_identity);
        }
        request
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

    /// Cryptographically verify a committed block and return its signer DID.
    ///
    /// This reads the signature linked by `cid`, verifies it over the
    /// canonical DAG-CBOR block bytes, and derives the DID from the verified
    /// public key. Unsigned, malformed, missing, or invalid blocks fail closed.
    pub async fn verified_block_signer_did(&self, cid: &str) -> anyhow::Result<String> {
        self.block_ops.verified_signer_did(cid).await
    }

    /// Load canonical signed-block material after applying document ACP for
    /// the supplied caller. The caller must independently verify both CIDs and
    /// the detached signature before trusting the returned signer.
    pub async fn authorized_signed_block_bytes(
        &self,
        cid: &str,
        caller_did: Option<&str>,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        self.block_ops.signed_block_bytes(cid, caller_did).await
    }

    /// Cryptographically verify a block visible inside an active transaction.
    ///
    /// This variant can verify uncommitted blocks before the caller commits or
    /// rolls back the transaction.
    pub async fn verified_block_signer_did_in_txn(
        &self,
        cid: &str,
        transaction: &TransactionHandle,
    ) -> anyhow::Result<String> {
        self.block_ops
            .verified_signer_did_in_txn(cid, transaction)
            .await
    }

    /// Add a schema from a GraphQL SDL type definition.
    pub async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        self.as_node_identity(async { self.schema_ops.add_schema(sdl).await.map(|_| ()) })
            .await
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
    ///
    /// Requires the `wasmtime-runtime` feature (on by default). Without it this
    /// returns an explicit error instead of registering a no-op transform.
    pub async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId> {
        self.as_node_identity(self.schema_ops.set_migration(config))
            .await
    }

    /// Eagerly migrate and cache every known-version document in a collection.
    ///
    /// Returns the number of documents advanced to the active version. This is a
    /// datastore-only operation and does not create or broadcast document commits.
    pub async fn materialize_collection(&self, collection_name: &str) -> anyhow::Result<usize> {
        self.as_node_identity(self.schema_ops.materialize_collection(collection_name))
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

    /// Begin a transaction owned by this embedded node.
    pub async fn begin_transaction(
        &self,
        readonly: bool,
    ) -> Result<TransactionHandle, query::TransactionError> {
        self.runner.begin_txn(readonly).await
    }

    /// Commit a transaction owned by this embedded node.
    pub async fn commit_transaction(
        &self,
        transaction: &TransactionHandle,
    ) -> Result<(), query::TransactionError> {
        self.runner.commit_txn(transaction).await
    }

    /// Roll back a transaction owned by this embedded node.
    pub async fn rollback_transaction(
        &self,
        transaction: &TransactionHandle,
    ) -> Result<(), query::TransactionError> {
        self.runner.rollback_txn(transaction).await
    }

    /// Access the raw query executor for advanced use.
    ///
    /// Calling `execute` or `execute_in_txn` on this executor bypasses the
    /// node's signing context and default ACP identity. Use [`Self::execute`]
    /// or [`Self::execute_request_in_txn`] for document reads and writes.
    pub fn runner(&self) -> &Arc<dyn QueryExecutor> {
        &self.runner
    }

    /// Access the event bus directly.
    pub fn event_bus(&self) -> &Arc<dyn events::Bus> {
        &self.event_bus
    }

    /// Register a DAC policy (Go: client.Store::AddDACPolicy). Returns the policy ID.
    pub async fn add_dac_policy(&self, identity: &str, policy: &str) -> anyhow::Result<String> {
        self.acp_ops.add_dac_policy(identity, policy).await
    }

    /// Register a DAC policy under an explicit id-salt counter (local ACP only).
    ///
    /// Local ACP policy ids are `sha256(policy fields || counter)` with a
    /// per-process counter, so two nodes that register the same policy after
    /// different histories derive different ids. A collection bound to a
    /// policy embeds the id in its SDL, and the SDL text determines the
    /// collection id — so every peer joining a policy-bound collection must
    /// reproduce the creator's exact policy id. Passing the creator's counter
    /// (typically 1, its first policy) makes the id reproducible, and storing
    /// an existing id overwrites it, so re-registration is idempotent.
    pub async fn add_dac_policy_with_counter(
        &self,
        identity: &str,
        policy: &str,
        counter: u64,
    ) -> anyhow::Result<String> {
        self.acp_ops
            .add_dac_policy_with_counter(identity, policy, counter)
            .await
    }

    /// Grant `target` the `relation` on a document (Go: client.Store::AddDACActorRelationship).
    ///
    /// `identity` is the requesting actor's DID; it must own the document or
    /// hold a relation that manages `relation`. `target` is an actor DID, the
    /// all-actors wildcard `*`, or a structured subject.
    ///
    /// Returns `existed_already`: `true` means the relationship was already
    /// present and the call was a no-op, `false` means it was newly added. A new
    /// grant also publishes a document-update event.
    pub async fn add_dac_actor_relationship(
        &self,
        identity: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<bool> {
        self.acp_ops
            .add_dac_actor_relationship(identity, collection, doc_id, relation, target)
            .await
    }

    /// Revoke `target`'s `relation` on a document (Go: client.Store::DeleteDACActorRelationship).
    ///
    /// `identity` is the requesting actor's DID; it must own the document or
    /// hold a relation that manages `relation`.
    ///
    /// Returns `record_found`: `true` means the relationship existed and was
    /// deleted, `false` means there was nothing to delete.
    pub async fn delete_dac_actor_relationship(
        &self,
        identity: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<bool> {
        self.acp_ops
            .delete_dac_actor_relationship(identity, collection, doc_id, relation, target)
            .await
    }

    /// Access the raw document ACP handle (Go: `node.DB.DocumentACP()`).
    ///
    /// This is an escape hatch for policy and relationship operations the node
    /// does not wrap. It bypasses node access control and the collection-policy
    /// lookup, so standard flows belong on [`Self::add_dac_policy`],
    /// [`Self::add_dac_actor_relationship`], and
    /// [`Self::delete_dac_actor_relationship`].
    pub fn document_acp(&self) -> Arc<dyn acp::DocumentACP> {
        self.document_acp.clone()
    }

    /// Access the resolved node-level embedding runtime config.
    pub fn embedding_config(&self) -> &db::EmbeddingClientConfig {
        &self.embedding_config
    }

    /// Capture backend-neutral transaction conflict and commit-gate diagnostics.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn transaction_stats(&self) -> Option<storage::TransactionStatsSnapshot> {
        self.transaction_stats
            .as_ref()
            .map(storage::TransactionStatsHandle::snapshot)
    }

    /// Capture process-lifetime transaction retry and client escape counters.
    pub fn conflict_metrics(&self) -> ConflictMetricsSnapshot {
        telemetry::conflict_metrics_snapshot()
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

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(runtime) = &self.signed_query_runtime {
            if !runtime
                .close_admission_and_wait_for(SIGNED_QUERY_DRAIN_TIMEOUT)
                .await
            {
                tracing::warn!(
                    active_queries = runtime.active_queries(),
                    timeout_secs = SIGNED_QUERY_DRAIN_TIMEOUT.as_secs(),
                    "timed out draining signed queries during node shutdown"
                );
            }
        }

        #[cfg(feature = "p2p")]
        if let Some(lifecycle) = &self.p2p_lifecycle {
            lifecycle.shutdown().await;
        }

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(runtime) = &self.signed_query_runtime {
            runtime.shutdown().await;
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

async fn execute_request_with_retry_loop<F, Fut>(
    request: QueryRequest,
    policy: ExecuteRetryPolicy,
    mut execute_once: F,
) -> QueryResponse
where
    F: FnMut(QueryRequest) -> Fut,
    Fut: std::future::Future<Output = QueryResponse>,
{
    let mut retry_count = 0;
    let mut backoff = policy.initial_backoff.min(policy.max_backoff);

    loop {
        let response = execute_once(request.clone()).await;
        if !is_transaction_conflict_response(&response) {
            if retry_count > 0 {
                telemetry::record_retry_success(telemetry::RetryLayer::EmbeddedExecute);
            }
            return response;
        }
        if retry_count >= policy.max_retries {
            telemetry::record_retry_exhaustion(telemetry::RetryLayer::EmbeddedExecute);
            return response;
        }

        telemetry::record_retry_attempt(telemetry::RetryLayer::EmbeddedExecute);
        retry_count += 1;
        let delay = jittered_backoff(backoff);
        tracing::warn!(
            retry = retry_count,
            max_retries = policy.max_retries,
            backoff_ms = delay.as_millis(),
            "retrying embedded execute after transaction conflict"
        );

        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        backoff = next_retry_backoff(backoff, policy.max_backoff);
    }
}

fn is_transaction_conflict_response(response: &QueryResponse) -> bool {
    response.is_transaction_conflict()
}

fn jittered_backoff(max_backoff: Duration) -> Duration {
    if max_backoff.is_zero() {
        return Duration::ZERO;
    }

    let max_nanos = max_backoff.as_nanos().min(u64::MAX as u128) as u64;
    Duration::from_nanos(rand::thread_rng().gen_range(0..=max_nanos))
}

fn next_retry_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return Duration::ZERO;
    }
    current.saturating_mul(2).min(max_backoff)
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
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub enum StorageBackend {
    /// regolith, the one store. Kept as an enum so `with_storage_backend`
    /// stays on the builder and a later variant does not churn it.
    #[default]
    Regolith,
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
    max_txn_retries: Option<u32>,
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
    persistence: node_acp::Persistence,
    document_acp_config: DocumentAcpConfig,
    db_options: db::DbOptions,
    event_bus: Arc<dyn events::Bus>,
    node_identity_did: Option<String>,
    #[cfg(feature = "p2p")]
    node_p2p_identity: Option<Arc<RawIdentity>>,
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
    #[cfg(feature = "p2p")]
    node_p2p_identity: Option<Arc<RawIdentity>>,
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

    /// Set the maximum number of auto-commit transaction conflict retries.
    pub fn with_max_txn_retries(mut self, retries: u32) -> Self {
        self.max_txn_retries = Some(retries);
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
    ///
    /// Requires the `sourcehub` feature (on by default).
    #[cfg(feature = "sourcehub")]
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

    /// Enable Iroh/QUIC P2P replication.
    ///
    /// Requires the `p2p` feature, which implies `native`. Does not compile
    /// libp2p; that stack lives on CLI, FFI, and `embedded`.
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
        let node_p2p_identity = match (node_identity_did.as_deref(), node_identity_config.as_ref())
        {
            (Some(did), Some(config)) => {
                local_raw_identity_from_registered_config(did, config)?.map(Arc::new)
            }
            _ => None,
        };

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
            if let Some(retries) = self.max_txn_retries {
                options = options.with_max_txn_retries(retries);
            }
            if let Some(identity) = node_p2p_identity.as_ref() {
                options = options.with_node_identity_arc(identity.clone());
            }
            options
        };
        #[cfg(feature = "http")]
        let max_txn_retries = db_options.max_txn_retries();

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
                #[cfg(feature = "p2p")]
                node_p2p_identity: node_p2p_identity.clone(),
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

            tracing::info!(
                storage_backend = "regolith",
                data_path = %path.display(),
                "embedded node starting"
            );
            let opts =
                storage::RegolithStoreOptions::default().with_durability(self.storage_durability);
            let store = storage::RegolithStore::open_with_options(&path, opts)
                .map_err(|e| anyhow::anyhow!("failed to open regolith store: {}", e))?;

            Self::build_with_persistent_store(store, self.at_rest_encryption_key, persistent_args)
                .await?
        } else {
            tracing::info!(
                storage_backend = "memory",
                "embedded node starting (ephemeral, no data_path)"
            );
            let store = Arc::new(
                storage::RegolithStore::in_memory()
                    .map_err(|e| anyhow::anyhow!("failed to open in-memory regolith: {}", e))?,
            );

            Self::build_with_store(
                store,
                StoreBuildArgs {
                    persistence: node_acp::Persistence::Memory,
                    document_acp_config: self.document_acp,
                    db_options,
                    event_bus,
                    node_identity_did: node_identity_did.clone(),
                    #[cfg(feature = "p2p")]
                    node_p2p_identity,
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
                max_txn_retries,
                query_limits,
                ..Default::default()
            };
            let server =
                defra_http::Server::from_arc_with_config(node.runner.clone(), server_config)
                    .with_event_bus_arc(node.event_bus.clone())
                    .with_schema_arc(Arc::new(HttpSchemaOps {
                        schema_ops: Arc::clone(&node.schema_ops),
                    }))
                    .with_collection_versions_arc(Arc::clone(&node.collection_version_ops));

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
            #[cfg(feature = "p2p")]
            node_p2p_identity,
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

        Self::build_with_store(
            store,
            StoreBuildArgs {
                persistence: node_acp::Persistence::Persistent,
                document_acp_config,
                db_options,
                event_bus,
                node_identity_did,
                #[cfg(feature = "p2p")]
                node_p2p_identity,
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
            persistence,
            document_acp_config,
            db_options,
            event_bus,
            node_identity_did,
            #[cfg(feature = "p2p")]
            node_p2p_identity,
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

        let node_query_identity = node_identity_did
            .as_ref()
            .map(|did| identity::Did::new(did.clone()))
            .transpose()
            .map_err(|error| anyhow::anyhow!("invalid node identity DID: {error}"))?;

        let embedding_config = db_options.embedding_config();
        #[cfg(not(target_arch = "wasm32"))]
        let transaction_stats = store.transaction_stats_handle();

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

        // Build the node-owned signed-query runtime before starting P2P. If
        // runtime construction fails, the builder can return without leaving
        // already-started network tasks behind.
        #[cfg(not(target_arch = "wasm32"))]
        let signed_query_runtime = node_identity_did
            .as_ref()
            .map(|_| SignedQueryRuntime::new())
            .transpose()
            .map_err(anyhow::Error::msg)?;

        // P2P setup (affects mutator choice)
        #[cfg(feature = "p2p")]
        let mut p2p_result = if let Some(p2p_cfg) = p2p_config {
            Some(
                p2p_runtime::setup_p2p(
                    store.clone(),
                    database.clone(),
                    event_bus.clone(),
                    &p2p_cfg,
                    node_p2p_identity,
                )
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
        let txn_broadcaster: Option<Arc<dyn db::event::emission::TxnBroadcaster>> =
            p2p_result.as_ref().map(|r| r.txn_broadcaster.clone());
        #[cfg(not(feature = "p2p"))]
        let txn_broadcaster: Option<Arc<dyn db::event::emission::TxnBroadcaster>> = None;

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

        let acp_setup =
            node_acp::create_document_acp(store.clone(), persistence, &document_acp_config).await?;
        let document_acp = acp_setup.document_acp.clone();
        #[cfg(feature = "sourcehub")]
        let _strict_replicated_doc_access = acp_setup.sourcehub_acp.is_some();
        #[cfg(not(feature = "sourcehub"))]
        let _strict_replicated_doc_access = false;

        #[cfg(feature = "p2p")]
        if let Some(wire_document_acp) = p2p_result
            .as_mut()
            .and_then(|result| result.wire_document_acp.take())
        {
            wire_document_acp(document_acp.clone(), _strict_replicated_doc_access);
        }

        // Build the KMS once document ACP exists (same ordering as the CLI
        // runtime and crates/embedded/src/node.rs): blockstore-backed key
        // store, NAC/DAC release policy, and the pubsub transport created in
        // setup_p2p. Without this, encrypted blocks replicate but no peer can
        // ever fetch the DEK, so encrypted documents never become readable on
        // replicas.
        #[cfg(feature = "p2p")]
        if let Some(p2p) = p2p_result.as_mut() {
            // Blockstore-backed KeyStore: the KMS serves DEKs for ANY
            // encrypted write by reading the node's durable
            // encstore->blockstore, not a RAM-only map. The DB owns the
            // blockstore Arc (set_kms_blockstore) so the adapter can hold a
            // Weak and avoid the lock-pinning cycle (#976).
            let kms_blockstore = database.set_kms_blockstore(Arc::new(
                blockstore::DefraBlockstore::new(store.clone(), true),
            ));
            let enc_block_store: Arc<dyn kms::EncBlockStore> =
                Arc::new(db::DbEncBlockStore::new(database.clone(), kms_blockstore));
            let key_store: Arc<dyn kms::KeyStore> =
                Arc::new(kms::BlockstoreKeyStore::new(enc_block_store));
            let doc_lookup: Arc<dyn kms::DocCollectionLookup> =
                Arc::new(db::DbDocCollectionLookup::new(database.clone()));
            let policy = Arc::new(kms::NacDacPolicy::new(document_acp.clone(), doc_lookup));
            if let Some(nac) = database.nac_manager() {
                policy.set_node_acp(Arc::new(db::DbNodeAcpRead::new(nac)));
            }

            // Node identity used for cross-peer DEK requests. Anonymous
            // nodes use a stable placeholder DID.
            let kms_node_did = database.node_did().unwrap_or_else(|| {
                identity::Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
                    .expect("static anonymous DID parses")
            });

            let kms_service: Arc<dyn kms::KmsService> = Arc::new(kms::DefraKms::new(
                key_store,
                vec![p2p.kms_transport.clone()],
                policy as Arc<dyn kms::AccessPolicy>,
                Arc::new(db::DbBlockDocIDResolver::new(database.clone())),
                kms_node_did,
            ));

            // Bind this node's transport peer id into the KMS so served
            // ECIES replies carry the correct AAD peer id.
            kms_service.set_local_peer_id(p2p.local_peer_id.clone());

            // Serve handler with a Weak to break the transport<->kms Arc
            // cycle (transport holds handler -> kms -> transports ->
            // transport).
            struct KmsServeHandler {
                kms: std::sync::Weak<dyn kms::KmsService>,
            }
            #[async_trait::async_trait]
            impl kms::IncomingHandler for KmsServeHandler {
                async fn handle(
                    &self,
                    from: kms::PeerIdentity,
                    req: kms::FetchEncryptionKeyRequest,
                ) -> kms::Result<kms::FetchEncryptionKeyReply> {
                    match self.kms.upgrade() {
                        Some(kms) => kms.serve_request(from, req).await,
                        None => Err(kms::Error::Internal("kms dropped".into())),
                    }
                }
            }
            p2p.kms_transport.install_handler(Arc::new(KmsServeHandler {
                kms: Arc::downgrade(&kms_service),
            }));

            if let Some(wire_kms) = p2p.wire_kms.take() {
                wire_kms(kms_service.clone());
            }

            // Wire the KMS into the DB-held read/write path.
            database.set_kms(kms_service);
        }

        // Assemble query runner
        let query_runner =
            query::QueryRunner::with_arc_registry_and_provider(fetcher, provider, registry.clone())
                .with_mutator(mutator)
                .with_collection_truncator(db::DbCollectionTruncator::new_arc(database.clone()))
                .with_acp(document_acp.clone())
                .with_node_did(database.node_did())
                .with_lens_store(database.lens_store().clone())
                .with_query_limits(query_limits);
        let query_runner = if let Some(timeout) = query_timeout {
            query_runner.with_query_timeout(timeout_secs(timeout))
        } else {
            query_runner
        };

        let runner: Arc<dyn QueryExecutor> = Arc::new(query_runner);
        #[cfg(feature = "sourcehub")]
        let policy_lookup =
            acp_ops::PolicyLookup::new(acp_setup.local_zanzibar_store, acp_setup.sourcehub_acp);
        #[cfg(not(feature = "sourcehub"))]
        let policy_lookup = acp_ops::PolicyLookup::new(acp_setup.local_zanzibar_store);
        let schema_ops: Arc<dyn SchemaOps> = Arc::new(db_impls::DbSchemaOps::new(
            database.clone(),
            query_limits,
            document_acp.clone(),
            policy_lookup.clone(),
        ));
        #[cfg(feature = "http")]
        let collection_version_ops: Arc<
            dyn defra_http::router::CollectionVersionOperations,
        > = Arc::new(db_impls::DbCollectionVersionOps::new(Arc::clone(&database)));
        let acp_ops: Arc<dyn acp_ops::AcpOps> = Arc::new(acp_ops::DbAcpOps::new(
            database.clone(),
            document_acp.clone(),
            policy_lookup,
            event_bus.clone(),
        ));
        let block_ops: Arc<dyn BlockOps> = Arc::new(db_impls::DbBlockOps::new(
            database,
            document_acp.clone(),
            registry,
            node_query_identity.clone(),
        ));

        #[cfg(feature = "p2p")]
        let (p2p_ops, p2p_lifecycle) = match p2p_result {
            Some(result) => (Some(result.ops), result.lifecycle),
            None => (None, None),
        };

        Ok(EmbeddedNode {
            runner,
            event_bus,
            schema_ops,
            #[cfg(feature = "http")]
            collection_version_ops,
            block_ops,
            acp_ops,
            document_acp,
            embedding_config,
            node_identity_did,
            node_query_identity,
            #[cfg(not(target_arch = "wasm32"))]
            signed_query_runtime,
            #[cfg(not(target_arch = "wasm32"))]
            transaction_stats,
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
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};

    use defra_core::signing::{RemoteSigner, SigningConfig, SigningKeyType};
    use query::{QueryRequest, QueryResponseError};
    use tokio::sync::Mutex;

    use super::{EmbeddedNode, ExecuteRetryPolicy};

    #[cfg(feature = "http")]
    use axum::{routing::get, Router};

    #[cfg(feature = "http")]
    use super::HttpConfig;

    pub(super) static SIGNING_STORE_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
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

    #[cfg(feature = "http")]
    #[tokio::test]
    async fn embedded_http_exposes_schema_add_and_only_read_collection_versions() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);

        let node = EmbeddedNode::builder()
            .with_http(HttpConfig::with_addr(address))
            .build()
            .await
            .unwrap();
        let client = reqwest::Client::new();
        let schema_url = format!("http://{address}/api/v0/schema");
        let created = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(response) = client
                    .post(&schema_url)
                    .header(reqwest::header::CONTENT_TYPE, "text/plain")
                    .body("type Book { title: String }")
                    .send()
                    .await
                {
                    if response.status().is_success() {
                        break response
                            .json::<Vec<schema::CollectionVersion>>()
                            .await
                            .unwrap();
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("embedded HTTP server did not expose schema add");
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].name, "Book");

        let schema = client
            .get(&schema_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(schema.contains("type Book"));

        let url = format!("http://{address}/api/v0/collections/versions");
        let versions = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(response) = client.get(&url).send().await {
                    if response.status().is_success() {
                        break response
                            .json::<Vec<schema::CollectionVersion>>()
                            .await
                            .unwrap();
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("embedded HTTP server did not expose collection versions");

        let book = versions
            .iter()
            .find(|version| version.name == "Book")
            .expect("Book collection version should be returned");
        assert!(!book.collection_id.is_empty());
        assert!(!book.version_id.is_empty());

        let delete_response = client
            .delete(&url)
            .json(&vec![book.version_id.clone()])
            .send()
            .await
            .unwrap();
        assert_eq!(
            delete_response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "embedded collection observation must not enable destructive management"
        );

        node.shutdown().await;
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

    #[test]
    fn execute_retry_policy_defaults_are_bounded() {
        let policy = ExecuteRetryPolicy::default();

        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_backoff, std::time::Duration::from_millis(10));
        assert_eq!(policy.max_backoff, std::time::Duration::from_millis(250));
    }

    #[test]
    fn execute_retry_backoff_doubles_until_cap() {
        let max = std::time::Duration::from_millis(25);

        assert_eq!(
            super::next_retry_backoff(std::time::Duration::from_millis(10), max),
            std::time::Duration::from_millis(20)
        );
        assert_eq!(
            super::next_retry_backoff(std::time::Duration::from_millis(20), max),
            max
        );
    }

    #[test]
    fn execute_retry_classifier_matches_transaction_conflicts_only() {
        let conflict = query::QueryResponse::transaction_conflict(
            "commit error: datastore error: storage error: transaction conflict. Please retry",
        );
        assert!(super::is_transaction_conflict_response(&conflict));

        let validation = query::QueryResponse::error("schema error: invalid field");
        assert!(!super::is_transaction_conflict_response(&validation));

        let partial = query::QueryResponse::partial(
            serde_json::json!({"ok": true}),
            vec![QueryResponseError::new(
                "commit error: transaction conflict. Please retry",
            )],
        );
        assert!(!super::is_transaction_conflict_response(&partial));
    }

    #[tokio::test]
    async fn execute_retry_loop_retries_conflicts_until_success() {
        // Asserts a delta on process-global conflict metrics, so it cannot run
        // beside the other tests that drive the embedded execute path. They
        // already take this guard.
        let _serial = SIGNING_STORE_GUARD.lock().await;
        let metrics_before = telemetry::conflict_metrics_snapshot().embedded_execute;
        let attempts = Arc::new(AtomicUsize::new(0));
        let policy =
            ExecuteRetryPolicy::new(3, std::time::Duration::ZERO, std::time::Duration::ZERO);

        let response = super::execute_request_with_retry_loop(
            QueryRequest::new("mutation { noop }"),
            policy,
            {
                let attempts = attempts.clone();
                move |_request| {
                    let attempts = attempts.clone();
                    async move {
                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                        if attempt < 2 {
                            query::QueryResponse::transaction_conflict(
                                "commit error: storage error: transaction conflict. Please retry",
                            )
                        } else {
                            query::QueryResponse::success(serde_json::json!({"ok": true}))
                        }
                    }
                }
            },
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(!response.has_errors());
        assert_eq!(response.data, Some(serde_json::json!({"ok": true})));
        // The conflict counters are process-global and every test that runs a
        // query through the retry loop moves them, so an exact delta is a race:
        // it read 3 where it wanted 2 in CI. The loop's own behaviour is pinned
        // exactly by `attempts` above; what is left to check here is that the
        // telemetry fired at all, which a lower bound does without depending on
        // what else the binary is running.
        let metrics_after = telemetry::conflict_metrics_snapshot().embedded_execute;
        assert!(
            metrics_after.attempts - metrics_before.attempts >= 2,
            "the two retried attempts must be recorded"
        );
        assert!(
            metrics_after.successes - metrics_before.successes >= 1,
            "the eventual success must be recorded"
        );
    }

    #[tokio::test]
    async fn node_identity_did_requires_registered_signer() {
        let _serial = SIGNING_STORE_GUARD.lock().await;
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
        let _serial = SIGNING_STORE_GUARD.lock().await;
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

    pub(super) struct TestRemoteSigner;

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
mod p2p_bench;

#[cfg(all(test, feature = "p2p"))]
mod p2p_tests;
