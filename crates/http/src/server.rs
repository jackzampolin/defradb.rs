//! HTTP server configuration and startup.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use axum::extract::DefaultBodyLimit;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::Router;
use tokio::net::TcpListener;
use tower::limit::ConcurrencyLimitLayer;
use tower::timeout::TimeoutLayer;
use tower::{Layer, ServiceBuilder};
use tower_http::cors::CorsLayer;
use tower_http::normalize_path::NormalizePathLayer;
use tower_http::trace::TraceLayer;

use query::executor::QueryExecutor;
use query::rest::RestOperations;
use query::QueryLimits;
use serde_json::Map;

use crate::error::Result;
use crate::router::{
    create_router_with_state_and_body_limits, AcpOperations, AppState, AppStateBuilder,
    BackupOperations, BlockOperations, BodyLimits, BrowserSyncOperations,
    CollectionManagementOperations, CollectionVersionOperations, DocumentAcpOperations,
    DumpOperations, EncryptedIndexOperations, IndexOperations, LensOperations, ManageRequester,
    NodeAcpOperations, P2POperations, SchemaOperations, TransactionOperations, ViewOperations,
};

/// Default cap on an inline backup import body (100 MiB).
///
/// Replaces a hardcoded check that used to live in the import handler, where
/// it silently overrode the flag: `0` did not mean unlimited, and a larger
/// flag value could not raise the bound.
pub const DEFAULT_MAX_BACKUP_SIZE: u64 = 100 * 1024 * 1024;

/// Server configuration options.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind to (default: 127.0.0.1:9181).
    pub address: SocketAddr,
    /// Allowed CORS origins. Supports "*" for all origins (matches Go DefraDB).
    /// Empty vec = no CORS headers (browsers block cross-origin requests).
    pub allowed_origins: Vec<String>,
    /// Max request body size in bytes (0 = unlimited).
    pub max_body_size: u64,
    /// Max schema request body size in bytes (0 = unlimited).
    pub max_schema_size: u64,
    /// Max backup import body size in bytes (0 = unlimited).
    ///
    /// Defaults to 100 MiB rather than 0. Unlike Go, whose import reads from a
    /// server-side filepath and has no request body to bound
    /// (`http/handler_store.go:38-51`), ours uploads the backup inline, so this
    /// is the only thing standing between an import and unbounded buffering.
    pub max_backup_size: u64,
    /// Disable signing of commits, even when a node identity is configured.
    /// Mirrors Go's `datastore.nosigning` (`cli/start.go:194`).
    pub no_signing: bool,
    /// Request timeout in seconds (0 = no timeout).
    pub request_timeout: u64,
    /// Max concurrent requests (0 = unlimited).
    pub max_concurrent_requests: usize,
    /// Maximum retries after an auto-commit transaction conflict.
    pub max_txn_retries: u32,
    /// GraphQL parsing and filter evaluation limits.
    pub query_limits: QueryLimits,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::from(([127, 0, 0, 1], 9181)),
            allowed_origins: Vec::new(),
            max_body_size: 0,
            max_schema_size: 0,
            max_backup_size: DEFAULT_MAX_BACKUP_SIZE,
            no_signing: false,
            request_timeout: 300,
            max_concurrent_requests: 1000,
            max_txn_retries: db::DEFAULT_MAX_TXN_RETRIES,
            query_limits: QueryLimits::default(),
        }
    }
}

/// HTTP server for DefraDB.
pub struct Server {
    config: ServerConfig,
    tls: Option<crate::TlsConfig>,
    executor: Arc<dyn QueryExecutor>,
    rest: Option<Arc<dyn RestOperations>>,
    p2p: Option<Arc<dyn P2POperations>>,
    manage: Option<Arc<dyn ManageRequester>>,
    acp: Option<Arc<dyn AcpOperations>>,
    index: Option<Arc<dyn IndexOperations>>,
    encrypted_index: Option<Arc<dyn EncryptedIndexOperations>>,
    backup: Option<Arc<dyn BackupOperations>>,
    block: Option<Arc<dyn BlockOperations>>,
    browser_sync: Option<Arc<dyn BrowserSyncOperations>>,
    schema: Option<Arc<dyn SchemaOperations>>,
    lens: Option<Arc<dyn LensOperations>>,
    nac: Option<Arc<dyn NodeAcpOperations>>,
    collection_versions: Option<Arc<dyn CollectionVersionOperations>>,
    collection_mgmt: Option<Arc<dyn CollectionManagementOperations>>,
    doc_acp: Option<Arc<dyn DocumentAcpOperations>>,
    view: Option<Arc<dyn ViewOperations>>,
    dump: Option<Arc<dyn DumpOperations>>,
    txn_ops: Option<Arc<dyn TransactionOperations>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    node_options: Option<Arc<Map<String, serde_json::Value>>>,
    node_identity_did: Option<String>,
    dev_mode: bool,
}

impl Server {
    /// Create a new server with the given executor.
    pub fn new<E: QueryExecutor + 'static>(executor: E) -> Self {
        Self {
            config: ServerConfig::default(),
            tls: None,
            executor: Arc::new(executor),
            rest: None,
            p2p: None,
            manage: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            browser_sync: None,
            schema: None,
            lens: None,
            nac: None,
            collection_versions: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_options: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Create a server with custom configuration.
    pub fn with_config<E: QueryExecutor + 'static>(executor: E, config: ServerConfig) -> Self {
        Self {
            config,
            tls: None,
            executor: Arc::new(executor),
            rest: None,
            p2p: None,
            manage: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            browser_sync: None,
            schema: None,
            lens: None,
            nac: None,
            collection_versions: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_options: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Create a server from an Arc'd executor.
    pub fn from_arc(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            config: ServerConfig::default(),
            tls: None,
            executor,
            rest: None,
            p2p: None,
            manage: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            browser_sync: None,
            schema: None,
            lens: None,
            nac: None,
            collection_versions: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_options: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Create a server from an Arc'd executor with custom configuration.
    pub fn from_arc_with_config(executor: Arc<dyn QueryExecutor>, config: ServerConfig) -> Self {
        Self {
            config,
            tls: None,
            executor,
            rest: None,
            p2p: None,
            manage: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            browser_sync: None,
            schema: None,
            lens: None,
            nac: None,
            collection_versions: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_options: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Serve HTTPS using a previously validated certificate and private key.
    pub fn with_tls(mut self, config: crate::TlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    /// Set REST operations for collection/document endpoints.
    ///
    /// When REST operations are configured, the server enables additional endpoints:
    /// - `GET /api/v1/collections` - List all collections
    /// - `POST /api/v1/collections/{name}` - Create document(s)
    /// - `GET /api/v1/collections/{name}` - List collection document IDs
    /// - `PATCH /api/v1/collections/{name}` - Update documents matching a filter
    /// - `DELETE /api/v1/collections/{name}` - Delete documents matching a filter
    /// - `GET /api/v1/collections/{name}/document/{docID}` - Get document
    /// - `PATCH /api/v1/collections/{name}/document/{docID}` - Update document
    /// - `DELETE /api/v1/collections/{name}/document/{docID}` - Delete document
    pub fn with_rest<R: RestOperations + 'static>(mut self, rest: R) -> Self {
        self.rest = Some(Arc::new(rest));
        self
    }

    /// Set REST operations from an Arc.
    pub fn with_rest_arc(mut self, rest: Arc<dyn RestOperations>) -> Self {
        self.rest = Some(rest);
        self
    }

    /// Set P2P operations for peer-to-peer networking endpoints.
    ///
    /// When P2P operations are configured, the server enables additional endpoints:
    /// - `GET /api/v0/p2p/info` - Get P2P node info (peer ID, addresses)
    /// - `GET /api/v0/p2p/shareable-address` - Get the single best shareable P2P address
    /// - `GET /api/v0/p2p/peers` - List connected peers
    /// - `POST /api/v0/p2p/peers` - Connect to a peer
    /// - And other P2P management endpoints
    pub fn with_p2p<P: P2POperations + 'static>(mut self, p2p: P) -> Self {
        self.p2p = Some(Arc::new(p2p));
        self
    }

    /// Set P2P operations from an Arc.
    pub fn with_p2p_arc(mut self, p2p: Arc<dyn P2POperations>) -> Self {
        self.p2p = Some(p2p);
        self
    }

    /// Set the outbound management requester from an Arc.
    ///
    /// When configured, the server can relay management requests to P2P-only
    /// peers on behalf of HTTP callers.
    pub fn with_manage_arc(mut self, manage: Arc<dyn ManageRequester>) -> Self {
        self.manage = Some(manage);
        self
    }

    /// Set ACP operations from an Arc.
    pub fn with_acp_arc(mut self, acp: Arc<dyn AcpOperations>) -> Self {
        self.acp = Some(acp);
        self
    }

    /// Set index operations from an Arc.
    pub fn with_index_arc(mut self, index: Arc<dyn IndexOperations>) -> Self {
        self.index = Some(index);
        self
    }

    /// Set encrypted index operations from an Arc.
    pub fn with_encrypted_index_arc(
        mut self,
        encrypted_index: Arc<dyn EncryptedIndexOperations>,
    ) -> Self {
        self.encrypted_index = Some(encrypted_index);
        self
    }

    /// Set backup operations from an Arc.
    pub fn with_backup_arc(mut self, backup: Arc<dyn BackupOperations>) -> Self {
        self.backup = Some(backup);
        self
    }

    /// Set block operations from an Arc.
    pub fn with_block_arc(mut self, block: Arc<dyn BlockOperations>) -> Self {
        self.block = Some(block);
        self
    }

    /// Set browser sync operations from an Arc.
    pub fn with_browser_sync_arc(mut self, browser_sync: Arc<dyn BrowserSyncOperations>) -> Self {
        self.browser_sync = Some(browser_sync);
        self
    }

    /// Set schema operations for schema management endpoints.
    ///
    /// When schema operations are configured, the server enables:
    /// - `POST /api/v0/schema` - Add schema from SDL
    pub fn with_schema<S: SchemaOperations + 'static>(mut self, schema: S) -> Self {
        self.schema = Some(Arc::new(schema));
        self
    }

    /// Set schema operations from an Arc.
    pub fn with_schema_arc(mut self, schema: Arc<dyn SchemaOperations>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set lens operations for schema migration endpoints.
    ///
    /// When lens operations are configured, the server enables:
    /// - `POST /api/v0/lens/set` - Set a migration between schema versions
    /// - `POST /api/v0/lens/reload` - Reload all lens modules
    pub fn with_lens<L: LensOperations + 'static>(mut self, lens: L) -> Self {
        self.lens = Some(Arc::new(lens));
        self
    }

    /// Set lens operations from an Arc.
    pub fn with_lens_arc(mut self, lens: Arc<dyn LensOperations>) -> Self {
        self.lens = Some(lens);
        self
    }

    /// Set NAC (Node Access Control) operations from an Arc.
    pub fn with_nac_arc(mut self, nac: Arc<dyn NodeAcpOperations>) -> Self {
        self.nac = Some(nac);
        self
    }

    /// Set document ACP operations from an Arc.
    pub fn with_doc_acp_arc(mut self, doc_acp: Arc<dyn DocumentAcpOperations>) -> Self {
        self.doc_acp = Some(doc_acp);
        self
    }

    /// Set collection management operations from an Arc.
    pub fn with_collection_mgmt_arc(
        mut self,
        collection_mgmt: Arc<dyn CollectionManagementOperations>,
    ) -> Self {
        self.collection_mgmt = Some(collection_mgmt);
        self
    }

    /// Set read-only collection-version operations from an Arc.
    pub fn with_collection_versions_arc(
        mut self,
        collection_versions: Arc<dyn CollectionVersionOperations>,
    ) -> Self {
        self.collection_versions = Some(collection_versions);
        self
    }

    /// Set view operations from an Arc.
    pub fn with_view_arc(mut self, view: Arc<dyn ViewOperations>) -> Self {
        self.view = Some(view);
        self
    }

    /// Set dump operations from an Arc.
    pub fn with_dump_arc(mut self, dump: Arc<dyn DumpOperations>) -> Self {
        self.dump = Some(dump);
        self
    }

    /// Set transaction operations from an Arc.
    pub fn with_txn_ops_arc(mut self, txn_ops: Arc<dyn TransactionOperations>) -> Self {
        self.txn_ops = Some(txn_ops);
        self
    }

    /// Set event bus for GraphQL subscriptions.
    ///
    /// When an event bus is configured, the server enables WebSocket
    /// subscriptions at `/api/v0/graphql/ws`. Without an event bus,
    /// subscription requests will receive an error.
    pub fn with_event_bus_arc(mut self, bus: Arc<dyn events::Bus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    /// Set the effective node configuration exposed by `/node/options`.
    pub fn with_node_options(mut self, options: Map<String, serde_json::Value>) -> Self {
        self.node_options = Some(Arc::new(options));
        self
    }

    /// Set the node identity DID for signing config fallback.
    pub fn with_node_identity_did(mut self, did: String) -> Self {
        self.node_identity_did = Some(did);
        self
    }

    /// Enable development mode (allows purge and other dev-only operations).
    pub fn with_dev_mode(mut self, dev_mode: bool) -> Self {
        self.dev_mode = dev_mode;
        self
    }

    /// Set GraphQL parsing and filter evaluation limits.
    pub fn with_query_limits(mut self, limits: QueryLimits) -> Self {
        self.config.query_limits = limits;
        self
    }

    /// Assemble the router's [`AppState`] from the configured components.
    ///
    /// Separated from [`Self::router`] so tests can observe what the server
    /// actually puts into state -- notably `signing_enabled`, whose whole
    /// defect class is a value computed correctly and never consumed.
    pub(crate) fn app_state(&self) -> AppState {
        // Build state with all configured components
        let mut builder = AppStateBuilder::new(Arc::clone(&self.executor));
        if let Some(ref rest) = self.rest {
            builder = builder.with_rest(Arc::clone(rest));
        }
        if let Some(ref p2p) = self.p2p {
            builder = builder.with_p2p(Arc::clone(p2p));
        }
        if let Some(ref manage) = self.manage {
            builder = builder.with_manage(Arc::clone(manage));
        }
        if let Some(ref acp) = self.acp {
            builder = builder.with_acp(Arc::clone(acp));
        }
        if let Some(ref index) = self.index {
            builder = builder.with_index(Arc::clone(index));
        }
        if let Some(ref encrypted_index) = self.encrypted_index {
            builder = builder.with_encrypted_index(Arc::clone(encrypted_index));
        }
        if let Some(ref backup) = self.backup {
            builder = builder.with_backup(Arc::clone(backup));
        }
        if let Some(ref block) = self.block {
            builder = builder.with_block(Arc::clone(block));
        }
        if let Some(ref browser_sync) = self.browser_sync {
            builder = builder.with_browser_sync(Arc::clone(browser_sync));
        }
        if let Some(ref schema) = self.schema {
            builder = builder.with_schema(Arc::clone(schema));
        }
        if let Some(ref lens) = self.lens {
            builder = builder.with_lens(Arc::clone(lens));
        }
        if let Some(ref nac) = self.nac {
            builder = builder.with_nac(Arc::clone(nac));
        }
        if let Some(ref collection_versions) = self.collection_versions {
            builder = builder.with_collection_versions(Arc::clone(collection_versions));
        }
        if let Some(ref collection_mgmt) = self.collection_mgmt {
            builder = builder.with_collection_mgmt(Arc::clone(collection_mgmt));
        }
        if let Some(ref doc_acp) = self.doc_acp {
            builder = builder.with_doc_acp(Arc::clone(doc_acp));
        }
        if let Some(ref view) = self.view {
            builder = builder.with_view(Arc::clone(view));
        }
        if let Some(ref dump) = self.dump {
            builder = builder.with_dump(Arc::clone(dump));
        }
        if let Some(ref txn_ops) = self.txn_ops {
            builder = builder.with_txn_ops(Arc::clone(txn_ops));
        }
        if let Some(ref event_bus) = self.event_bus {
            builder = builder.with_event_bus(Arc::clone(event_bus));
        }
        if let Some(ref options) = self.node_options {
            builder = builder.with_node_options((**options).clone());
        }
        if let Some(ref did) = self.node_identity_did {
            builder = builder.with_node_identity_did(did.clone());
            builder = builder.with_signing_enabled(self.signing_enabled());
        }
        builder = builder.with_dev_mode(self.dev_mode);
        builder = builder.with_max_txn_retries(self.config.max_txn_retries);
        builder = builder.with_query_limits(self.config.query_limits);
        builder.build()
    }

    /// Build the router with all routes and middleware.
    ///
    /// CORS configuration matches Go DefraDB behavior:
    /// - Empty origins = no CORS headers (browsers block cross-origin requests)
    /// - "*" in origins = allow all origins
    /// - Otherwise, case-insensitive matching against configured origins
    ///
    /// Returns an error if any configured CORS origins are invalid.
    pub fn router(&self) -> Result<Router> {
        let cors = self.build_cors_layer()?;
        let state = self.app_state();
        let state_for_middleware = state.clone();
        let browser_sync_body_limit = if self.config.max_body_size == 0 {
            defra_core::browser_sync::MAX_SYNC_BODY_BYTES
        } else {
            defra_core::browser_sync::MAX_SYNC_BODY_BYTES.min(self.config.max_body_size as usize)
        };
        let limits = BodyLimits {
            sync: browser_sync_body_limit,
            schema: self.route_body_limit(self.config.max_schema_size),
            backup_import: self.route_body_limit(self.config.max_backup_size),
        };
        let mut router = create_router_with_state_and_body_limits(state, limits);

        // Global auth middleware: enforces route-level permissions before handlers
        // run, and binds the caller's identity to the request task for DB-layer
        // NAC checks. Applied via route_layer so MatchedPath is available.
        router = router.route_layer(axum::middleware::from_fn_with_state(
            state_for_middleware,
            crate::auth_middleware::auth_middleware,
        ));

        // Apply global body limit (0 = unlimited)
        if self.config.max_body_size > 0 {
            router = router.layer(DefaultBodyLimit::max(self.config.max_body_size as usize));
        } else {
            router = router.layer(DefaultBodyLimit::disable());
        }

        // Apply concurrency limit (0 = unlimited)
        if self.config.max_concurrent_requests > 0 {
            router = router.layer(ConcurrencyLimitLayer::new(
                self.config.max_concurrent_requests,
            ));
        }

        // Apply request timeout (0 = no timeout)
        // HandleErrorLayer must wrap TimeoutLayer via ServiceBuilder so the
        // timeout error is converted to an HTTP status before reaching Router
        // (which requires Error: Into<Infallible>).
        if self.config.request_timeout > 0 {
            router = router.layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|err: axum::BoxError| async move {
                        if err.is::<tower::timeout::error::Elapsed>() {
                            StatusCode::REQUEST_TIMEOUT
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        }
                    }))
                    .layer(TimeoutLayer::new(Duration::from_secs(
                        self.config.request_timeout,
                    ))),
            );
        }

        router = router.layer(TraceLayer::new_for_http()).layer(cors);

        // Router layers run after route matching, so normalize misses through a
        // clone that already carries the complete auth and request middleware.
        let normalized = NormalizePathLayer::trim_trailing_slash().layer(router.clone());
        Ok(router.fallback_service(normalized))
    }

    /// Build CORS layer matching Go DefraDB behavior.
    fn build_cors_layer(&self) -> Result<CorsLayer> {
        if self.config.allowed_origins.is_empty() {
            // No origins configured = no CORS headers (matches Go DefraDB)
            return Ok(CorsLayer::new());
        }

        // Check for wildcard (matches Go DefraDB: if "*" in origins, allow all)
        let allow_any = self.config.allowed_origins.iter().any(|o| o == "*");

        // Build CORS layer with Go DefraDB settings
        let cors = CorsLayer::new()
            // Methods matching Go DefraDB: GET, HEAD, POST, PATCH, DELETE
            .allow_methods([
                Method::GET,
                Method::HEAD,
                Method::POST,
                Method::PATCH,
                Method::DELETE,
            ])
            // Headers matching Go DefraDB: Content-Type, Authorization
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
            // MaxAge matching Go DefraDB: 300 seconds
            .max_age(Duration::from_secs(300));

        if allow_any {
            Ok(cors.allow_origin(tower_http::cors::Any))
        } else {
            // Validate and convert all origins upfront - fail fast on invalid origins
            let valid_origins = self.validate_cors_origins()?;
            Ok(cors.allow_origin(valid_origins))
        }
    }

    /// Validate CORS origins and convert to HeaderValues.
    /// Fails fast if any origin is invalid rather than silently skipping.
    fn validate_cors_origins(&self) -> Result<Vec<HeaderValue>> {
        let mut valid_origins = Vec::new();
        let mut invalid_origins = Vec::new();

        for origin in &self.config.allowed_origins {
            // Skip wildcard (handled separately)
            if origin == "*" {
                continue;
            }
            // Lowercase for case-insensitive matching (matches Go DefraDB)
            let lower = origin.to_lowercase();
            match lower.parse::<HeaderValue>() {
                Ok(hv) => valid_origins.push(hv),
                Err(_) => invalid_origins.push(origin.clone()),
            }
        }

        if !invalid_origins.is_empty() {
            let msg = format!(
                "invalid CORS origins: {:?}. Origins must be valid HTTP header values.",
                invalid_origins
            );
            tracing::error!("{}", msg);
            return Err(crate::error::HttpError::BadRequest(msg));
        }

        Ok(valid_origins)
    }

    /// Run the server (blocks until shutdown).
    pub async fn run(self) -> Result<()> {
        let router = self.router()?;
        let listener = TcpListener::bind(self.config.address).await.map_err(|e| {
            let hint = match e.kind() {
                std::io::ErrorKind::AddrInUse => "port is already in use",
                std::io::ErrorKind::PermissionDenied => "permission denied (try port > 1024)",
                std::io::ErrorKind::AddrNotAvailable => "address not available on this host",
                _ => "check network configuration",
            };
            tracing::error!(
                address = %self.config.address,
                error = %e,
                hint = hint,
                "Failed to bind HTTP server"
            );
            crate::error::HttpError::Internal(format!(
                "failed to bind to {}: {} ({})",
                self.config.address, e, hint
            ))
        })?;

        let scheme = if self.tls.is_some() { "https" } else { "http" };
        tracing::info!("Providing HTTP API at {scheme}://{}", self.config.address);

        let result = match self.tls {
            Some(tls) => tls.serve(listener, router).await,
            None => axum::serve(listener, router).await,
        };
        result.map_err(|e| {
            tracing::error!(error = %e, "HTTP server encountered fatal error");
            crate::error::HttpError::Internal(format!("server error: {}", e))
        })?;

        Ok(())
    }

    /// Get the configured address.
    pub fn address(&self) -> SocketAddr {
        self.config.address
    }

    /// Whether commits should be signed.
    ///
    /// Signing requires a configured node identity, and `--no-signing` turns it
    /// off even when one is present. Matches Go, which keeps the identity for
    /// ACP purposes and gates only the signature
    /// (`cli/start.go:194`, `internal/db/db.go:164`).
    fn signing_enabled(&self) -> bool {
        self.node_identity_did.is_some() && !self.config.no_signing
    }

    /// Resolve a per-route body cap, clamped so it can never raise the
    /// effective cap above the global `max_body_size`.
    ///
    /// `0` means unlimited for both, matching the CLI flags' documented
    /// meaning: an unset route cap leaves the route bound only by the global
    /// limit, and an unset global limit leaves the route cap standing alone.
    fn route_body_limit(&self, route_limit: u64) -> Option<usize> {
        if route_limit == 0 {
            return None;
        }
        let effective = if self.config.max_body_size == 0 {
            route_limit
        } else {
            route_limit.min(self.config.max_body_size)
        };
        Some(effective as usize)
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
