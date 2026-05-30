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
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use query::executor::QueryExecutor;
use query::rest::RestOperations;
use query::QueryLimits;

use crate::error::Result;
use crate::router::{
    create_router_with_state, AcpOperations, AppStateBuilder, BackupOperations, BlockOperations,
    CollectionManagementOperations, DocumentAcpOperations, DumpOperations,
    EncryptedIndexOperations, IndexOperations, LensOperations, NodeAcpOperations, P2POperations,
    SchemaOperations, TransactionOperations, ViewOperations,
};

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
    pub max_backup_size: u64,
    /// Request timeout in seconds (0 = no timeout).
    pub request_timeout: u64,
    /// Max concurrent requests (0 = unlimited).
    pub max_concurrent_requests: usize,
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
            max_backup_size: 0,
            request_timeout: 300,
            max_concurrent_requests: 1000,
            query_limits: QueryLimits::default(),
        }
    }
}

/// HTTP server for DefraDB.
pub struct Server {
    config: ServerConfig,
    executor: Arc<dyn QueryExecutor>,
    rest: Option<Arc<dyn RestOperations>>,
    p2p: Option<Arc<dyn P2POperations>>,
    acp: Option<Arc<dyn AcpOperations>>,
    index: Option<Arc<dyn IndexOperations>>,
    encrypted_index: Option<Arc<dyn EncryptedIndexOperations>>,
    backup: Option<Arc<dyn BackupOperations>>,
    block: Option<Arc<dyn BlockOperations>>,
    schema: Option<Arc<dyn SchemaOperations>>,
    lens: Option<Arc<dyn LensOperations>>,
    nac: Option<Arc<dyn NodeAcpOperations>>,
    collection_mgmt: Option<Arc<dyn CollectionManagementOperations>>,
    doc_acp: Option<Arc<dyn DocumentAcpOperations>>,
    view: Option<Arc<dyn ViewOperations>>,
    dump: Option<Arc<dyn DumpOperations>>,
    txn_ops: Option<Arc<dyn TransactionOperations>>,
    event_bus: Option<Arc<dyn events::Bus>>,
    node_identity_did: Option<String>,
    dev_mode: bool,
}

impl Server {
    /// Create a new server with the given executor.
    pub fn new<E: QueryExecutor + 'static>(executor: E) -> Self {
        Self {
            config: ServerConfig::default(),
            executor: Arc::new(executor),
            rest: None,
            p2p: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            schema: None,
            lens: None,
            nac: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Create a server with custom configuration.
    pub fn with_config<E: QueryExecutor + 'static>(executor: E, config: ServerConfig) -> Self {
        Self {
            config,
            executor: Arc::new(executor),
            rest: None,
            p2p: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            schema: None,
            lens: None,
            nac: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Create a server from an Arc'd executor.
    pub fn from_arc(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            config: ServerConfig::default(),
            executor,
            rest: None,
            p2p: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            schema: None,
            lens: None,
            nac: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Create a server from an Arc'd executor with custom configuration.
    pub fn from_arc_with_config(executor: Arc<dyn QueryExecutor>, config: ServerConfig) -> Self {
        Self {
            config,
            executor,
            rest: None,
            p2p: None,
            acp: None,
            index: None,
            encrypted_index: None,
            backup: None,
            block: None,
            schema: None,
            lens: None,
            nac: None,
            collection_mgmt: None,
            doc_acp: None,
            view: None,
            dump: None,
            txn_ops: None,
            event_bus: None,
            node_identity_did: None,
            dev_mode: false,
        }
    }

    /// Set REST operations for collection/document endpoints.
    ///
    /// When REST operations are configured, the server enables additional endpoints:
    /// - `GET /api/v1/collections` - List all collections
    /// - `POST /api/v1/collections/{name}` - Create document(s)
    /// - `GET /api/v1/collections/{name}` - List collection document IDs
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

        // Build state with all configured components
        let mut builder = AppStateBuilder::new(Arc::clone(&self.executor));
        if let Some(ref rest) = self.rest {
            builder = builder.with_rest(Arc::clone(rest));
        }
        if let Some(ref p2p) = self.p2p {
            builder = builder.with_p2p(Arc::clone(p2p));
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
        if let Some(ref schema) = self.schema {
            builder = builder.with_schema(Arc::clone(schema));
        }
        if let Some(ref lens) = self.lens {
            builder = builder.with_lens(Arc::clone(lens));
        }
        if let Some(ref nac) = self.nac {
            builder = builder.with_nac(Arc::clone(nac));
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
        if let Some(ref did) = self.node_identity_did {
            builder = builder.with_node_identity_did(did.clone());
            builder = builder.with_signing_enabled(true);
        }
        builder = builder.with_dev_mode(self.dev_mode);
        builder = builder.with_query_limits(self.config.query_limits);
        let state = builder.build();
        let state_for_middleware = state.clone();
        let mut router = create_router_with_state(state);

        // Global auth middleware: enforces route-level permissions before handlers run.
        // Applied via route_layer so MatchedPath is available (routing has completed).
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

        Ok(router)
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

        tracing::info!("Providing HTTP API at http://{}", self.config.address);

        axum::serve(listener, router).await.map_err(|e| {
            tracing::error!(error = %e, "HTTP server encountered fatal error");
            crate::error::HttpError::Internal(format!("server error: {}", e))
        })?;

        Ok(())
    }

    /// Get the configured address.
    pub fn address(&self) -> SocketAddr {
        self.config.address
    }
}
