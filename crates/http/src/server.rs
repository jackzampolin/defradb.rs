//! HTTP server configuration and startup.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderValue, Method};
use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use query::executor::QueryExecutor;
use query::rest::RestOperations;

use crate::error::Result;
use crate::router::{AppStateBuilder, P2POperations, SchemaOperations, create_router_with_state};

/// Server configuration options.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind to (default: 127.0.0.1:9181).
    pub address: SocketAddr,
    /// Allowed CORS origins. Supports "*" for all origins (matches Go DefraDB).
    /// Empty vec = no CORS headers (browsers block cross-origin requests).
    pub allowed_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::from(([127, 0, 0, 1], 9181)),
            allowed_origins: Vec::new(),
        }
    }
}

/// HTTP server for DefraDB.
pub struct Server {
    config: ServerConfig,
    executor: Arc<dyn QueryExecutor>,
    rest: Option<Arc<dyn RestOperations>>,
    p2p: Option<Arc<dyn P2POperations>>,
    schema: Option<Arc<dyn SchemaOperations>>,
}

impl Server {
    /// Create a new server with the given executor.
    pub fn new<E: QueryExecutor + 'static>(executor: E) -> Self {
        Self {
            config: ServerConfig::default(),
            executor: Arc::new(executor),
            rest: None,
            p2p: None,
            schema: None,
        }
    }

    /// Create a server with custom configuration.
    pub fn with_config<E: QueryExecutor + 'static>(executor: E, config: ServerConfig) -> Self {
        Self {
            config,
            executor: Arc::new(executor),
            rest: None,
            p2p: None,
            schema: None,
        }
    }

    /// Create a server from an Arc'd executor.
    pub fn from_arc(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            config: ServerConfig::default(),
            executor,
            rest: None,
            p2p: None,
            schema: None,
        }
    }

    /// Create a server from an Arc'd executor with custom configuration.
    pub fn from_arc_with_config(executor: Arc<dyn QueryExecutor>, config: ServerConfig) -> Self {
        Self {
            config,
            executor,
            rest: None,
            p2p: None,
            schema: None,
        }
    }

    /// Set REST operations for collection/document endpoints.
    ///
    /// When REST operations are configured, the server enables additional endpoints:
    /// - `GET /api/v0/collections` - List all collections
    /// - `GET /api/v0/collections/{name}` - Get document IDs in collection
    /// - `POST /api/v0/collections/{name}` - Create document(s)
    /// - `GET /api/v0/collections/{name}/{docID}` - Get document
    /// - `PATCH /api/v0/collections/{name}/{docID}` - Update document
    /// - `DELETE /api/v0/collections/{name}/{docID}` - Delete document
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
        if let Some(ref schema) = self.schema {
            builder = builder.with_schema(Arc::clone(schema));
        }
        let state = builder.build();
        let router = create_router_with_state(state);

        Ok(router.layer(TraceLayer::new_for_http()).layer(cors))
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

        tracing::info!("DefraDB HTTP server listening on {}", self.config.address);

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
