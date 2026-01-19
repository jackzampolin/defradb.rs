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
use crate::router::{create_router, create_router_with_rest};

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
}

impl Server {
    /// Create a new server with the given executor.
    pub fn new<E: QueryExecutor + 'static>(executor: E) -> Self {
        Self {
            config: ServerConfig::default(),
            executor: Arc::new(executor),
            rest: None,
        }
    }

    /// Create a server with custom configuration.
    pub fn with_config<E: QueryExecutor + 'static>(executor: E, config: ServerConfig) -> Self {
        Self {
            config,
            executor: Arc::new(executor),
            rest: None,
        }
    }

    /// Create a server from an Arc'd executor.
    pub fn from_arc(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            config: ServerConfig::default(),
            executor,
            rest: None,
        }
    }

    /// Create a server from an Arc'd executor with custom configuration.
    pub fn from_arc_with_config(executor: Arc<dyn QueryExecutor>, config: ServerConfig) -> Self {
        Self {
            config,
            executor,
            rest: None,
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

        let router = match &self.rest {
            Some(rest) => create_router_with_rest(Arc::clone(&self.executor), Arc::clone(rest)),
            None => create_router(Arc::clone(&self.executor)),
        };

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
