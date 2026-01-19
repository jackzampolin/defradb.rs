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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use serde_json::json;
    use tower::util::ServiceExt;

    use crate::mock::{FailingMockExecutor, MockQueryExecutor};

    fn test_server() -> Server {
        Server::new(MockQueryExecutor::new())
    }

    #[tokio::test]
    async fn test_health_check_route() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health-check")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_version_route() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_graphql_post_route() {
        let router = test_server().router().unwrap();
        let body = json!({"query": "{ users { name } }"}).to_string();

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v0/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_graphql_post_invalid_json() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v0/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from("{invalid json}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Axum returns 400 Bad Request for JSON parse errors
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_graphql_get_route() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/graphql?query=%7B%20users%20%7B%20name%20%7D%20%7D")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_graphql_get_with_variables() {
        let router = test_server().router().unwrap();
        let vars = urlencoding::encode(r#"{"limit":10}"#);

        let response = router
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/api/v0/graphql?query=%7B%20users%20%7D&variables={}",
                        vars
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_graphql_get_invalid_variables() {
        let router = test_server().router().unwrap();
        let invalid_vars = urlencoding::encode("{invalid}");

        let response = router
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/api/v0/graphql?query=%7B%20users%20%7D&variables={}",
                        invalid_vars
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should still return 200 but with error in body
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_route() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_not_found_route() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.address.port(), 9181);
        assert!(config.allowed_origins.is_empty());
    }

    #[tokio::test]
    async fn test_graphql_post_empty_query() {
        let router = test_server().router().unwrap();
        let body = json!({"query": ""}).to_string();

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v0/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Empty query should still be accepted (executor handles validation)
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_graphql_get_missing_query_param() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/graphql")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Missing required 'query' param returns 400
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_cors_with_allowed_origin() {
        let config = ServerConfig {
            address: SocketAddr::from(([127, 0, 0, 1], 0)),
            allowed_origins: vec!["http://localhost:3000".to_string()],
        };
        let server = Server::with_config(MockQueryExecutor::new(), config);
        let router = server.router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v0/graphql")
                    .header("Origin", "http://localhost:3000")
                    .header("Access-Control-Request-Method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Preflight should succeed with CORS headers
        assert!(response
            .headers()
            .contains_key("access-control-allow-origin"));
        assert!(response
            .headers()
            .contains_key("access-control-allow-methods"));
    }

    #[tokio::test]
    async fn test_cors_wildcard() {
        let config = ServerConfig {
            address: SocketAddr::from(([127, 0, 0, 1], 0)),
            allowed_origins: vec!["*".to_string()],
        };
        let server = Server::with_config(MockQueryExecutor::new(), config);
        let router = server.router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v0/graphql")
                    .header("Origin", "http://any-origin.com")
                    .header("Access-Control-Request-Method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Wildcard allows any origin
        assert!(response
            .headers()
            .contains_key("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn test_cors_case_insensitive() {
        let config = ServerConfig {
            address: SocketAddr::from(([127, 0, 0, 1], 0)),
            allowed_origins: vec!["http://LOCALHOST:3000".to_string()],
        };
        let server = Server::with_config(MockQueryExecutor::new(), config);
        let router = server.router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v0/graphql")
                    .header("Origin", "http://localhost:3000")
                    .header("Access-Control-Request-Method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Case-insensitive matching (matches Go DefraDB)
        assert!(response
            .headers()
            .contains_key("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn test_cors_invalid_origin_fails_fast() {
        let config = ServerConfig {
            address: SocketAddr::from(([127, 0, 0, 1], 0)),
            // Non-ASCII characters are invalid in HTTP header values
            allowed_origins: vec![
                "http://localhost:3000".to_string(),
                "http://invalid\x00origin".to_string(),
            ],
        };
        let server = Server::with_config(MockQueryExecutor::new(), config);

        // router() should return an error for invalid origins
        let result = server.router();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("invalid CORS origins"));
    }

    #[tokio::test]
    async fn test_graphql_post_returns_errors_in_body() {
        let server = Server::new(FailingMockExecutor::with_schema_error("ignored"));
        let router = server.router().unwrap();
        let body = json!({"query": "{ users { name } }"}).to_string();

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v0/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // GraphQL spec: errors return 200 OK with errors in body
        assert_eq!(response.status(), StatusCode::OK);

        // Verify response body contains errors
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body_bytes);
        assert!(
            body_str.contains("errors"),
            "Response should contain errors: {}",
            body_str
        );
    }

    #[tokio::test]
    async fn test_graphql_post_response_body_structure() {
        let router = test_server().router().unwrap();
        let body = json!({"query": "{ users { name } }"}).to_string();

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v0/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify response body has correct structure
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(
            json.get("data").is_some(),
            "Response should contain 'data' field"
        );
        assert!(
            json.get("data").unwrap().get("users").is_some(),
            "Response should contain 'users' in data"
        );
    }

    #[tokio::test]
    async fn test_version_response_body() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify version response has required fields
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(
            json.get("version").is_some(),
            "Response should contain 'version' field"
        );
        assert!(
            json.get("commit").is_some(),
            "Response should contain 'commit' field"
        );
    }

    #[tokio::test]
    async fn test_schema_response_body() {
        let router = test_server().router().unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify schema response contains SDL content
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body_bytes);
        assert!(
            body_str.contains("type User"),
            "Schema should contain User type"
        );
        assert!(
            body_str.contains("type Query"),
            "Schema should contain Query type"
        );
    }

    #[tokio::test]
    async fn test_server_with_rest_operations() {
        use crate::mock::MockRestOperations;

        let server = Server::new(MockQueryExecutor::new()).with_rest(MockRestOperations::new());
        let router = server.router().unwrap();

        // Test that REST endpoints work when REST operations are configured
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify response body
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(
            json.get("collections").is_some(),
            "Response should contain 'collections' field"
        );
    }

    #[tokio::test]
    async fn test_server_without_rest_returns_error_for_collections() {
        let server = Server::new(MockQueryExecutor::new());
        let router = server.router().unwrap();

        // Without REST operations, collections endpoint should return 500
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
