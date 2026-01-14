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

use crate::error::Result;
use crate::router::create_router;

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
}

impl Server {
    /// Create a new server with the given executor.
    pub fn new<E: QueryExecutor + 'static>(executor: E) -> Self {
        Self {
            config: ServerConfig::default(),
            executor: Arc::new(executor),
        }
    }

    /// Create a server with custom configuration.
    pub fn with_config<E: QueryExecutor + 'static>(executor: E, config: ServerConfig) -> Self {
        Self {
            config,
            executor: Arc::new(executor),
        }
    }

    /// Create a server from an Arc'd executor.
    pub fn from_arc(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            config: ServerConfig::default(),
            executor,
        }
    }

    /// Build the router with all routes and middleware.
    ///
    /// CORS configuration matches Go DefraDB behavior:
    /// - Empty origins = no CORS headers (browsers block cross-origin requests)
    /// - "*" in origins = allow all origins
    /// - Otherwise, case-insensitive matching against configured origins
    pub fn router(&self) -> Router {
        let cors = self.build_cors_layer();

        create_router(Arc::clone(&self.executor))
            .layer(TraceLayer::new_for_http())
            .layer(cors)
    }

    /// Build CORS layer matching Go DefraDB behavior.
    fn build_cors_layer(&self) -> CorsLayer {
        if self.config.allowed_origins.is_empty() {
            // No origins configured = no CORS headers (matches Go DefraDB)
            return CorsLayer::new();
        }

        // Check for wildcard (matches Go DefraDB: if "*" in origins, allow all)
        let allow_any = self.config.allowed_origins.iter().any(|o| o == "*");

        // Convert origins to lowercase for case-insensitive matching (matches Go DefraDB)
        let allowed_lower: Vec<String> = self
            .config
            .allowed_origins
            .iter()
            .map(|o| o.to_lowercase())
            .collect();

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
            cors.allow_origin(tower_http::cors::Any)
        } else {
            cors.allow_origin(
                allowed_lower
                    .into_iter()
                    .filter_map(|origin| {
                        origin.parse::<HeaderValue>().ok().or_else(|| {
                            tracing::warn!(origin = %origin, "Invalid CORS origin, skipping");
                            None
                        })
                    })
                    .collect::<Vec<_>>(),
            )
        }
    }

    /// Run the server (blocks until shutdown).
    pub async fn run(self) -> Result<()> {
        let router = self.router();
        let listener = TcpListener::bind(self.config.address).await.map_err(|e| {
            tracing::error!(
                address = %self.config.address,
                error = %e,
                "Failed to bind HTTP server"
            );
            crate::error::HttpError::Internal(format!(
                "failed to bind to {}: {} (check if port is in use)",
                self.config.address, e
            ))
        })?;

        tracing::info!("DefraDB HTTP server listening on {}", self.config.address);

        axum::serve(listener, router)
            .await
            .map_err(|e| crate::error::HttpError::Internal(e.to_string()))?;

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

    use crate::mock::MockQueryExecutor;

    fn test_server() -> Server {
        Server::new(MockQueryExecutor::new())
    }

    #[tokio::test]
    async fn test_health_check_route() {
        let router = test_server().router();

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
        let router = test_server().router();

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
        let router = test_server().router();
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
        let router = test_server().router();

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
        let router = test_server().router();

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
        let router = test_server().router();
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
        let router = test_server().router();
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
        let router = test_server().router();

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
        let router = test_server().router();

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
        let router = test_server().router();
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
        let router = test_server().router();

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
        let router = server.router();

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
        let router = server.router();

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
        let router = server.router();

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
}
