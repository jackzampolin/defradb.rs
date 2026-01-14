//! HTTP server configuration and startup.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use query::executor::QueryExecutor;

use crate::error::Result;
use crate::router::create_router;

/// Server configuration options.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind to (default: 127.0.0.1:9181).
    pub address: SocketAddr,
    /// Allowed CORS origins (empty vec = no cross-origin requests allowed).
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
    pub fn router(&self) -> Router {
        let cors = if self.config.allowed_origins.is_empty() {
            // No origins configured = no CORS (restrictive default)
            CorsLayer::new()
        } else {
            let mut valid_origins = Vec::new();
            for origin in &self.config.allowed_origins {
                match origin.parse() {
                    Ok(parsed) => valid_origins.push(parsed),
                    Err(e) => {
                        tracing::warn!(
                            origin = %origin,
                            error = %e,
                            "Invalid CORS origin in configuration, skipping"
                        );
                    }
                }
            }
            CorsLayer::new()
                .allow_origin(valid_origins)
                .allow_methods(Any)
                .allow_headers(Any)
        };

        create_router(Arc::clone(&self.executor))
            .layer(TraceLayer::new_for_http())
            .layer(cors)
    }

    /// Run the server (blocks until shutdown).
    pub async fn run(self) -> Result<()> {
        let router = self.router();
        let listener = TcpListener::bind(self.config.address)
            .await
            .map_err(|e| crate::error::HttpError::Internal(e.to_string()))?;

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
}
