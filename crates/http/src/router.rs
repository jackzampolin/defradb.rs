//! Router configuration and route definitions.

use std::sync::Arc;

use axum::{
    routing::{delete, get, patch, post},
    Router,
};

use query::executor::QueryExecutor;
use query::rest::RestOperations;

use crate::handlers;

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<dyn QueryExecutor>,
    pub rest: Option<Arc<dyn RestOperations>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("executor", &"<QueryExecutor>")
            .field("rest", &self.rest.as_ref().map(|_| "<RestOperations>"))
            .finish()
    }
}

/// Create the main router with all routes.
///
/// This creates a router with GraphQL endpoints only (no REST).
/// Use `create_router_with_rest` to include REST endpoints.
pub fn create_router(executor: Arc<dyn QueryExecutor>) -> Router {
    create_router_internal(executor, None)
}

/// Create the main router with all routes including REST endpoints.
pub fn create_router_with_rest(
    executor: Arc<dyn QueryExecutor>,
    rest: Arc<dyn RestOperations>,
) -> Router {
    create_router_internal(executor, Some(rest))
}

fn create_router_internal(
    executor: Arc<dyn QueryExecutor>,
    rest: Option<Arc<dyn RestOperations>>,
) -> Router {
    let state = AppState { executor, rest };

    // Health check at root level (matches Go DefraDB)
    let root_routes = Router::new().route("/health-check", get(handlers::health_check));

    // Transaction routes
    let tx_routes = Router::new()
        .route("/begin", post(handlers::tx_begin))
        .route("/commit", post(handlers::tx_commit))
        .route("/rollback", post(handlers::tx_rollback));

    // Collection routes (REST API)
    let collection_routes = Router::new()
        .route("/", get(handlers::list_collections))
        .route("/:name", get(handlers::get_collection_doc_ids))
        .route("/:name", post(handlers::create_document))
        .route("/:name/:docID", get(handlers::get_document))
        .route("/:name/:docID", patch(handlers::update_document))
        .route("/:name/:docID", delete(handlers::delete_document));

    // API v0 routes
    let api_routes = Router::new()
        // GraphQL endpoints
        .route("/graphql", post(handlers::graphql_transactional))
        .route("/graphql", get(handlers::graphql_get))
        .route("/schema", get(handlers::schema))
        .route("/version", get(handlers::version))
        // Transaction endpoints
        .nest("/tx", tx_routes)
        // REST collection endpoints
        .nest("/collections", collection_routes)
        .with_state(state);

    root_routes.nest("/api/v0", api_routes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{MockQueryExecutor, MockRestOperations};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_check_route() {
        let executor = Arc::new(MockQueryExecutor::new());
        let router = create_router(executor);

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
        let executor = Arc::new(MockQueryExecutor::new());
        let router = create_router(executor);

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
    async fn test_collections_route_without_rest() {
        let executor = Arc::new(MockQueryExecutor::new());
        let router = create_router(executor);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return 500 because REST operations are not configured
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_collections_route_with_rest() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_document_route() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections/Users/bae-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_document_route() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v0/collections/Users")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name": "Charlie", "age": 35}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_update_document_route() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v0/collections/Users/bae-123")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"age": 31}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_document_route() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v0/collections/Users/bae-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_document_not_found() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections/Users/bae-nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_collection_not_found() {
        let executor = Arc::new(MockQueryExecutor::new());
        let rest = Arc::new(MockRestOperations::new());
        let router = create_router_with_rest(executor, rest);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v0/collections/NonExistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
