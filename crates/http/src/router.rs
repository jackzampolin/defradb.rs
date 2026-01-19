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
