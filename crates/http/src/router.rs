//! Router configuration and route definitions.

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use query::executor::QueryExecutor;

use crate::handlers;

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<dyn QueryExecutor>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("executor", &"<QueryExecutor>")
            .finish()
    }
}

/// Create the main router with all routes.
pub fn create_router(executor: Arc<dyn QueryExecutor>) -> Router {
    let state = AppState { executor };

    // Health check at root level (matches Go DefraDB)
    let root_routes = Router::new().route("/health-check", get(handlers::health_check));

    // Transaction routes
    let tx_routes = Router::new()
        .route("/begin", post(handlers::tx_begin))
        .route("/commit", post(handlers::tx_commit))
        .route("/rollback", post(handlers::tx_rollback));

    // API v0 routes
    let api_routes = Router::new()
        // Use the transactional handler which supports optional txn_id
        .route("/graphql", post(handlers::graphql_transactional))
        .route("/graphql", get(handlers::graphql_get))
        .route("/schema", get(handlers::schema))
        .route("/version", get(handlers::version))
        .nest("/tx", tx_routes)
        .with_state(state);

    root_routes.nest("/api/v0", api_routes)
}
