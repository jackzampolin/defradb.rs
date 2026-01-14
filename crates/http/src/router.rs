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

/// Create the main router with all routes.
pub fn create_router(executor: Arc<dyn QueryExecutor>) -> Router {
    let state = AppState { executor };

    // Health check at root level (matches Go DefraDB)
    let root_routes = Router::new().route("/health-check", get(handlers::health_check));

    // API v0 routes
    let api_routes = Router::new()
        .route("/graphql", post(handlers::graphql))
        .route("/graphql", get(handlers::graphql_get))
        .route("/schema", get(handlers::schema))
        .route("/version", get(handlers::version))
        .with_state(state);

    root_routes.nest("/api/v0", api_routes)
}
