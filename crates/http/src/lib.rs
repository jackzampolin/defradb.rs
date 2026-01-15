//! HTTP server for DefraDB.
//!
//! Provides an Axum-based HTTP API compatible with Go DefraDB's API structure.
//!
//! # Endpoints
//!
//! - `GET /health-check` - Health check
//! - `POST /api/v0/graphql` - Execute GraphQL queries
//! - `GET /api/v0/graphql` - Execute GraphQL queries (via query params)
//! - `GET /api/v0/schema` - Get GraphQL schema
//! - `GET /api/v0/version` - Get version info
//!
//! # Example
//!
//! ```ignore
//! use defra_http::Server;
//! use query::executor::QueryExecutor;
//!
//! #[tokio::main]
//! async fn main() {
//!     let executor = MyQueryExecutor::new();
//!     let server = Server::new(executor);
//!     server.run().await.unwrap();
//! }
//! ```

pub mod error;
pub mod handlers;
pub mod router;
pub mod server;

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

pub use error::{HttpError, Result};
pub use router::{create_router, AppState};
pub use server::{Server, ServerConfig};

#[cfg(any(test, feature = "test-utils"))]
pub use mock::MockQueryExecutor;
