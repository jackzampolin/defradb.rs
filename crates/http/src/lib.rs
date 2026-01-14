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
//! use defra_http::{Server, MockQueryExecutor};
//!
//! #[tokio::main]
//! async fn main() {
//!     let executor = MockQueryExecutor::new();
//!     let server = Server::new(executor);
//!     server.run().await.unwrap();
//! }
//! ```

pub mod error;
pub mod handlers;
pub mod mock;
pub mod router;
pub mod server;

pub use error::{HttpError, Result};
pub use mock::MockQueryExecutor;
pub use router::{create_router, AppState};
pub use server::{Server, ServerConfig};
