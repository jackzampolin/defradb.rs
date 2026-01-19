//! HTTP server for DefraDB.
//!
//! Provides an Axum-based HTTP API compatible with Go DefraDB's API structure.
//!
//! # Endpoints
//!
//! ## Core
//! - `GET /health-check` - Health check
//! - `GET /api/v0/version` - Get version info
//!
//! ## GraphQL
//! - `POST /api/v0/graphql` - Execute GraphQL queries
//! - `GET /api/v0/graphql` - Execute GraphQL queries (via query params)
//! - `GET /api/v0/schema` - Get GraphQL schema
//!
//! ## Transactions
//! - `POST /api/v0/tx/begin` - Begin a transaction
//! - `POST /api/v0/tx/commit` - Commit a transaction
//! - `POST /api/v0/tx/rollback` - Rollback a transaction
//!
//! ## REST Collections (requires RestOperations)
//! - `GET /api/v0/collections` - List all collections
//! - `GET /api/v0/collections/{name}` - Get document IDs in collection
//! - `POST /api/v0/collections/{name}` - Create document(s)
//! - `GET /api/v0/collections/{name}/{docID}` - Get document
//! - `PATCH /api/v0/collections/{name}/{docID}` - Update document
//! - `DELETE /api/v0/collections/{name}/{docID}` - Delete document
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
pub mod identity_extractor;
pub mod router;
pub mod server;

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

pub use error::{HttpError, Result};
pub use identity_extractor::{ExtractIdentity, ExtractTokenIdentity, IdentityExtractionError};
pub use router::{create_router, create_router_with_rest, AppState};
pub use server::{Server, ServerConfig};

#[cfg(any(test, feature = "test-utils"))]
pub use mock::{
    FailingMockExecutor, FailingMockRestOperations, MockQueryExecutor, MockRestOperations,
};
