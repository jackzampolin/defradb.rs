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
//! ## P2P (requires P2POperations)
//! - `GET /api/v0/p2p/info` - Get P2P node info
//! - `GET /api/v0/p2p/peers` - List connected peers
//! - `POST /api/v0/p2p/peers` - Connect to peer
//! - `GET /api/v0/p2p/replicator` - List replicators
//! - `POST /api/v0/p2p/replicator` - Add replicator
//! - `DELETE /api/v0/p2p/replicator` - Remove replicator
//! - `GET /api/v0/p2p/collections` - List P2P collections
//! - `POST /api/v0/p2p/collections` - Add P2P collections
//! - `DELETE /api/v0/p2p/collections` - Remove P2P collections
//!
//! ## ACP (requires AcpOperations)
//! - `POST /api/v0/acp/policy` - Add policy
//! - `GET /api/v0/acp/policy` - List policies
//! - `GET /api/v0/acp/policy/{id}` - Get policy by ID
//!
//! ## Index (requires IndexOperations)
//! - `POST /api/v0/index` - Create index
//! - `GET /api/v0/index` - List indexes
//! - `DELETE /api/v0/index` - Drop index
//!
//! ## Backup (requires BackupOperations)
//! - `GET /api/v0/backup/export` - Export database
//! - `POST /api/v0/backup/import` - Import database
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
pub mod validation;

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

pub use error::{HttpError, Result};
pub use identity_extractor::{ExtractIdentity, ExtractTokenIdentity, IdentityExtractionError};
pub use router::{
    create_router, create_router_with_rest, create_router_with_state, AcpOperations, AppState,
    AppStateBuilder, BackupOperations, IndexFieldInfo, IndexInfo, IndexOperations, P2POperations,
    PolicyInfo, ReplicatorInfo,
};
pub use server::{Server, ServerConfig};

#[cfg(any(test, feature = "test-utils"))]
pub use mock::{
    FailingMockExecutor, FailingMockRestOperations, MockQueryExecutor, MockRestOperations,
};
