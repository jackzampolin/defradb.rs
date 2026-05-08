//! HTTP server for DefraDB.
//!
//! Provides an Axum-based HTTP API compatible with Go DefraDB's API structure.
//! The current API is mounted under `/api/v1`; `/api/v0` remains available for
//! backwards compatibility.
//!
//! # Endpoints
//!
//! ## Core
//! - `GET /health-check` - Health check
//! - `GET /api/v1/version` - Get version info
//!
//! ## GraphQL
//! - `POST /api/v1/graphql` - Execute GraphQL queries
//! - `GET /api/v1/graphql` - Execute GraphQL queries (via query params)
//! - `GET /api/v1/schema` - Get GraphQL schema
//!
//! ## Transactions
//! - `POST /api/v1/tx` - Begin a transaction
//! - `POST /api/v1/tx/concurrent` - Begin a concurrent transaction
//! - `POST /api/v1/tx/{id}` - Commit a transaction
//! - `DELETE /api/v1/tx/{id}` - Discard a transaction
//!
//! ## REST Collections (requires RestOperations)
//! - `GET /api/v1/collections` - List all collections
//! - `POST /api/v1/collections/{name}` - Create document(s)
//! - `GET /api/v1/collections/{name}/document/{docID}` - Get document
//! - `PATCH /api/v1/collections/{name}/document/{docID}` - Update document
//! - `DELETE /api/v1/collections/{name}/document/{docID}` - Delete document
//!
//! ## P2P (requires P2POperations)
//! - `GET /api/v1/p2p/info` - Get P2P node info
//! - `GET /api/v1/p2p/shareable-address` - Get the single best shareable P2P address
//! - `GET /api/v1/p2p/peers` - List connected peers
//! - `POST /api/v1/p2p/peers` - Connect to peer
//! - `GET /api/v1/p2p/replicator` - List replicators
//! - `POST /api/v1/p2p/replicator` - Add replicator
//! - `DELETE /api/v1/p2p/replicator` - Remove replicator
//! - `GET /api/v1/p2p/collections` - List P2P collections
//! - `POST /api/v1/p2p/collections` - Add P2P collections
//! - `DELETE /api/v1/p2p/collections` - Remove P2P collections
//!
//! ## ACP (requires AcpOperations)
//! - `POST /api/v1/acp/policy` - Add policy
//! - `GET /api/v1/acp/policy` - List policies
//! - `GET /api/v1/acp/policy/{id}` - Get policy by ID
//!
//! ## Index (requires IndexOperations)
//! - `POST /api/v1/index` - Create index
//! - `GET /api/v1/index` - List indexes
//! - `DELETE /api/v1/index` - Drop index
//!
//! ## Backup (requires BackupOperations)
//! - `POST /api/v1/backup/export` - Export database
//! - `POST /api/v1/backup/import` - Import database
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

pub mod auth_error;
pub mod auth_middleware;
pub mod error;
pub mod handlers;
pub mod identity_extractor;
pub mod nac_guard;
pub mod query_context;
pub mod route_permissions;
pub mod router;
pub mod server;
pub mod validation;

#[cfg(any(test, feature = "test-utils"))]
pub mod mock;

pub use error::{HttpError, Result};
pub use identity_extractor::{ExtractIdentity, ExtractTokenIdentity, IdentityExtractionError};
pub use router::{
    create_router, create_router_with_rest, create_router_with_state, AcpLightClientStatus,
    AcpOperations, AppState, AppStateBuilder, BackupOperations, BlockOperations,
    DocumentAcpOperations, IndexFieldInfo, IndexInfo, IndexOperations, NacStatus, NacStatusInfo,
    NodeAcpOperations, NodePermission, P2PError, P2POperations, P2PResult, PolicyInfo,
    ReplicatorInfo, TransactionOperations, ViewOperations,
};
pub use server::{Server, ServerConfig};

#[cfg(any(test, feature = "test-utils"))]
pub use mock::{
    FailingMockAcpOperations, FailingMockBackupOperations, FailingMockExecutor,
    FailingMockIndexOperations, FailingMockNodeAcpOperations, FailingMockP2POperations,
    FailingMockRestOperations, MockAcpOperations, MockBackupOperations, MockBlockOperations,
    MockCollectionManagementOperations, MockDocumentAcpOperations, MockEncryptedIndexOperations,
    MockIndexOperations, MockLensOperations, MockNodeAcpOperations, MockP2POperations,
    MockQueryExecutor, MockRestOperations, MockTransactionOperations,
};
