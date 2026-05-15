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
//! The same routes are mounted under `/api/v0` for backwards compatibility.
//! - `GET /api/v1/p2p/info` - Get P2P node info (`handlers/p2p/peers.rs`)
//! - `GET /api/v1/p2p/shareable-address` - Get the single best shareable P2P address (`handlers/p2p/peers.rs`)
//! - `GET /api/v1/p2p/active-peers` - List connected peers in Go-compatible format (`handlers/p2p/peers.rs`)
//! - `POST /api/v1/p2p/connect` - Connect to peers in Go-compatible format (`handlers/p2p/peers.rs`)
//! - `GET /api/v1/p2p/peers` - List connected peers (`handlers/p2p/peers.rs`)
//! - `POST /api/v1/p2p/peers` - Connect to peer (`handlers/p2p/peers.rs`)
//! - `GET /api/v1/p2p/replicators` - List replicators (`handlers/p2p/replicators.rs`)
//! - `POST /api/v1/p2p/replicators` - Add replicator (`handlers/p2p/replicators.rs`)
//! - `DELETE /api/v1/p2p/replicators` - Remove replicator (`handlers/p2p/replicators.rs`)
//! - `GET /api/v1/p2p/replicator` - Legacy alias for listing replicators (`handlers/p2p/replicators.rs`)
//! - `POST /api/v1/p2p/replicator` - Legacy alias for adding a replicator (`handlers/p2p/replicators.rs`)
//! - `DELETE /api/v1/p2p/replicator` - Legacy alias for removing a replicator (`handlers/p2p/replicators.rs`)
//! - `GET /api/v1/p2p/collections` - List P2P collections (`handlers/p2p/collections.rs`)
//! - `POST /api/v1/p2p/collections` - Add P2P collections (`handlers/p2p/collections.rs`)
//! - `DELETE /api/v1/p2p/collections` - Remove P2P collections (`handlers/p2p/collections.rs`)
//! - `POST /api/v1/p2p/collections/sync-versions` - Sync collection versions (`handlers/p2p/collections.rs`)
//! - `POST /api/v1/p2p/collections/sync-branchable` - Sync branchable collection heads (`handlers/p2p/collections.rs`)
//! - `GET /api/v1/p2p/documents` - List P2P documents (`handlers/p2p/documents.rs`)
//! - `POST /api/v1/p2p/documents` - Add P2P documents (`handlers/p2p/documents.rs`)
//! - `DELETE /api/v1/p2p/documents` - Remove P2P documents (`handlers/p2p/documents.rs`)
//! - `POST /api/v1/p2p/documents/sync` - Sync specific documents (`handlers/p2p/documents.rs`)
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
