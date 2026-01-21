//! Router configuration and route definitions.

use std::sync::Arc;

use axum::{
    routing::{delete, get, patch, post},
    Router,
};

use query::executor::QueryExecutor;
use query::rest::RestOperations;

use crate::handlers;

/// Trait for P2P operations that can be accessed via HTTP.
///
/// Abstracts P2P host functionality to decouple HTTP handlers from the
/// actual P2P implementation, enabling both dependency injection and testing.
///
/// All methods return `Result<T, String>` where the error string should be
/// a user-friendly message. For validation failures, use descriptive messages
/// like "invalid address format". For internal errors, use messages like
/// "failed to connect: <reason>".
#[async_trait::async_trait]
pub trait P2POperations: Send + Sync {
    /// Get the local peer ID.
    async fn local_peer_id(&self) -> Result<String, String>;

    /// Get listening addresses.
    async fn listen_addresses(&self) -> Result<Vec<String>, String>;

    /// Get connected peers.
    async fn connected_peers(&self) -> Result<Vec<String>, String>;

    /// Connect to a peer at the given address.
    async fn connect_peer(&self, addr: &str) -> Result<(), String>;

    /// Get all replicators.
    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String>;

    /// Add a replicator for collections.
    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String>;

    /// Remove a replicator for collections.
    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String>;

    /// Get P2P collections.
    async fn get_collections(&self) -> Result<Vec<String>, String>;

    /// Add collections to P2P.
    async fn add_collections(&self, collections: Vec<String>) -> Result<(), String>;

    /// Remove collections from P2P.
    async fn remove_collections(&self, collections: Vec<String>) -> Result<(), String>;
}

/// Replicator information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicatorInfo {
    pub id: Option<String>,
    pub collections: Vec<String>,
    pub address: Option<String>,
}

/// Trait for ACP (Access Control Policy) operations.
///
/// ACP policies define access permissions for collections and documents,
/// determining which identities can read, write, or manage data.
///
/// Policies should be provided in YAML or JSON format following the ACP
/// policy specification.
#[async_trait::async_trait]
pub trait AcpOperations: Send + Sync {
    /// Add a new policy. Returns the policy ID on success.
    ///
    /// The policy should be valid YAML or JSON. Returns an error string
    /// if the policy is malformed or cannot be added.
    async fn add_policy(&self, policy: &str) -> Result<String, String>;

    /// List all policies.
    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String>;

    /// Get a policy by ID.
    ///
    /// Returns `Ok(None)` if the policy doesn't exist, `Ok(Some(info))` if found,
    /// or `Err(message)` on internal errors.
    async fn get_policy(&self, id: &str) -> Result<Option<PolicyInfo>, String>;
}

/// Policy information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PolicyInfo {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_time: Option<String>,
}

/// Trait for index operations.
///
/// Indexes improve query performance for specific fields. Creating unique
/// indexes also enforces uniqueness constraints on the indexed fields.
#[async_trait::async_trait]
pub trait IndexOperations: Send + Sync {
    /// Create an index on a collection. Returns the created index info.
    ///
    /// If `name` is `None`, an index name will be auto-generated.
    /// The `unique` flag enforces uniqueness constraints on the indexed fields.
    async fn create_index(
        &self,
        collection: &str,
        fields: Vec<String>,
        name: Option<&str>,
        unique: bool,
    ) -> Result<IndexInfo, String>;

    /// List indexes, optionally filtered by collection.
    ///
    /// If `collection` is `None`, returns indexes from all collections.
    async fn list_indexes(&self, collection: Option<&str>) -> Result<Vec<IndexInfo>, String>;

    /// Drop an index by collection and name.
    async fn drop_index(&self, collection: &str, name: &str) -> Result<(), String>;
}

/// Index information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub collection: String,
    pub fields: Vec<IndexFieldInfo>,
    #[serde(default)]
    pub unique: bool,
}

/// Index field information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexFieldInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
}

/// Result of a backup import operation.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ImportResult {
    /// Number of documents successfully imported.
    pub documents_imported: u64,
    /// Number of documents skipped (e.g., duplicates).
    pub documents_skipped: u64,
    /// Collections that were affected by the import.
    pub collections_affected: Vec<String>,
    /// Errors encountered during import (non-fatal).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

/// Trait for schema operations.
///
/// Enables adding and managing collection schemas via SDL.
#[async_trait::async_trait]
pub trait SchemaOperations: Send + Sync {
    /// Add a schema from SDL string.
    ///
    /// Parses the SDL and creates collections for each type defined.
    /// Returns the created collection versions.
    async fn add_schema(&self, sdl: &str) -> Result<Vec<schema::CollectionVersion>, String>;
}

/// Trait for backup operations.
///
/// Enables exporting and importing database state as JSON. Export produces
/// a JSON representation of documents that can be reimported to restore state.
///
/// For export, the JSON includes document metadata and relationships.
/// For import, the JSON must match the expected structure with valid document
/// IDs and collection references.
#[async_trait::async_trait]
pub trait BackupOperations: Send + Sync {
    /// Export database to JSON.
    ///
    /// If `collections` is `None`, exports all collections.
    /// If `pretty` is true, the JSON output is formatted with indentation.
    async fn export(
        &self,
        collections: Option<Vec<String>>,
        pretty: bool,
    ) -> Result<String, String>;

    /// Import database from JSON.
    ///
    /// The `data` parameter should be valid JSON matching the export format.
    /// Returns `ImportResult` with details about what was imported, skipped, and any errors.
    /// A fatal error (e.g., completely malformed data) returns `Err(message)`.
    async fn import(&self, data: &str) -> Result<ImportResult, String>;
}

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<dyn QueryExecutor>,
    pub rest: Option<Arc<dyn RestOperations>>,
    pub p2p: Option<Arc<dyn P2POperations>>,
    pub acp: Option<Arc<dyn AcpOperations>>,
    pub index: Option<Arc<dyn IndexOperations>>,
    pub backup: Option<Arc<dyn BackupOperations>>,
    pub schema: Option<Arc<dyn SchemaOperations>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("executor", &"<QueryExecutor>")
            .field("rest", &self.rest.as_ref().map(|_| "<RestOperations>"))
            .field("p2p", &self.p2p.as_ref().map(|_| "<P2POperations>"))
            .field("acp", &self.acp.as_ref().map(|_| "<AcpOperations>"))
            .field("index", &self.index.as_ref().map(|_| "<IndexOperations>"))
            .field(
                "backup",
                &self.backup.as_ref().map(|_| "<BackupOperations>"),
            )
            .field(
                "schema",
                &self.schema.as_ref().map(|_| "<SchemaOperations>"),
            )
            .finish()
    }
}

impl AppState {
    /// Get P2P operations or return ServiceUnavailable error.
    pub fn require_p2p(&self) -> Result<&Arc<dyn P2POperations>, crate::error::HttpError> {
        self.p2p.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "P2P networking is not enabled. Start the server with P2P enabled to use this feature.".into()
            )
        })
    }

    /// Get ACP operations or return ServiceUnavailable error.
    pub fn require_acp(&self) -> Result<&Arc<dyn AcpOperations>, crate::error::HttpError> {
        self.acp.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "ACP (Access Control Policy) is not enabled. Start the server with ACP enabled to use this feature.".into()
            )
        })
    }

    /// Get index operations or return ServiceUnavailable error.
    pub fn require_index(&self) -> Result<&Arc<dyn IndexOperations>, crate::error::HttpError> {
        self.index.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Index operations are not enabled. Start the server with indexing enabled to use this feature.".into()
            )
        })
    }

    /// Get backup operations or return ServiceUnavailable error.
    pub fn require_backup(&self) -> Result<&Arc<dyn BackupOperations>, crate::error::HttpError> {
        self.backup.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Backup operations are not enabled. Start the server with backup enabled to use this feature.".into()
            )
        })
    }

    /// Get schema operations or return ServiceUnavailable error.
    pub fn require_schema(&self) -> Result<&Arc<dyn SchemaOperations>, crate::error::HttpError> {
        self.schema.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Schema operations are not enabled. Start the server with schema enabled to use this feature.".into()
            )
        })
    }
}

/// Builder for constructing AppState with optional components.
pub struct AppStateBuilder {
    executor: Arc<dyn QueryExecutor>,
    rest: Option<Arc<dyn RestOperations>>,
    p2p: Option<Arc<dyn P2POperations>>,
    acp: Option<Arc<dyn AcpOperations>>,
    index: Option<Arc<dyn IndexOperations>>,
    backup: Option<Arc<dyn BackupOperations>>,
    schema: Option<Arc<dyn SchemaOperations>>,
}

impl AppStateBuilder {
    /// Create a new builder with the required executor.
    pub fn new(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            executor,
            rest: None,
            p2p: None,
            acp: None,
            index: None,
            backup: None,
            schema: None,
        }
    }

    /// Set REST operations.
    pub fn with_rest(mut self, rest: Arc<dyn RestOperations>) -> Self {
        self.rest = Some(rest);
        self
    }

    /// Set P2P operations.
    pub fn with_p2p(mut self, p2p: Arc<dyn P2POperations>) -> Self {
        self.p2p = Some(p2p);
        self
    }

    /// Set ACP operations.
    pub fn with_acp(mut self, acp: Arc<dyn AcpOperations>) -> Self {
        self.acp = Some(acp);
        self
    }

    /// Set index operations.
    pub fn with_index(mut self, index: Arc<dyn IndexOperations>) -> Self {
        self.index = Some(index);
        self
    }

    /// Set backup operations.
    pub fn with_backup(mut self, backup: Arc<dyn BackupOperations>) -> Self {
        self.backup = Some(backup);
        self
    }

    /// Set schema operations.
    pub fn with_schema(mut self, schema: Arc<dyn SchemaOperations>) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Build the AppState.
    pub fn build(self) -> AppState {
        AppState {
            executor: self.executor,
            rest: self.rest,
            p2p: self.p2p,
            acp: self.acp,
            index: self.index,
            backup: self.backup,
            schema: self.schema,
        }
    }
}

/// Create the main router with all routes.
///
/// This creates a router with GraphQL endpoints only (no REST).
/// Use `create_router_with_rest` to include REST endpoints.
pub fn create_router(executor: Arc<dyn QueryExecutor>) -> Router {
    let state = AppStateBuilder::new(executor).build();
    create_router_with_state(state)
}

/// Create the main router with all routes including REST endpoints.
pub fn create_router_with_rest(
    executor: Arc<dyn QueryExecutor>,
    rest: Arc<dyn RestOperations>,
) -> Router {
    let state = AppStateBuilder::new(executor).with_rest(rest).build();
    create_router_with_state(state)
}

/// Create the main router with full AppState.
///
/// This allows configuring all optional components (REST, P2P, ACP, Index, Backup).
pub fn create_router_with_state(state: AppState) -> Router {
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

    // P2P routes
    let p2p_routes = Router::new()
        .route("/info", get(handlers::p2p::get_info))
        .route("/connect", post(handlers::p2p::connect)) // Go-compatible
        .route("/peers", get(handlers::p2p::list_peers))
        .route("/peers", post(handlers::p2p::connect_peer)) // Legacy
        .route("/replicators", get(handlers::p2p::list_replicators)) // Go uses /replicators
        .route("/replicators", post(handlers::p2p::add_replicator))
        .route("/replicators", delete(handlers::p2p::remove_replicator))
        .route("/replicator", get(handlers::p2p::list_replicators)) // Legacy
        .route("/replicator", post(handlers::p2p::add_replicator))
        .route("/replicator", delete(handlers::p2p::remove_replicator))
        .route("/collections", get(handlers::p2p::list_collections))
        .route("/collections", post(handlers::p2p::add_collections))
        .route("/collections", delete(handlers::p2p::remove_collections));

    // ACP routes
    let acp_routes = Router::new()
        .route("/policy", post(handlers::acp::add_policy))
        .route("/policy", get(handlers::acp::list_policies))
        .route("/policy/:id", get(handlers::acp::get_policy));

    // Index routes
    let index_routes = Router::new()
        .route("/", post(handlers::index::create_index))
        .route("/", get(handlers::index::list_indexes))
        .route("/", delete(handlers::index::drop_index));

    // Backup routes
    let backup_routes = Router::new()
        .route("/export", get(handlers::backup::export))
        .route("/import", post(handlers::backup::import));

    // API v0 routes
    let api_routes = Router::new()
        // GraphQL endpoints
        .route("/graphql", post(handlers::graphql_transactional))
        .route("/graphql", get(handlers::graphql_get))
        .route("/schema", get(handlers::schema))
        .route("/schema", post(handlers::schema::add_schema))
        .route("/version", get(handlers::version))
        // Transaction endpoints
        .nest("/tx", tx_routes)
        // REST collection endpoints
        .nest("/collections", collection_routes)
        // P2P endpoints
        .nest("/p2p", p2p_routes)
        // ACP endpoints
        .nest("/acp", acp_routes)
        // Index endpoints
        .nest("/index", index_routes)
        // Backup endpoints
        .nest("/backup", backup_routes)
        .with_state(state);

    root_routes.nest("/api/v0", api_routes)
}
