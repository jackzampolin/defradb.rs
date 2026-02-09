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

    /// Get P2P documents (for document-level replication).
    async fn get_documents(&self) -> Result<Vec<P2pDocumentInfo>, String>;

    /// Add documents to P2P replication.
    async fn add_documents(&self, docs: Vec<P2pDocumentRequest>) -> Result<(), String>;

    /// Remove documents from P2P replication.
    async fn remove_documents(&self, docs: Vec<P2pDocumentRequest>) -> Result<(), String>;

    /// Sync collections with peers (trigger immediate sync).
    async fn sync_collections(&self) -> Result<(), String>;

    /// Sync documents with peers (trigger immediate sync).
    async fn sync_documents(&self) -> Result<(), String>;
}

/// Replicator information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicatorInfo {
    pub id: Option<String>,
    pub collections: Vec<String>,
    pub address: Option<String>,
}

/// P2P document information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct P2pDocumentInfo {
    /// Collection name the document belongs to.
    #[serde(rename = "Collection")]
    pub collection: String,
    /// Document ID.
    #[serde(rename = "DocID")]
    pub doc_id: String,
}

/// Request to add/remove P2P documents (Go-compatible format).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct P2pDocumentRequest {
    /// Collection name the document belongs to.
    #[serde(rename = "Collection")]
    pub collection: String,
    /// Document ID.
    #[serde(rename = "DocID")]
    pub doc_id: String,
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

/// Re-export NAC types from the acp crate for convenience.
pub use acp::nac::{NacStatus, NodePermission};

/// Trait for Node Access Control (NAC) operations.
///
/// NAC provides node-level access control using the Zanzibar permission model.
/// When enabled, node operations require authentication and authorization.
#[async_trait::async_trait]
pub trait NodeAcpOperations: Send + Sync {
    /// Check if an identity has a specific node permission.
    ///
    /// Returns `true` if:
    /// - NAC is not enabled (all operations allowed)
    /// - The identity has the required permission
    async fn check_permission(
        &self,
        identity: &identity::Did,
        permission: NodePermission,
    ) -> Result<bool, String>;

    /// Get the current NAC status.
    async fn get_status(&self) -> NacStatus;

    /// Get the owner identity.
    async fn owner(&self) -> Option<identity::Did>;

    /// Check if an identity is an admin.
    async fn is_admin(&self, identity: &identity::Did) -> Result<bool, String>;

    /// Add an admin relationship.
    async fn add_admin(
        &self,
        requestor: &identity::Did,
        target: &identity::Did,
    ) -> Result<bool, String>;

    /// Remove an admin relationship.
    async fn remove_admin(
        &self,
        requestor: &identity::Did,
        target: &identity::Did,
    ) -> Result<bool, String>;
}

/// NAC status information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NacStatusInfo {
    /// Current NAC status (not configured, enabled, disabled temporarily)
    pub status: String,
    /// Owner DID if NAC is enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
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

/// Trait for lens migration operations.
///
/// Enables setting up migrations between schema versions using WASM transforms.
#[async_trait::async_trait]
pub trait LensOperations: Send + Sync {
    /// Set a migration between schema versions.
    ///
    /// The config should be a JSON string containing:
    /// - SourceSchemaVersionID: The source version CID
    /// - DestinationSchemaVersionID: The destination version CID
    /// - Lens: The lens configuration with path to WASM module
    ///
    /// Returns the transform ID assigned to this migration.
    async fn set_migration(&self, config: &str) -> Result<String, String>;

    /// Reload all lens modules from disk.
    async fn reload(&self) -> Result<(), String>;
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
    pub lens: Option<Arc<dyn LensOperations>>,
    pub nac: Option<Arc<dyn NodeAcpOperations>>,
    pub event_bus: Option<Arc<dyn events::Bus>>,
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
            .field("lens", &self.lens.as_ref().map(|_| "<LensOperations>"))
            .field("nac", &self.nac.as_ref().map(|_| "<NodeAcpOperations>"))
            .field("event_bus", &self.event_bus.as_ref().map(|_| "<EventBus>"))
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

    /// Get lens operations or return ServiceUnavailable error.
    pub fn require_lens(&self) -> Result<&Arc<dyn LensOperations>, crate::error::HttpError> {
        self.lens.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "Lens operations are not enabled. Start the server with lens enabled to use this feature.".into()
            )
        })
    }

    /// Get NAC operations or return ServiceUnavailable error.
    pub fn require_nac(&self) -> Result<&Arc<dyn NodeAcpOperations>, crate::error::HttpError> {
        self.nac.as_ref().ok_or_else(|| {
            crate::error::HttpError::ServiceUnavailable(
                "NAC (Node Access Control) is not enabled. Start the server with --acp-node-enable to use this feature.".into()
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
    lens: Option<Arc<dyn LensOperations>>,
    nac: Option<Arc<dyn NodeAcpOperations>>,
    event_bus: Option<Arc<dyn events::Bus>>,
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
            lens: None,
            nac: None,
            event_bus: None,
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

    /// Set lens operations.
    pub fn with_lens(mut self, lens: Arc<dyn LensOperations>) -> Self {
        self.lens = Some(lens);
        self
    }

    /// Set NAC (Node Access Control) operations.
    pub fn with_nac(mut self, nac: Arc<dyn NodeAcpOperations>) -> Self {
        self.nac = Some(nac);
        self
    }

    /// Set event bus for subscriptions.
    pub fn with_event_bus(mut self, bus: Arc<dyn events::Bus>) -> Self {
        self.event_bus = Some(bus);
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
            lens: self.lens,
            nac: self.nac,
            event_bus: self.event_bus,
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

    // Transaction routes (Go-compatible)
    // Go DefraDB:
    //   POST /tx - begin transaction (query param: ?read_only=true)
    //   POST /tx/concurrent - begin concurrent transaction
    //   POST /tx/{id} - commit transaction
    //   DELETE /tx/{id} - discard transaction
    let tx_routes = Router::new()
        .route("/", post(handlers::tx_begin))
        .route("/concurrent", post(handlers::tx_begin_concurrent))
        .route("/:id", post(handlers::tx_commit))
        .route("/:id", delete(handlers::tx_discard));

    // Collection routes (REST API)
    let collection_routes = Router::new()
        .route("/", get(handlers::list_collections))
        .route("/", patch(handlers::patch_collection))
        .route("/set-active", post(handlers::set_active))
        .route("/:name", get(handlers::get_collection_doc_ids))
        .route("/:name", post(handlers::create_document))
        .route("/:name/truncate", delete(handlers::truncate_collection))
        .route("/:name/:docID", get(handlers::get_document))
        .route("/:name/:docID", patch(handlers::update_document))
        .route("/:name/:docID", delete(handlers::delete_document))
        // Go-compatible index routes (collection in path)
        .route("/:name/indexes", get(handlers::index::go_list_indexes))
        .route("/:name/indexes", post(handlers::index::go_create_index))
        .route(
            "/:name/indexes/:index",
            delete(handlers::index::go_drop_index),
        );

    // P2P routes
    let p2p_routes = Router::new()
        .route("/info", get(handlers::p2p::get_info))
        .route("/active-peers", get(handlers::p2p::active_peers)) // Go-compatible
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
        .route("/collections", delete(handlers::p2p::remove_collections))
        .route("/collections/sync", post(handlers::p2p::sync_collections)) // Go-compatible
        .route("/documents", get(handlers::p2p::list_documents)) // Go-compatible
        .route("/documents", post(handlers::p2p::add_documents))
        .route("/documents", delete(handlers::p2p::remove_documents))
        .route("/documents/sync", post(handlers::p2p::sync_documents)); // Go-compatible

    // ACP routes
    let acp_routes = Router::new()
        .route("/policy", post(handlers::acp::add_policy))
        .route("/policy", get(handlers::acp::list_policies))
        .route("/policy/:id", get(handlers::acp::get_policy))
        .route(
            "/document/relationship",
            post(handlers::acp::add_doc_relationship),
        )
        .route(
            "/document/relationship",
            delete(handlers::acp::remove_doc_relationship),
        );

    // Index routes
    let index_routes = Router::new()
        .route("/", post(handlers::index::create_index))
        .route("/", get(handlers::index::list_indexes))
        .route("/", delete(handlers::index::drop_index));

    // Backup routes (POST for both to match Go DefraDB)
    let backup_routes = Router::new()
        .route("/export", post(handlers::backup::export))
        .route("/import", post(handlers::backup::import));

    // Lens migration routes
    let lens_routes = Router::new()
        .route("/", post(handlers::lens::add_lens))
        .route("/", get(handlers::lens::list_lenses))
        .route("/set", post(handlers::lens::set_migration))
        .route("/reload", post(handlers::lens::reload));

    // NAC (Node Access Control) routes
    let nac_routes = Router::new()
        .route("/status", get(handlers::nac::get_status))
        .route("/admin", post(handlers::nac::add_admin))
        .route("/admin", delete(handlers::nac::remove_admin));

    // Go-compatible ACP node routes (aliased from /acp/node/*)
    // Go DefraDB uses:
    //   GET /acp/node/status
    //   POST /acp/node/relationship
    //   DELETE /acp/node/relationship
    //   POST /acp/node/disable
    //   POST /acp/node/re-enable
    let acp_node_routes = Router::new()
        .route("/status", get(handlers::nac::get_status))
        .route("/relationship", post(handlers::nac::go_add_relationship))
        .route(
            "/relationship",
            delete(handlers::nac::go_remove_relationship),
        )
        .route("/disable", post(handlers::nac::disable))
        .route("/re-enable", post(handlers::nac::re_enable));

    // API v0 routes
    let api_routes = Router::new()
        // GraphQL endpoints
        .route("/graphql", post(handlers::graphql_transactional))
        .route("/graphql", get(handlers::graphql_get))
        .route(
            "/graphql/ws",
            axum::routing::any(handlers::graphql_ws_handler),
        )
        .route("/schema", get(handlers::schema))
        .route("/schema", post(handlers::schema::add_schema))
        .route("/version", get(handlers::version))
        // Transaction endpoints
        .nest("/tx", tx_routes)
        // REST collection endpoints
        .nest("/collections", collection_routes)
        // P2P endpoints
        .nest("/p2p", p2p_routes)
        // ACP endpoints (document-level access control)
        .nest("/acp", acp_routes)
        // Go-compatible ACP node routes (NAC via /acp/node/*)
        .nest("/acp/node", acp_node_routes)
        // Index endpoints
        .nest("/index", index_routes)
        // Backup endpoints
        .nest("/backup", backup_routes)
        // Lens migration endpoints
        .nest("/lens", lens_routes)
        // NAC endpoints (Rust-native routes)
        .nest("/nac", nac_routes)
        // Utility endpoints (Go-compatible)
        .route("/purge", post(handlers::utility::purge))
        .route("/node/identity", get(handlers::utility::get_node_identity))
        .with_state(state);

    root_routes.nest("/api/v0", api_routes)
}
