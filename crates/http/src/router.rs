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
/// This trait abstracts the P2P host handle for testing.
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

/// Trait for ACP policy operations.
#[async_trait::async_trait]
pub trait AcpOperations: Send + Sync {
    /// Add a new policy. Returns the policy ID.
    async fn add_policy(&self, policy: &str) -> Result<String, String>;

    /// List all policies.
    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String>;

    /// Get a policy by ID.
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
#[async_trait::async_trait]
pub trait IndexOperations: Send + Sync {
    /// Create an index. Returns the created index info.
    async fn create_index(
        &self,
        collection: &str,
        fields: Vec<String>,
        name: Option<&str>,
        unique: bool,
    ) -> Result<IndexInfo, String>;

    /// List indexes, optionally filtered by collection.
    async fn list_indexes(&self, collection: Option<&str>) -> Result<Vec<IndexInfo>, String>;

    /// Drop an index.
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

/// Trait for backup operations.
#[async_trait::async_trait]
pub trait BackupOperations: Send + Sync {
    /// Export database to JSON.
    async fn export(
        &self,
        collections: Option<Vec<String>>,
        pretty: bool,
    ) -> Result<String, String>;

    /// Import database from JSON.
    async fn import(&self, data: &str) -> Result<(), String>;
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
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("executor", &"<QueryExecutor>")
            .field("rest", &self.rest.as_ref().map(|_| "<RestOperations>"))
            .field("p2p", &self.p2p.as_ref().map(|_| "<P2POperations>"))
            .field("acp", &self.acp.as_ref().map(|_| "<AcpOperations>"))
            .field("index", &self.index.as_ref().map(|_| "<IndexOperations>"))
            .field("backup", &self.backup.as_ref().map(|_| "<BackupOperations>"))
            .finish()
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

    /// Build the AppState.
    pub fn build(self) -> AppState {
        AppState {
            executor: self.executor,
            rest: self.rest,
            p2p: self.p2p,
            acp: self.acp,
            index: self.index,
            backup: self.backup,
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
        .route("/peers", get(handlers::p2p::list_peers))
        .route("/peers", post(handlers::p2p::connect_peer))
        .route("/replicator", get(handlers::p2p::list_replicators))
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
