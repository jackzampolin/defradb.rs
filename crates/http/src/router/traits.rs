//! Operation traits and supporting types for the HTTP router.

use thiserror::Error;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExplicitReplayCapabilityInput {
    #[serde(rename = "CollectionID")]
    pub collection_id: String,
    #[serde(rename = "Capability")]
    pub capability: String,
}

/// HTTP-facing P2P error categories.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum P2PError {
    #[error("invalid request: {0}")]
    InvalidInput(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type P2PResult<T> = Result<T, P2PError>;

impl From<String> for P2PError {
    fn from(message: String) -> Self {
        Self::Internal(message)
    }
}

impl From<&str> for P2PError {
    fn from(message: &str) -> Self {
        Self::Internal(message.to_string())
    }
}

/// Trait for P2P operations that can be accessed via HTTP.
///
/// Abstracts P2P host functionality to decouple HTTP handlers from the
/// actual P2P implementation, enabling both dependency injection and testing.
#[async_trait::async_trait]
pub trait P2POperations: Send + Sync {
    /// Get the local peer ID.
    async fn local_peer_id(&self) -> P2PResult<String>;

    /// Get listening addresses.
    async fn listen_addresses(&self) -> P2PResult<Vec<String>>;

    /// Get connected peers.
    async fn connected_peers(&self) -> P2PResult<Vec<String>>;

    /// Connect to a peer at the given address.
    async fn connect_peer(&self, addr: &str) -> P2PResult<()>;

    /// Get all replicators.
    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>>;

    /// Add a replicator for collections.
    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
        expected_authorizer_did: Option<&str>,
    ) -> P2PResult<()>;

    /// Remove a replicator for collections.
    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> P2PResult<()>;

    /// Get P2P collections.
    async fn get_collections(&self) -> P2PResult<Vec<String>>;

    /// Add collections to P2P.
    async fn add_collections(&self, collections: Vec<String>) -> P2PResult<()>;

    /// Remove collections from P2P.
    async fn remove_collections(&self, collections: Vec<String>) -> P2PResult<()>;

    /// Get P2P documents (for document-level replication).
    async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>>;

    /// Add documents to P2P replication.
    async fn add_documents(&self, docs: Vec<P2pDocumentRequest>) -> P2PResult<()>;

    /// Remove documents from P2P replication.
    async fn remove_documents(&self, docs: Vec<P2pDocumentRequest>) -> P2PResult<()>;

    /// Re-broadcast the current document heads.
    async fn republish_document(&self, collection_name: &str, doc_id: &str) -> P2PResult<()>;

    /// Sync specific documents from connected peers.
    async fn sync_documents(&self, collection_name: &str, doc_ids: Vec<String>) -> P2PResult<()>;

    /// Sync a branchable collection from connected peers.
    async fn sync_branchable_collection(&self, collection_id: &str) -> P2PResult<()>;

    /// Sync collection versions (schema definitions) from connected peers via Bitswap.
    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> P2PResult<()>;
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

/// Request body for document sync (Go-compatible).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncDocumentsRequest {
    #[serde(rename = "collectionName")]
    pub collection_name: String,
    #[serde(rename = "docIDs")]
    pub doc_ids: Vec<String>,
}

/// Request body for branchable collection sync (Go-compatible).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncBranchableRequest {
    #[serde(rename = "collectionID")]
    pub collection_id: String,
}

/// Request body for collection version sync (Go-compatible).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncVersionsRequest {
    #[serde(rename = "versionIDs")]
    pub version_ids: Vec<String>,
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

    /// Validate a collection's `@policy(id:..., resource:...)` directive
    /// against the ACP store. Called at schema-add time.
    ///
    /// Matches Go's `acp.ValidateResourceInterface` in `acp/validation.go`:
    /// 1. The policy with `policy_id` must exist
    /// 2. The resource with `resource_name` must exist on that policy
    /// 3. The resource must declare the DPI-required `read`, `update`,
    ///    and `delete` permissions (Go's `RequiredResourcePermissionsForDocument`)
    ///
    /// Error messages must match Go's format so the behavior is indistinguishable
    /// from the caller's perspective.
    ///
    /// The default impl returns `Ok(())` (permissive) so backends that don't
    /// query a policy store — test mocks, SourceHub light-client placeholders —
    /// don't break. Production impls (`AcpAdapter`, `SourceHubAcpAdapter`)
    /// override with real validation.
    async fn validate_resource_interface(
        &self,
        _policy_id: &str,
        _resource_name: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Get ACP light client status when this ACP backend exposes one.
    async fn get_light_client_status(&self) -> Result<AcpLightClientStatus, String> {
        Err("ACP light client status is not available for this ACP backend".to_string())
    }
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

/// ACP light client status for observability/debugging.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AcpLightClientStatus {
    pub height: u64,
    pub module_state_root: String,
    pub cache_entries: usize,
    pub last_invalidation_height: u64,
    pub connected: bool,
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

    /// Delete an index by collection and name.
    async fn delete_index(&self, collection: &str, name: &str) -> Result<(), String>;
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

/// Trait for encrypted index (searchable encryption) operations.
#[async_trait::async_trait]
pub trait EncryptedIndexOperations: Send + Sync {
    /// Add an encrypted index on a collection field.
    async fn add_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<EncryptedIndexInfo, String>;

    /// List encrypted indexes. If `collection` is `None`, returns indexes from all collections.
    async fn list_encrypted_indexes(
        &self,
        collection: Option<&str>,
    ) -> Result<Vec<EncryptedIndexInfo>, String>;

    /// Delete an encrypted index from a collection field.
    async fn delete_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<(), String>;
}

/// Encrypted index information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptedIndexInfo {
    /// Collection name (used for grouping in list-all responses, not serialized in per-collection responses).
    #[serde(skip)]
    pub collection: String,
    #[serde(rename = "FieldName")]
    pub field_name: String,
    #[serde(rename = "Type")]
    pub index_type: String,
}

/// Trait for block operations.
///
/// Block operations provide signature verification capabilities for
/// DAG-CBOR blocks stored in the blockstore.
#[async_trait::async_trait]
pub trait BlockOperations: Send + Sync {
    /// Verify the signature of a block.
    ///
    /// Loads a block by CID, checks its signature, and verifies using the
    /// provided public key. `key_type` defaults to "secp256k1" if `None`.
    /// `caller_did` is the DID string of the caller for document-level ACP checks.
    async fn verify_signature(
        &self,
        cid: &str,
        public_key: &str,
        key_type: Option<&str>,
        caller_did: Option<&str>,
    ) -> Result<(), String>;
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

    /// Temporarily disable NAC on this node.
    ///
    /// The requestor must be an admin.
    async fn disable(&self, requestor: &identity::Did) -> Result<(), String>;

    /// Re-enable NAC after it was temporarily disabled.
    ///
    /// The requestor must be an admin (uses persisted check).
    async fn re_enable(&self, requestor: &identity::Did) -> Result<(), String>;

    /// Enable NAC with the given owner identity.
    ///
    /// Initializes NAC and sets the owner. Can only be called when NAC
    /// is not already configured.
    async fn enable(&self, owner: &identity::Did) -> Result<(), String>;

    /// Add a NAC relationship (admin or permission grant).
    ///
    /// Routes to the appropriate operation based on the relation name:
    /// - "admin" → add admin
    /// - valid permission name → add permission grant
    /// - "owner" or invalid → error
    async fn add_relationship(
        &self,
        requestor: &identity::Did,
        target: &identity::Did,
        relation: &str,
    ) -> Result<bool, String>;

    /// Remove a NAC relationship (admin or permission grant).
    ///
    /// Routes to the appropriate operation based on the relation name:
    /// - "admin" → remove admin
    /// - valid permission name → remove permission grant
    /// - "owner" or invalid → error
    async fn remove_relationship(
        &self,
        requestor: &identity::Did,
        target: &identity::Did,
        relation: &str,
    ) -> Result<bool, String>;

    /// Get full NAC status info including all FFI-compatible fields.
    async fn info(&self) -> NacStatusInfo;
}

/// NAC status information for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NacStatusInfo {
    pub status: String,
    pub configured_enabled: bool,
    pub dev_mode: bool,
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

/// Trait for collection management operations beyond basic CRUD.
///
/// Provides schema patching, version activation, and truncation operations
/// that operate at the collection level rather than the document level.
#[async_trait::async_trait]
pub trait CollectionManagementOperations: Send + Sync {
    /// Apply a JSON Patch (RFC 6902) to a collection schema.
    ///
    /// Creates a new schema version with the patched fields.
    /// The `patch` should be a JSON array of patch operations.
    async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> Result<serde_json::Value, String>;

    /// Set the active collection version.
    ///
    /// Activates the specified version and deactivates other versions
    /// of the same collection.
    async fn set_active_version(&self, version_id: &str) -> Result<(), String>;

    /// Truncate a collection, deleting all documents while preserving the schema.
    async fn truncate_collection(&self, name: &str) -> Result<(), String>;

    /// Purge all data from all collections.
    async fn purge(&self) -> Result<(), String>;

    /// Get a collection by name.
    async fn get_collection_by_name(
        &self,
        name: &str,
    ) -> Result<Option<schema::CollectionVersion>, String>;

    /// Check if a collection exists.
    async fn has_collection(&self, name: &str) -> Result<bool, String>;

    /// Find a collection by its collection ID.
    async fn find_collection_by_id(
        &self,
        collection_id: &str,
    ) -> Result<Option<schema::CollectionVersion>, String>;

    /// Get a collection by version ID, searching both cache and storage.
    async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> Result<Option<schema::CollectionVersion>, String>;

    /// Delete multiple collection versions in a batch.
    async fn delete_collection_versions(&self, version_ids: Vec<String>) -> Result<(), String>;

    /// Get all collection versions (active + inactive) from the system store.
    async fn get_all_collections(&self) -> Result<Vec<schema::CollectionVersion>, String>;

    /// Delete a collection by name.
    async fn delete_collection(&self, name: &str) -> Result<(), String>;
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

    /// Add a lens configuration directly.
    ///
    /// The config should be a JSON string containing the full lens configuration.
    /// Returns the transform ID assigned to this lens.
    async fn add(&self, config: &str) -> Result<String, String>;

    /// List all registered lens modules.
    ///
    /// Returns a JSON value representing all registered transforms.
    async fn list(&self) -> Result<serde_json::Value, String>;
}

/// Trait for document-level ACP operations.
///
/// Manages per-document access control relationships (e.g., granting a user
/// read or write access to a specific document).
#[async_trait::async_trait]
pub trait DocumentAcpOperations: Send + Sync {
    /// Add an actor relationship to a document.
    ///
    /// Grants the `target_actor` the specified `relation` on the document
    /// identified by `collection` and `doc_id`.
    ///
    /// Returns `true` if a new relationship was created, `false` if it already existed.
    async fn add_doc_relationship(
        &self,
        requestor: &identity::Did,
        target_actor: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool, String>;

    /// Delete an actor relationship from a document.
    ///
    /// Revokes the `target_actor`'s `relation` on the document
    /// identified by `collection` and `doc_id`.
    ///
    /// Returns `true` if the relationship was removed, `false` if it didn't exist.
    async fn delete_doc_relationship(
        &self,
        requestor: &identity::Did,
        target_actor: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool, String>;
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

/// Trait for transaction-scoped operations.
///
/// Provides access to operations that must execute within an existing transaction,
/// such as setting migrations, adding schemas, or reading collection versions
/// including uncommitted writes.
#[async_trait::async_trait]
pub trait TransactionOperations: Send + Sync {
    /// Set a migration within an existing transaction.
    ///
    /// Registers a lens migration configuration within the specified transaction.
    /// The migration will only be visible after the transaction is committed.
    /// Returns the transform ID.
    async fn set_migration_in_txn(&self, txn_id: &str, config: &str) -> Result<String, String>;

    /// Get all collection versions visible within a transaction.
    ///
    /// Reads from the transaction's systemstore, which includes both
    /// committed data and any uncommitted writes made within this transaction.
    async fn get_collections_in_txn(
        &self,
        txn_id: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String>;

    /// Add a schema within an existing transaction.
    ///
    /// Parses the SDL and creates collections within the transaction.
    /// The collections are only visible after the transaction is committed,
    /// but can be used by queries within the same transaction.
    async fn add_schema_in_txn(
        &self,
        txn_id: &str,
        sdl: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String>;
}

/// Trait for view operations.
///
/// Views are virtual collections backed by a GQL query. Materialized views
/// cache their results and may also expose explicit maintenance operations.
#[async_trait::async_trait]
pub trait ViewOperations: Send + Sync {
    /// Add a view from a GQL query and SDL schema.
    ///
    /// Returns the created collection versions for the view.
    async fn add_view(
        &self,
        gql_query: &str,
        sdl: &str,
        transform: Option<&str>,
    ) -> Result<Vec<schema::CollectionVersion>, String>;

    /// Refresh materialized view caches.
    ///
    /// If `names` is provided, only those views are refreshed. Otherwise all
    /// materialized views are refreshed.
    async fn refresh_views(&self, names: Option<Vec<String>>) -> Result<(), String>;

    /// Run manual downsample history GC.
    ///
    /// If `names` is provided, only those downsample targets are processed.
    /// Otherwise all downsample targets are processed.
    async fn gc_downsample_histories(&self, names: Option<Vec<String>>) -> Result<(), String>;
}

/// Trait for debug dump operations.
#[async_trait::async_trait]
pub trait DumpOperations: Send + Sync {
    /// Dump all database key/value pairs as a list of human-readable strings.
    async fn print_dump(&self) -> Result<Vec<String>, String>;
}
