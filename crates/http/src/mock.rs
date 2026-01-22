//! Mock executors for testing HTTP routes.

use async_trait::async_trait;
use identity::Did;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use query::error::{QueryError, Result, TransactionError};
use query::executor::{QueryExecutor, QueryRequest, QueryResponse};
use query::rest::{RestError, RestOperations, RestResult};
use query::TransactionHandle;

/// Mock executor for testing HTTP routes with pattern-matched query responses.
#[derive(Debug, Clone)]
pub struct MockQueryExecutor {
    schema_sdl: String,
}

/// Mock executor for testing error paths. Schema errors are optional.
#[derive(Debug, Clone)]
pub struct FailingMockExecutor {
    schema_error: Option<String>,
}

impl FailingMockExecutor {
    /// Create a mock that fails schema() calls.
    pub fn with_schema_error(msg: impl Into<String>) -> Self {
        Self {
            schema_error: Some(msg.into()),
        }
    }
}

#[async_trait]
impl QueryExecutor for FailingMockExecutor {
    async fn execute(&self, _request: QueryRequest) -> QueryResponse {
        QueryResponse::error("execution failed")
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> QueryResponse {
        self.execute(request).await
    }

    async fn begin_txn(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Err(TransactionError::not_supported(
            "mock executor does not support transactions",
        ))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "mock executor does not support transactions",
        ))
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "mock executor does not support transactions",
        ))
    }

    async fn schema(&self) -> Result<String> {
        match &self.schema_error {
            Some(msg) => Err(QueryError::internal(msg.clone())),
            None => Ok("type Query { ping: String }".to_string()),
        }
    }
}

impl MockQueryExecutor {
    /// Create a mock executor with default schema.
    pub fn new() -> Self {
        Self {
            schema_sdl: default_mock_schema(),
        }
    }

    /// Create with custom schema.
    pub fn with_schema(schema: impl Into<String>) -> Self {
        Self {
            schema_sdl: schema.into(),
        }
    }
}

impl Default for MockQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueryExecutor for MockQueryExecutor {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        let query = request.query.to_lowercase();

        if query.contains("users") {
            QueryResponse::success(json!({
                "users": [
                    {"_docID": "bae-123", "name": "Alice", "age": 30},
                    {"_docID": "bae-456", "name": "Bob", "age": 25}
                ]
            }))
        } else if query.contains("__schema") || query.contains("__type") {
            QueryResponse::success(json!({
                "__schema": {
                    "types": [],
                    "queryType": {"name": "Query"},
                    "mutationType": {"name": "Mutation"}
                }
            }))
        } else if query.contains("create") || query.contains("update") || query.contains("delete") {
            QueryResponse::success(json!({
                "_docID": "bae-new-789"
            }))
        } else {
            QueryResponse::success(json!({
                "data": null,
                "message": "Mock response"
            }))
        }
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> QueryResponse {
        self.execute(request).await
    }

    async fn begin_txn(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Ok(TransactionHandle::new("mock-txn-001".to_string()))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Ok(())
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Ok(())
    }

    async fn schema(&self) -> Result<String> {
        Ok(self.schema_sdl.clone())
    }
}

fn default_mock_schema() -> String {
    r#"
type User {
    _docID: ID!
    name: String!
    age: Int
    email: String
}

type Query {
    users: [User!]!
    user(_docID: ID!): User
}

type Mutation {
    create_User(input: UserInput!): User!
    update_User(_docID: ID!, input: UserInput!): User!
    delete_User(_docID: ID!): User!
}

input UserInput {
    name: String!
    age: Int
    email: String
}
"#
    .to_string()
}

// ============================================================================
// Mock REST Operations
// ============================================================================

/// Internal document storage for mock REST operations.
#[derive(Debug, Clone, Default)]
struct MockDocument {
    doc_id: String,
    data: JsonValue,
}

/// Mock REST operations for testing collection and document handlers.
#[derive(Debug)]
pub struct MockRestOperations {
    /// Collections with their documents.
    collections: Arc<RwLock<HashMap<String, Vec<MockDocument>>>>,
    /// Counter for generating unique document IDs.
    next_id: Arc<RwLock<u64>>,
}

impl Clone for MockRestOperations {
    fn clone(&self) -> Self {
        Self {
            collections: Arc::clone(&self.collections),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

impl Default for MockRestOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRestOperations {
    /// Create a new mock REST operations instance with default collections.
    pub fn new() -> Self {
        let mut collections = HashMap::new();

        // Add default Users collection with sample data
        collections.insert(
            "Users".to_string(),
            vec![
                MockDocument {
                    doc_id: "bae-123".to_string(),
                    data: json!({"name": "Alice", "age": 30}),
                },
                MockDocument {
                    doc_id: "bae-456".to_string(),
                    data: json!({"name": "Bob", "age": 25}),
                },
            ],
        );

        // Add empty Books collection
        collections.insert("Books".to_string(), vec![]);

        Self {
            collections: Arc::new(RwLock::new(collections)),
            next_id: Arc::new(RwLock::new(1000)),
        }
    }

    /// Create an empty mock REST operations instance.
    pub fn empty() -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Add a collection (for test setup).
    pub fn with_collection(self, name: &str) -> Self {
        self.collections
            .write()
            .unwrap()
            .insert(name.to_string(), vec![]);
        self
    }

    /// Generate a new unique document ID.
    fn generate_doc_id(&self) -> String {
        let mut id = self.next_id.write().unwrap();
        *id += 1;
        format!("bae-{:08x}", *id)
    }
}

#[async_trait]
impl RestOperations for MockRestOperations {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        let collections = self.collections.read().unwrap();
        let mut names: Vec<String> = collections.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    async fn get_collection_doc_ids(
        &self,
        collection: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        let collections = self.collections.read().unwrap();
        match collections.get(collection) {
            Some(docs) => Ok(docs.iter().map(|d| d.doc_id.clone()).collect()),
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        let collections = self.collections.read().unwrap();
        match collections.get(collection) {
            Some(docs) => {
                let doc = docs.iter().find(|d| d.doc_id == doc_id);
                match doc {
                    Some(d) => {
                        let mut result = d.data.clone();
                        if let Some(obj) = result.as_object_mut() {
                            obj.insert("_docID".to_string(), json!(d.doc_id));
                        }
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn create_document(
        &self,
        collection: &str,
        data: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        let mut collections = self.collections.write().unwrap();
        match collections.get_mut(collection) {
            Some(docs) => {
                let doc_id = self.generate_doc_id();
                let doc = MockDocument {
                    doc_id: doc_id.clone(),
                    data: data.clone(),
                };
                docs.push(doc);

                let mut result = data;
                if let Some(obj) = result.as_object_mut() {
                    obj.insert("_docID".to_string(), json!(doc_id));
                }
                Ok(result)
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
        identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>> {
        let mut results = Vec::with_capacity(data.len());
        for item in data {
            let result = self.create_document(collection, item, identity).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        let mut collections = self.collections.write().unwrap();
        match collections.get_mut(collection) {
            Some(docs) => {
                let doc = docs.iter_mut().find(|d| d.doc_id == doc_id);
                match doc {
                    Some(d) => {
                        // Merge patch into existing data
                        if let (Some(existing), Some(updates)) =
                            (d.data.as_object_mut(), patch.as_object())
                        {
                            for (key, value) in updates {
                                existing.insert(key.clone(), value.clone());
                            }
                        }

                        let mut result = d.data.clone();
                        if let Some(obj) = result.as_object_mut() {
                            obj.insert("_docID".to_string(), json!(d.doc_id));
                        }
                        Ok(result)
                    }
                    None => Err(RestError::document_not_found(doc_id)),
                }
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }

    async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<bool> {
        let mut collections = self.collections.write().unwrap();
        match collections.get_mut(collection) {
            Some(docs) => {
                let initial_len = docs.len();
                docs.retain(|d| d.doc_id != doc_id);
                Ok(docs.len() < initial_len)
            }
            None => Err(RestError::collection_not_found(collection)),
        }
    }
}

/// Mock REST operations that always fails (for error path testing).
///
/// Supports configurable error types for testing different error paths.
#[derive(Debug, Clone)]
pub struct FailingMockRestOperations {
    error: RestError,
}

impl FailingMockRestOperations {
    /// Create a mock that always returns an internal error with the given message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            error: RestError::internal(message),
        }
    }

    /// Create a mock that always returns the specified error.
    pub fn with_error(error: RestError) -> Self {
        Self { error }
    }

    /// Create a mock that returns InvalidDocId errors.
    pub fn with_invalid_doc_id(id: impl Into<String>) -> Self {
        Self {
            error: RestError::invalid_doc_id(id),
        }
    }

    /// Create a mock that returns InvalidInput errors.
    pub fn with_invalid_input(msg: impl Into<String>) -> Self {
        Self {
            error: RestError::invalid_input(msg),
        }
    }

    /// Create a mock that returns PermissionDenied errors.
    pub fn with_permission_denied(msg: impl Into<String>) -> Self {
        Self {
            error: RestError::permission_denied(msg),
        }
    }
}

#[async_trait]
impl RestOperations for FailingMockRestOperations {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        Err(self.error.clone())
    }

    async fn get_collection_doc_ids(
        &self,
        _collection: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        Err(self.error.clone())
    }

    async fn get_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        Err(self.error.clone())
    }

    async fn create_document(
        &self,
        _collection: &str,
        _data: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        Err(self.error.clone())
    }

    async fn create_documents(
        &self,
        _collection: &str,
        _data: Vec<JsonValue>,
        _identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>> {
        Err(self.error.clone())
    }

    async fn update_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _patch: JsonValue,
        _identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        Err(self.error.clone())
    }

    async fn delete_document(
        &self,
        _collection: &str,
        _doc_id: &str,
        _identity: Option<&Did>,
    ) -> RestResult<bool> {
        Err(self.error.clone())
    }
}

// ============================================================================
// Mock P2P Operations
// ============================================================================

use crate::router::{
    AcpOperations, BackupOperations, ImportResult, IndexFieldInfo, IndexInfo, IndexOperations,
    P2POperations, P2pDocumentInfo, P2pDocumentRequest, PolicyInfo, ReplicatorInfo,
};

/// Mock P2P operations for testing P2P handlers.
#[derive(Debug)]
pub struct MockP2POperations {
    peer_id: String,
    addresses: Vec<String>,
    peers: Arc<RwLock<Vec<String>>>,
    replicators: Arc<RwLock<Vec<ReplicatorInfo>>>,
    collections: Arc<RwLock<Vec<String>>>,
}

impl Clone for MockP2POperations {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id.clone(),
            addresses: self.addresses.clone(),
            peers: Arc::clone(&self.peers),
            replicators: Arc::clone(&self.replicators),
            collections: Arc::clone(&self.collections),
        }
    }
}

impl Default for MockP2POperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockP2POperations {
    /// Create a new mock P2P operations instance.
    pub fn new() -> Self {
        Self {
            peer_id: "12D3KooWMockPeerId123456789".to_string(),
            addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
            peers: Arc::new(RwLock::new(vec![])),
            replicators: Arc::new(RwLock::new(vec![])),
            collections: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Create with a connected peer.
    pub fn with_peer(self, peer_id: &str) -> Self {
        self.peers.write().unwrap().push(peer_id.to_string());
        self
    }

    /// Create with a replicator.
    pub fn with_replicator(self, collections: Vec<String>, address: Option<String>) -> Self {
        self.replicators.write().unwrap().push(ReplicatorInfo {
            id: Some("12D3KooWReplicator".to_string()),
            collections,
            address,
        });
        self
    }

    /// Create with P2P collections.
    pub fn with_collections(self, collections: Vec<String>) -> Self {
        *self.collections.write().unwrap() = collections;
        self
    }
}

#[async_trait]
impl P2POperations for MockP2POperations {
    async fn local_peer_id(&self) -> std::result::Result<String, String> {
        Ok(self.peer_id.clone())
    }

    async fn listen_addresses(&self) -> std::result::Result<Vec<String>, String> {
        Ok(self.addresses.clone())
    }

    async fn connected_peers(&self) -> std::result::Result<Vec<String>, String> {
        Ok(self.peers.read().unwrap().clone())
    }

    async fn connect_peer(&self, addr: &str) -> std::result::Result<(), String> {
        // Extract a mock peer ID from the address
        let peer_id = if addr.contains("/p2p/") {
            addr.split("/p2p/").last().unwrap_or("unknown").to_string()
        } else {
            format!("peer-{}", addr.len())
        };
        self.peers.write().unwrap().push(peer_id);
        Ok(())
    }

    async fn get_replicators(&self) -> std::result::Result<Vec<ReplicatorInfo>, String> {
        Ok(self.replicators.read().unwrap().clone())
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> std::result::Result<(), String> {
        self.replicators.write().unwrap().push(ReplicatorInfo {
            id: Some("12D3KooWNewReplicator".to_string()),
            collections,
            address: addr.map(|s| s.to_string()),
        });
        Ok(())
    }

    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        _addr: Option<&str>,
    ) -> std::result::Result<(), String> {
        let mut replicators = self.replicators.write().unwrap();
        replicators.retain(|r| !collections.iter().all(|c| r.collections.contains(c)));
        Ok(())
    }

    async fn get_collections(&self) -> std::result::Result<Vec<String>, String> {
        Ok(self.collections.read().unwrap().clone())
    }

    async fn add_collections(&self, collections: Vec<String>) -> std::result::Result<(), String> {
        let mut existing = self.collections.write().unwrap();
        for col in collections {
            if !existing.contains(&col) {
                existing.push(col);
            }
        }
        Ok(())
    }

    async fn remove_collections(
        &self,
        collections: Vec<String>,
    ) -> std::result::Result<(), String> {
        let mut existing = self.collections.write().unwrap();
        existing.retain(|c| !collections.contains(c));
        Ok(())
    }

    async fn get_documents(&self) -> std::result::Result<Vec<P2pDocumentInfo>, String> {
        Ok(vec![])
    }

    async fn add_documents(
        &self,
        _docs: Vec<P2pDocumentRequest>,
    ) -> std::result::Result<(), String> {
        Ok(())
    }

    async fn remove_documents(
        &self,
        _docs: Vec<P2pDocumentRequest>,
    ) -> std::result::Result<(), String> {
        Ok(())
    }

    async fn sync_collections(&self) -> std::result::Result<(), String> {
        Ok(())
    }

    async fn sync_documents(&self) -> std::result::Result<(), String> {
        Ok(())
    }
}

// ============================================================================
// Mock ACP Operations
// ============================================================================

/// Mock ACP operations for testing ACP handlers.
#[derive(Debug)]
pub struct MockAcpOperations {
    policies: Arc<RwLock<Vec<PolicyInfo>>>,
    next_id: Arc<RwLock<u64>>,
}

impl Clone for MockAcpOperations {
    fn clone(&self) -> Self {
        Self {
            policies: Arc::clone(&self.policies),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

impl Default for MockAcpOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAcpOperations {
    /// Create a new mock ACP operations instance.
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(vec![])),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Create with a pre-existing policy.
    pub fn with_policy(self, id: &str, name: Option<&str>) -> Self {
        self.policies.write().unwrap().push(PolicyInfo {
            id: id.to_string(),
            name: name.map(|s| s.to_string()),
            description: None,
            resources: None,
            actor: None,
            creation_time: Some("2024-01-01T00:00:00Z".to_string()),
        });
        self
    }
}

#[async_trait]
impl AcpOperations for MockAcpOperations {
    async fn add_policy(&self, _policy: &str) -> std::result::Result<String, String> {
        let mut id = self.next_id.write().unwrap();
        let policy_id = format!("policy-{:04}", *id);
        *id += 1;

        self.policies.write().unwrap().push(PolicyInfo {
            id: policy_id.clone(),
            name: Some("Test Policy".to_string()),
            description: Some("A test policy".to_string()),
            resources: None,
            actor: None,
            creation_time: Some("2024-01-01T00:00:00Z".to_string()),
        });

        Ok(policy_id)
    }

    async fn list_policies(&self) -> std::result::Result<Vec<PolicyInfo>, String> {
        Ok(self.policies.read().unwrap().clone())
    }

    async fn get_policy(&self, id: &str) -> std::result::Result<Option<PolicyInfo>, String> {
        let policies = self.policies.read().unwrap();
        Ok(policies.iter().find(|p| p.id == id).cloned())
    }
}

// ============================================================================
// Mock Index Operations
// ============================================================================

/// Mock index operations for testing index handlers.
#[derive(Debug)]
pub struct MockIndexOperations {
    indexes: Arc<RwLock<Vec<IndexInfo>>>,
    next_id: Arc<RwLock<u64>>,
}

impl Clone for MockIndexOperations {
    fn clone(&self) -> Self {
        Self {
            indexes: Arc::clone(&self.indexes),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

impl Default for MockIndexOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockIndexOperations {
    /// Create a new mock index operations instance.
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(vec![])),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Create with a pre-existing index.
    pub fn with_index(self, collection: &str, name: &str, fields: Vec<&str>, unique: bool) -> Self {
        self.indexes.write().unwrap().push(IndexInfo {
            name: name.to_string(),
            collection: collection.to_string(),
            fields: fields
                .into_iter()
                .map(|f| IndexFieldInfo {
                    name: f.to_string(),
                    direction: Some("ASC".to_string()),
                })
                .collect(),
            unique,
        });
        self
    }
}

#[async_trait]
impl IndexOperations for MockIndexOperations {
    async fn create_index(
        &self,
        collection: &str,
        fields: Vec<String>,
        name: Option<&str>,
        unique: bool,
    ) -> std::result::Result<IndexInfo, String> {
        let index_name = match name {
            Some(n) => n.to_string(),
            None => {
                let mut id = self.next_id.write().unwrap();
                let name = format!("idx_{}_{}", collection.to_lowercase(), *id);
                *id += 1;
                name
            }
        };

        let index = IndexInfo {
            name: index_name,
            collection: collection.to_string(),
            fields: fields
                .into_iter()
                .map(|f| IndexFieldInfo {
                    name: f,
                    direction: Some("ASC".to_string()),
                })
                .collect(),
            unique,
        };

        self.indexes.write().unwrap().push(index.clone());
        Ok(index)
    }

    async fn list_indexes(
        &self,
        collection: Option<&str>,
    ) -> std::result::Result<Vec<IndexInfo>, String> {
        let indexes = self.indexes.read().unwrap();
        match collection {
            Some(col) => Ok(indexes
                .iter()
                .filter(|i| i.collection == col)
                .cloned()
                .collect()),
            None => Ok(indexes.clone()),
        }
    }

    async fn drop_index(&self, collection: &str, name: &str) -> std::result::Result<(), String> {
        let mut indexes = self.indexes.write().unwrap();
        let initial_len = indexes.len();
        indexes.retain(|i| !(i.collection == collection && i.name == name));
        if indexes.len() < initial_len {
            Ok(())
        } else {
            Err(format!(
                "index '{}' not found in collection '{}'",
                name, collection
            ))
        }
    }
}

// ============================================================================
// Mock Backup Operations
// ============================================================================

/// Mock backup operations for testing backup handlers.
#[derive(Debug)]
pub struct MockBackupOperations {
    data: Arc<RwLock<String>>,
}

impl Clone for MockBackupOperations {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
        }
    }
}

impl Default for MockBackupOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBackupOperations {
    /// Create a new mock backup operations instance.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(
                r#"{"Users": [{"_docID": "bae-123", "name": "Alice"}]}"#.to_string(),
            )),
        }
    }

    /// Create with custom backup data.
    pub fn with_data(data: &str) -> Self {
        Self {
            data: Arc::new(RwLock::new(data.to_string())),
        }
    }
}

#[async_trait]
impl BackupOperations for MockBackupOperations {
    async fn export(
        &self,
        _collections: Option<Vec<String>>,
        pretty: bool,
    ) -> std::result::Result<String, String> {
        let data = self.data.read().unwrap().clone();
        if pretty {
            // Format the JSON nicely
            let parsed: serde_json::Value =
                serde_json::from_str(&data).map_err(|e| format!("invalid JSON: {}", e))?;
            serde_json::to_string_pretty(&parsed).map_err(|e| format!("failed to serialize: {}", e))
        } else {
            Ok(data)
        }
    }

    async fn import(&self, data: &str) -> std::result::Result<ImportResult, String> {
        // Parse the incoming data to validate it
        let parsed: serde_json::Value =
            serde_json::from_str(data).map_err(|e| format!("invalid JSON: {}", e))?;

        // Count documents and collections
        let mut documents_imported = 0u64;
        let mut collections_affected = Vec::new();

        if let Some(obj) = parsed.as_object() {
            for (collection, docs) in obj {
                collections_affected.push(collection.clone());
                if let Some(arr) = docs.as_array() {
                    documents_imported += arr.len() as u64;
                }
            }
        }

        // Store the new data
        *self.data.write().unwrap() = data.to_string();

        Ok(ImportResult {
            documents_imported,
            documents_skipped: 0,
            collections_affected,
            errors: vec![],
        })
    }
}

// ============================================================================
// Failing Mock Operations (for error path testing)
// ============================================================================

/// Mock P2P operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockP2POperations {
    error: String,
}

impl FailingMockP2POperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl P2POperations for FailingMockP2POperations {
    async fn local_peer_id(&self) -> std::result::Result<String, String> {
        Err(self.error.clone())
    }

    async fn listen_addresses(&self) -> std::result::Result<Vec<String>, String> {
        Err(self.error.clone())
    }

    async fn connected_peers(&self) -> std::result::Result<Vec<String>, String> {
        Err(self.error.clone())
    }

    async fn connect_peer(&self, _addr: &str) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn get_replicators(&self) -> std::result::Result<Vec<ReplicatorInfo>, String> {
        Err(self.error.clone())
    }

    async fn add_replicator(
        &self,
        _collections: Vec<String>,
        _addr: Option<&str>,
    ) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn remove_replicator(
        &self,
        _collections: Vec<String>,
        _addr: Option<&str>,
    ) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn get_collections(&self) -> std::result::Result<Vec<String>, String> {
        Err(self.error.clone())
    }

    async fn add_collections(&self, _collections: Vec<String>) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn remove_collections(
        &self,
        _collections: Vec<String>,
    ) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn get_documents(&self) -> std::result::Result<Vec<P2pDocumentInfo>, String> {
        Err(self.error.clone())
    }

    async fn add_documents(
        &self,
        _docs: Vec<P2pDocumentRequest>,
    ) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn remove_documents(
        &self,
        _docs: Vec<P2pDocumentRequest>,
    ) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn sync_collections(&self) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }

    async fn sync_documents(&self) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }
}

/// Mock ACP operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockAcpOperations {
    error: String,
}

impl FailingMockAcpOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl AcpOperations for FailingMockAcpOperations {
    async fn add_policy(&self, _policy: &str) -> std::result::Result<String, String> {
        Err(self.error.clone())
    }

    async fn list_policies(&self) -> std::result::Result<Vec<PolicyInfo>, String> {
        Err(self.error.clone())
    }

    async fn get_policy(&self, _id: &str) -> std::result::Result<Option<PolicyInfo>, String> {
        Err(self.error.clone())
    }
}

/// Mock index operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockIndexOperations {
    error: String,
}

impl FailingMockIndexOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl IndexOperations for FailingMockIndexOperations {
    async fn create_index(
        &self,
        _collection: &str,
        _fields: Vec<String>,
        _name: Option<&str>,
        _unique: bool,
    ) -> std::result::Result<IndexInfo, String> {
        Err(self.error.clone())
    }

    async fn list_indexes(
        &self,
        _collection: Option<&str>,
    ) -> std::result::Result<Vec<IndexInfo>, String> {
        Err(self.error.clone())
    }

    async fn drop_index(&self, _collection: &str, _name: &str) -> std::result::Result<(), String> {
        Err(self.error.clone())
    }
}

/// Mock backup operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockBackupOperations {
    error: String,
}

impl FailingMockBackupOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl BackupOperations for FailingMockBackupOperations {
    async fn export(
        &self,
        _collections: Option<Vec<String>>,
        _pretty: bool,
    ) -> std::result::Result<String, String> {
        Err(self.error.clone())
    }

    async fn import(&self, _data: &str) -> std::result::Result<ImportResult, String> {
        Err(self.error.clone())
    }
}

// ============================================================================
// Mock NAC Operations
// ============================================================================

use crate::router::{NacStatus, NodeAcpOperations, NodePermission};

/// Mock NAC operations for testing NAC-protected handlers.
///
/// Configurable mock that allows controlling:
/// - NAC status (enabled, disabled, not configured)
/// - Owner identity
/// - Admin identities
/// - Permission grants
#[derive(Debug)]
pub struct MockNodeAcpOperations {
    status: Arc<RwLock<NacStatus>>,
    owner: Arc<RwLock<Option<Did>>>,
    admins: Arc<RwLock<Vec<Did>>>,
    /// Permission grants: (identity, permission) pairs
    grants: Arc<RwLock<Vec<(Did, NodePermission)>>>,
}

impl Clone for MockNodeAcpOperations {
    fn clone(&self) -> Self {
        Self {
            status: Arc::clone(&self.status),
            owner: Arc::clone(&self.owner),
            admins: Arc::clone(&self.admins),
            grants: Arc::clone(&self.grants),
        }
    }
}

impl Default for MockNodeAcpOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockNodeAcpOperations {
    /// Create a new mock NAC with NAC not configured (permissive).
    pub fn new() -> Self {
        Self {
            status: Arc::new(RwLock::new(NacStatus::NotConfigured)),
            owner: Arc::new(RwLock::new(None)),
            admins: Arc::new(RwLock::new(vec![])),
            grants: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Create with NAC enabled and the given owner.
    pub fn enabled_with_owner(owner: Did) -> Self {
        Self {
            status: Arc::new(RwLock::new(NacStatus::Enabled)),
            owner: Arc::new(RwLock::new(Some(owner))),
            admins: Arc::new(RwLock::new(vec![])),
            grants: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Create with NAC disabled temporarily.
    pub fn disabled() -> Self {
        Self {
            status: Arc::new(RwLock::new(NacStatus::DisabledTemporarily)),
            owner: Arc::new(RwLock::new(None)),
            admins: Arc::new(RwLock::new(vec![])),
            grants: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Add an admin identity.
    pub fn with_admin(self, admin: Did) -> Self {
        self.admins.write().unwrap().push(admin);
        self
    }

    /// Add a permission grant.
    pub fn with_grant(self, identity: Did, permission: NodePermission) -> Self {
        self.grants.write().unwrap().push((identity, permission));
        self
    }
}

#[async_trait]
impl NodeAcpOperations for MockNodeAcpOperations {
    async fn check_permission(
        &self,
        identity: &Did,
        permission: NodePermission,
    ) -> std::result::Result<bool, String> {
        let status = *self.status.read().unwrap();

        // If NAC is not enabled, allow all
        if status != NacStatus::Enabled {
            return Ok(true);
        }

        // Check if owner
        if let Some(owner) = self.owner.read().unwrap().as_ref() {
            if owner == identity {
                return Ok(true);
            }
        }

        // Check if admin (admins have all permissions)
        if self.admins.read().unwrap().contains(identity) {
            return Ok(true);
        }

        // Check specific permission grants
        let grants = self.grants.read().unwrap();
        for (grantee, perm) in grants.iter() {
            if grantee == identity && *perm == permission {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn get_status(&self) -> NacStatus {
        *self.status.read().unwrap()
    }

    async fn owner(&self) -> Option<Did> {
        self.owner.read().unwrap().clone()
    }

    async fn is_admin(&self, identity: &Did) -> std::result::Result<bool, String> {
        let status = *self.status.read().unwrap();
        if status != NacStatus::Enabled {
            return Ok(true); // Everyone is admin when NAC is disabled
        }

        // Check if owner
        if let Some(owner) = self.owner.read().unwrap().as_ref() {
            if owner == identity {
                return Ok(true);
            }
        }

        // Check admins list
        Ok(self.admins.read().unwrap().contains(identity))
    }

    async fn add_admin(&self, _requestor: &Did, target: &Did) -> std::result::Result<bool, String> {
        let status = *self.status.read().unwrap();
        if status == NacStatus::DisabledTemporarily {
            return Err(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            );
        }

        let mut admins = self.admins.write().unwrap();
        if admins.contains(target) {
            return Ok(false);
        }
        admins.push(target.clone());
        Ok(true)
    }

    async fn remove_admin(
        &self,
        _requestor: &Did,
        target: &Did,
    ) -> std::result::Result<bool, String> {
        let status = *self.status.read().unwrap();
        if status == NacStatus::DisabledTemporarily {
            return Err(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            );
        }

        // Cannot remove owner
        if let Some(owner) = self.owner.read().unwrap().as_ref() {
            if owner == target {
                return Err("cannot remove owner's admin access".into());
            }
        }

        let mut admins = self.admins.write().unwrap();
        let initial_len = admins.len();
        admins.retain(|a| a != target);
        Ok(admins.len() < initial_len)
    }
}

/// Mock NAC operations that always fails with a configurable error.
///
/// Use this to test error handling paths in handlers when NAC
/// permission checks fail with internal errors (not just permission denied).
#[derive(Debug, Clone)]
pub struct FailingMockNodeAcpOperations {
    error: String,
}

impl FailingMockNodeAcpOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl NodeAcpOperations for FailingMockNodeAcpOperations {
    async fn check_permission(
        &self,
        _identity: &Did,
        _permission: NodePermission,
    ) -> std::result::Result<bool, String> {
        Err(self.error.clone())
    }

    async fn get_status(&self) -> NacStatus {
        NacStatus::Enabled
    }

    async fn owner(&self) -> Option<Did> {
        None
    }

    async fn is_admin(&self, _identity: &Did) -> std::result::Result<bool, String> {
        Err(self.error.clone())
    }

    async fn add_admin(
        &self,
        _requestor: &Did,
        _target: &Did,
    ) -> std::result::Result<bool, String> {
        Err(self.error.clone())
    }

    async fn remove_admin(
        &self,
        _requestor: &Did,
        _target: &Did,
    ) -> std::result::Result<bool, String> {
        Err(self.error.clone())
    }
}
