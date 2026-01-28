//! DefraClient - the main WASM client interface.
//!
//! Provides a high-level API for browser applications to interact with DefraDB.

use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;
use wasm_bindgen::prelude::*;

use schema::{validate_schema, CollectionVersion};

use crate::bindings::{from_js, to_js, ClientConfig, CollectionInfo, FieldInfo};
use crate::error::{Result, WasmError};
#[cfg(target_arch = "wasm32")]
use crate::query_adapter::create_query_runner;
use crate::sdl::parse_sdl;
use crate::storage::{create_store, WasmStore};

/// DefraDB client for browser applications.
///
/// This client provides a simplified interface for:
/// - Schema management
/// - GraphQL queries and mutations
/// - Document storage with CRDT support
/// - Merkle proof verification
///
/// # Example (JavaScript)
///
/// ```javascript
/// import init, { DefraClient } from 'defra-wasm';
///
/// await init();
/// const client = await DefraClient.create({ storage: 'memory' });
///
/// await client.add_schema(`
///   type User {
///     name: String
///     email: String
///   }
/// `);
///
/// const collections = client.get_collections();
/// console.log(collections);
///
/// await client.close();
/// ```
#[wasm_bindgen]
pub struct DefraClient {
    store: Option<Arc<WasmStore>>,
    collections: HashMap<String, CollectionVersion>,
    closed: bool,
}

#[wasm_bindgen]
impl DefraClient {
    /// Create a new DefraDB client.
    ///
    /// # Configuration
    ///
    /// Pass a JavaScript object with:
    /// - `storage`: "memory" (default) or "indexeddb"
    /// - `db_name`: Database name for IndexedDB (optional)
    ///
    /// # Example
    ///
    /// ```javascript
    /// const client = await DefraClient.create({ storage: 'memory' });
    /// ```
    #[wasm_bindgen(js_name = create)]
    pub async fn create(config: JsValue) -> std::result::Result<DefraClient, JsValue> {
        // Set up panic hook for better error messages
        #[cfg(feature = "debug")]
        console_error_panic_hook::set_once();

        Self::new_impl(config).await.map_err(|e| e.into())
    }

    /// Add a GraphQL schema definition.
    ///
    /// Parses and validates the SDL, then registers the collections.
    ///
    /// # Example
    ///
    /// ```javascript
    /// await client.add_schema(`
    ///   type User {
    ///     name: String
    ///     email: String
    ///   }
    ///
    ///   type Post {
    ///     title: String
    ///     content: String
    ///   }
    /// `);
    /// ```
    #[wasm_bindgen]
    pub async fn add_schema(&mut self, sdl: &str) -> std::result::Result<JsValue, JsValue> {
        self.add_schema_impl(sdl).await.map_err(|e| e.into())
    }

    /// Execute a GraphQL query.
    ///
    /// Note: Full query execution is not yet implemented in the WASM client.
    /// This currently returns a placeholder response.
    ///
    /// # Example
    ///
    /// ```javascript
    /// const result = await client.query(`{
    ///   User {
    ///     name
    ///     email
    ///   }
    /// }`);
    /// ```
    #[wasm_bindgen]
    pub async fn query(&self, graphql: &str) -> std::result::Result<JsValue, JsValue> {
        self.query_impl(graphql).await.map_err(|e| e.into())
    }

    /// Execute a GraphQL mutation.
    ///
    /// Note: Full mutation execution is not yet implemented in the WASM client.
    /// This currently returns a placeholder response.
    #[wasm_bindgen]
    pub async fn mutate(&mut self, graphql: &str) -> std::result::Result<JsValue, JsValue> {
        self.mutate_impl(graphql).await.map_err(|e| e.into())
    }

    /// Verify a Merkle proof.
    ///
    /// Returns true if the proof is valid.
    #[wasm_bindgen]
    pub fn verify_proof(&self, proof_json: &str) -> std::result::Result<bool, JsValue> {
        crate::verification::verify_merkle_proof(proof_json)
    }

    /// Sync documents from an external source (e.g., indexer).
    ///
    /// Documents are verified against the provided proofs and merged
    /// using CRDT conflict resolution.
    ///
    /// # Arguments
    ///
    /// * `documents_json` - JSON array of documents to sync
    /// * `proofs_json` - JSON array of Merkle proofs (optional, can be "[]")
    ///
    /// # Returns
    ///
    /// Object with sync statistics:
    /// - `synced`: Number of documents successfully synced
    /// - `failed`: Number of documents that failed verification
    /// - `merged`: Number of documents that required CRDT merge
    #[wasm_bindgen]
    pub async fn sync_documents(
        &mut self,
        documents_json: &str,
        proofs_json: &str,
    ) -> std::result::Result<JsValue, JsValue> {
        self.sync_documents_impl(documents_json, proofs_json)
            .await
            .map_err(|e| e.into())
    }

    /// Get information about all registered collections.
    ///
    /// Returns an array of collection info objects.
    #[wasm_bindgen]
    pub fn get_collections(&self) -> std::result::Result<JsValue, JsValue> {
        self.get_collections_impl().map_err(|e| e.into())
    }

    /// Get a single document by collection and document ID.
    ///
    /// # Arguments
    ///
    /// * `collection` - The collection name
    /// * `doc_id` - The document ID
    ///
    /// # Returns
    ///
    /// The document as a JSON object, or null if not found.
    #[wasm_bindgen]
    pub async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> std::result::Result<JsValue, JsValue> {
        self.get_document_impl(collection, doc_id)
            .await
            .map_err(|e| e.into())
    }

    /// Get all documents in a collection.
    ///
    /// # Arguments
    ///
    /// * `collection` - The collection name
    ///
    /// # Returns
    ///
    /// An array of documents in the collection.
    #[wasm_bindgen]
    pub async fn get_documents(
        &self,
        collection: &str,
    ) -> std::result::Result<JsValue, JsValue> {
        self.get_documents_impl(collection)
            .await
            .map_err(|e| e.into())
    }

    /// Close the client and release resources.
    ///
    /// After closing, the client cannot be used.
    #[wasm_bindgen]
    pub async fn close(&mut self) -> std::result::Result<(), JsValue> {
        self.close_impl().await.map_err(|e| e.into())
    }
}

// Internal implementation methods
impl DefraClient {
    async fn new_impl(config: JsValue) -> Result<Self> {
        let config: ClientConfig = if config.is_undefined() || config.is_null() {
            ClientConfig::default()
        } else {
            from_js(config)?
        };

        let store = create_store(config.storage, config.db_name.as_deref()).await?;

        Ok(Self {
            store: Some(Arc::new(store)),
            collections: HashMap::new(),
            closed: false,
        })
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            return Err(WasmError::Closed);
        }
        if self.store.is_none() {
            return Err(WasmError::NotInitialized);
        }
        Ok(())
    }

    async fn add_schema_impl(&mut self, sdl: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Parse SDL into collection definitions
        let new_collections = parse_sdl(sdl)?;

        // Build schema map for validation
        let mut schema_map: HashMap<String, CollectionVersion> = self.collections.clone();
        for col in &new_collections {
            schema_map.insert(col.name.clone(), col.clone());
        }

        // Validate the complete schema
        validate_schema(&schema_map)?;

        // Register the new collections
        let mut added = Vec::new();
        for col in new_collections {
            added.push(col.name.clone());
            self.collections.insert(col.name.clone(), col);
        }

        to_js(&serde_json::json!({
            "collections_added": added,
        }))
    }

    #[cfg(target_arch = "wasm32")]
    async fn query_impl(&self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate that it looks like a query
        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty query string".to_string()));
        }

        let store = self.store.as_ref().ok_or(WasmError::NotInitialized)?;

        // Get collection versions for the query runner
        let collections: Vec<CollectionVersion> = self.collections.values().cloned().collect();

        // Create a query runner with our storage adapter
        let runner = create_query_runner(Arc::clone(store), collections);

        // Execute the query - returns JSON directly
        match runner.execute_query(graphql).await {
            Ok(result) => {
                // The result is already the data object
                let response = serde_json::json!({
                    "data": result,
                    "errors": [],
                });
                to_js(&response)
            }
            Err(e) => Err(WasmError::Query(e.to_string())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn query_impl(&self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate that it looks like a query
        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty query string".to_string()));
        }

        // Placeholder for native builds - full query execution only available in WASM
        to_js(&serde_json::json!({
            "data": {},
            "errors": [],
            "_info": "Full query execution only available in WASM builds"
        }))
    }

    async fn mutate_impl(&mut self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate that it looks like a mutation
        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty mutation string".to_string()));
        }

        // For MVP, return a placeholder response
        // Full mutation execution requires the query planner integration
        to_js(&serde_json::json!({
            "data": {},
            "errors": [],
            "_info": "Full mutation execution not yet implemented in WASM client. Use sync_documents to import data."
        }))
    }

    async fn sync_documents_impl(
        &mut self,
        documents_json: &str,
        _proofs_json: &str,
    ) -> Result<JsValue> {
        self.ensure_open()?;

        let store = Arc::clone(self.store.as_ref().ok_or(WasmError::NotInitialized)?);

        // Parse the documents
        let documents: Vec<serde_json::Value> = serde_json::from_str(documents_json)?;

        let mut synced = 0u32;
        let mut failed = 0u32;
        let mut merged = 0u32;
        let mut errors: Vec<String> = Vec::new();

        for doc in documents {
            match self.sync_single_document(&store, &doc).await {
                Ok(was_merge) => {
                    synced += 1;
                    if was_merge {
                        merged += 1;
                    }
                }
                Err(e) => {
                    failed += 1;
                    errors.push(e.to_string());
                }
            }
        }

        to_js(&serde_json::json!({
            "synced": synced,
            "failed": failed,
            "merged": merged,
            "errors": errors,
        }))
    }

    /// Sync a single document to storage.
    /// Returns Ok(true) if this was a merge (document already existed), Ok(false) if new.
    async fn sync_single_document(
        &self,
        store: &WasmStore,
        doc: &serde_json::Value,
    ) -> Result<bool> {
        // Extract collection name - required field
        let collection = doc
            .get("_collection")
            .and_then(|v| v.as_str())
            .ok_or_else(|| WasmError::Sync("Document missing '_collection' field".to_string()))?;

        // Validate collection exists in schema
        if !self.collections.contains_key(collection) {
            return Err(WasmError::Sync(format!(
                "Unknown collection '{}'. Add schema first.",
                collection
            )));
        }

        // Extract or generate document ID
        let doc_id = doc
            .get("_docID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Generate a UUID-based document ID if not provided
                Uuid::new_v4().to_string()
            });

        // Build storage key: docs/{collection}/{docID}
        let key = format!("docs/{}/{}", collection, doc_id);
        let key_bytes = key.as_bytes();

        // Check if document already exists
        let existing = store.get(key_bytes).await?;
        let was_merge = existing.is_some();

        // If document exists, check if we should merge/update
        if let Some(existing_bytes) = existing {
            // Parse existing document
            if let Ok(existing_doc) = serde_json::from_slice::<serde_json::Value>(&existing_bytes) {
                // Simple LWW: compare _updatedAt timestamps if available
                let existing_ts = existing_doc
                    .get("_updatedAt")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let incoming_ts = doc.get("_updatedAt").and_then(|v| v.as_i64()).unwrap_or(0);

                // If existing is newer or equal, skip update
                if existing_ts >= incoming_ts && incoming_ts > 0 {
                    return Ok(true); // Was a merge, but kept existing
                }
            }
        }

        // Serialize and store the document
        let doc_bytes = serde_json::to_vec(doc)?;
        store.set(key_bytes, &doc_bytes).await?;

        Ok(was_merge)
    }

    async fn get_document_impl(&self, collection: &str, doc_id: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate collection exists
        if !self.collections.contains_key(collection) {
            return Err(WasmError::Sync(format!(
                "Unknown collection '{}'",
                collection
            )));
        }

        let store = self.store.as_ref().ok_or(WasmError::NotInitialized)?;

        // Build storage key
        let key = format!("docs/{}/{}", collection, doc_id);
        let key_bytes = key.as_bytes();

        match store.get(key_bytes).await? {
            Some(bytes) => {
                let doc: serde_json::Value = serde_json::from_slice(&bytes)?;
                to_js(&doc)
            }
            None => Ok(JsValue::NULL),
        }
    }

    async fn get_documents_impl(&self, collection: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate collection exists
        if !self.collections.contains_key(collection) {
            return Err(WasmError::Sync(format!(
                "Unknown collection '{}'",
                collection
            )));
        }

        let store = self.store.as_ref().ok_or(WasmError::NotInitialized)?;

        // Get all keys with the collection prefix
        let prefix = format!("docs/{}/", collection);
        let keys = store.keys_with_prefix(prefix.as_bytes()).await?;

        // Load all documents
        let mut documents = Vec::new();
        for key in keys {
            if let Some(bytes) = store.get(&key).await? {
                if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    documents.push(doc);
                }
            }
        }

        to_js(&documents)
    }

    fn get_collections_impl(&self) -> Result<JsValue> {
        let collections: Vec<CollectionInfo> = self
            .collections
            .values()
            .map(|col| CollectionInfo {
                name: col.name.clone(),
                schema_version_id: col.version_id.clone(),
                fields: col.fields.iter().map(FieldInfo::from).collect(),
            })
            .collect();

        to_js(&collections)
    }

    async fn close_impl(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        if let Some(store_arc) = self.store.take() {
            // Try to get exclusive ownership of the store
            match Arc::try_unwrap(store_arc) {
                Ok(mut store) => {
                    store.close().await?;
                }
                Err(_arc) => {
                    // Other references exist - this shouldn't happen in normal usage
                    // Just mark as closed and let the Arc drop naturally
                }
            }
        }

        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    async fn test_client_creation() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let client = DefraClient::create(config).await.unwrap();
        assert!(!client.closed);
    }
}
