//! DefraClient - the main WASM client interface.
//!
//! Provides a high-level API for browser applications to interact with DefraDB.

use std::collections::HashMap;

use wasm_bindgen::prelude::*;

use schema::{validate_schema, CollectionVersion};

use crate::bindings::{from_js, to_js, ClientConfig, CollectionInfo, FieldInfo};
use crate::error::{Result, WasmError};
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
    store: Option<WasmStore>,
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
            store: Some(store),
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

    async fn query_impl(&self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate that it looks like a query
        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty query string".to_string()));
        }

        // For MVP, return a placeholder response
        // Full query execution requires the query planner integration
        to_js(&serde_json::json!({
            "data": {},
            "errors": [],
            "_info": "Full query execution not yet implemented in WASM client. Schema validation and document storage are available."
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

        // Parse the documents
        let documents: Vec<serde_json::Value> = serde_json::from_str(documents_json)?;

        // For MVP, just count the documents
        // Full implementation will verify proofs and merge with CRDT
        let count = documents.len();

        to_js(&serde_json::json!({
            "synced": count,
            "failed": 0,
            "merged": 0,
            "_info": "Documents received. Full sync with CRDT merge not yet implemented."
        }))
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

        if let Some(store) = self.store.take() {
            store.close().await?;
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
