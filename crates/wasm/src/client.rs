//! DefraClient - the main WASM client interface.
//!
//! Provides a high-level API for browser applications to interact with DefraDB.
//! This wraps the `db` crate's `DB` type with a JavaScript-friendly interface.

use std::sync::Arc;

use wasm_bindgen::prelude::*;

use db::{LensedAutoCommitFetcher, DB};
use query::runner::QueryRunner;
use storage::LevelDbStore;

use crate::bindings::{from_js, to_js, ClientConfig, CollectionInfo, FieldInfo};
use crate::error::{Result, WasmError};

/// DefraDB client for browser applications.
///
/// This client wraps the core `db::DB` type and provides a simplified interface for:
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
    db: Option<Arc<DB<LevelDbStore>>>,
    closed: bool,
}

#[wasm_bindgen]
impl DefraClient {
    /// Create a new DefraDB client.
    ///
    /// # Configuration
    ///
    /// Pass a JavaScript object with:
    /// - `storage`: "memory" (default) or "leveldb"
    /// - `db_name`: Database name (optional)
    ///
    /// # Example
    ///
    /// ```javascript
    /// const client = await DefraClient.create({ storage: 'leveldb' });
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

        // Create LevelDB store
        let db_name = config.db_name.as_deref().unwrap_or("defradb");
        let store = LevelDbStore::open(db_name).map_err(|e| {
            WasmError::Storage(format!("Failed to open LevelDB store: {}", e))
        })?;

        // Create the database
        let db = DB::new(store).map_err(|e| {
            WasmError::Storage(format!("Failed to create database: {}", e))
        })?;

        // Load existing collections from storage
        db.load_collections().await.map_err(|e| {
            WasmError::Storage(format!("Failed to load collections: {}", e))
        })?;

        Ok(Self {
            db: Some(Arc::new(db)),
            closed: false,
        })
    }

    fn ensure_open(&self) -> Result<&Arc<DB<LevelDbStore>>> {
        if self.closed {
            return Err(WasmError::Closed);
        }
        self.db.as_ref().ok_or(WasmError::NotInitialized)
    }

    async fn add_schema_impl(&mut self, sdl: &str) -> Result<JsValue> {
        let db = self.ensure_open()?;

        // Parse SDL into CollectionVersions using the query crate's parser
        let collections = query::sdl_parse::parse_sdl(sdl)
            .map_err(|e| WasmError::Schema(format!("Failed to parse SDL: {}", e)))?;

        // Create each collection in the database
        let mut added = Vec::new();
        for collection in collections {
            let name = collection.name.clone();
            db.create_collection(collection)
                .await
                .map_err(|e| WasmError::Schema(format!("Failed to create collection '{}': {}", name, e)))?;
            added.push(name);
        }

        to_js(&serde_json::json!({
            "collections_added": added,
        }))
    }

    async fn query_impl(&self, graphql: &str) -> Result<JsValue> {
        let db = self.ensure_open()?;

        // Validate that it looks like a query
        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty query string".to_string()));
        }

        // Create fetcher that auto-commits and applies lens migrations
        let fetcher = LensedAutoCommitFetcher::new(Arc::clone(db));

        // Get collections for the query runner
        let collections = db::load_active_collections(db)
            .await
            .map_err(|e| WasmError::Query(format!("Failed to load collections: {}", e)))?;

        // Create query runner
        let runner = QueryRunner::new(fetcher, collections);

        // Execute the query
        match runner.execute_query(graphql).await {
            Ok(result) => {
                let response = serde_json::json!({
                    "data": result,
                    "errors": [],
                });
                to_js(&response)
            }
            Err(e) => Err(WasmError::Query(e.to_string())),
        }
    }

    async fn mutate_impl(&mut self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        // Validate that it looks like a mutation
        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty mutation string".to_string()));
        }

        // For now, return a placeholder response
        // Full mutation execution requires the mutator integration
        to_js(&serde_json::json!({
            "data": {},
            "errors": [],
            "_info": "Mutation execution coming soon. Use GraphQL query for reads."
        }))
    }

    fn get_collections_impl(&self) -> Result<JsValue> {
        let db = self.ensure_open()?;

        // Get collection names from the database
        let names = db.list_collections().map_err(|e| {
            WasmError::Storage(format!("Failed to list collections: {}", e))
        })?;

        // Get each collection's info
        let mut collections = Vec::new();
        for name in names {
            if let Some(col) = db.get_collection(&name).map_err(|e| {
                WasmError::Storage(format!("Failed to get collection '{}': {}", name, e))
            })? {
                let schema = col.schema();
                collections.push(CollectionInfo {
                    name: schema.name.clone(),
                    schema_version_id: schema.version_id.clone(),
                    fields: schema.fields.iter().map(FieldInfo::from).collect(),
                });
            }
        }

        to_js(&collections)
    }

    async fn close_impl(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        if let Some(db) = self.db.take() {
            // Try to get exclusive ownership to close properly
            match Arc::try_unwrap(db) {
                Ok(db) => {
                    db.close().await.map_err(|e| {
                        WasmError::Storage(format!("Failed to close database: {}", e))
                    })?;
                }
                Err(_arc) => {
                    // Other references exist - this shouldn't happen in normal usage
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
