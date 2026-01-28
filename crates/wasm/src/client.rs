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

    /// Persist pending data to OPFS.
    ///
    /// Call this to flush in-memory LevelDB data to the browser's
    /// Origin Private File System. Without calling persist, data only
    /// lives in memory and will be lost if the tab is closed.
    ///
    /// # Example
    ///
    /// ```javascript
    /// // Persist on visibility change (tab going to background)
    /// document.addEventListener('visibilitychange', () => {
    ///     if (document.visibilityState === 'hidden') {
    ///         client.persist();
    ///     }
    /// });
    ///
    /// // Or on a timer
    /// setInterval(() => client.persist(), 10000);
    /// ```
    #[wasm_bindgen]
    pub async fn persist(&self) -> std::result::Result<(), JsValue> {
        self.persist_impl().await.map_err(|e| e.into())
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

        // Create LevelDB store with OPFS persistence
        let db_name = config.db_name.as_deref().unwrap_or("defradb");
        let store = LevelDbStore::open_with_opfs(db_name).await.map_err(|e| {
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

    async fn persist_impl(&self) -> Result<()> {
        let db = self.ensure_open()?;
        db.store().persist().await.map_err(|e| {
            WasmError::Storage(format!("Persist failed: {}", e))
        })?;
        Ok(())
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

        // Persist to OPFS so schema definitions survive page refresh
        self.persist_impl().await?;

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

    async fn mutate_impl(&mut self, _graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;
        Err(WasmError::Query(
            "Mutations are not yet supported. Use query() for reads.".to_string(),
        ))
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
            match Arc::try_unwrap(db) {
                Ok(db) => {
                    db.close().await.map_err(|e| {
                        WasmError::Storage(format!("Failed to close database: {}", e))
                    })?;
                }
                Err(arc) => {
                    // Other references exist — persist to avoid data loss, then drop our ref
                    web_sys::console::warn_1(
                        &format!(
                            "Cannot close DB exclusively ({} refs), persisting before release",
                            Arc::strong_count(&arc)
                        )
                        .into(),
                    );
                    arc.store().persist().await.map_err(|e| {
                        WasmError::Storage(format!("Persist failed during close: {}", e))
                    })?;
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

    #[wasm_bindgen_test]
    async fn test_close_is_idempotent() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();
        client.close().await.unwrap();
        assert!(client.closed);
        // Second close should succeed without error
        client.close().await.unwrap();
    }

    #[wasm_bindgen_test]
    async fn test_query_after_close_fails() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();
        client.close().await.unwrap();
        let result = client.query("{ User { name } }").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_persist_after_close_fails() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();
        client.close().await.unwrap();
        let result = client.persist().await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_mutate_returns_error() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();
        let result = client.mutate("mutation { create_User(input: {}) { _docID } }").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_empty_query_fails() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let client = DefraClient::create(config).await.unwrap();
        let result = client.query("").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_empty_query_whitespace_fails() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let client = DefraClient::create(config).await.unwrap();
        let result = client.query("   ").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_get_collections_empty() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let client = DefraClient::create(config).await.unwrap();
        let result = client.get_collections().unwrap();
        let collections: Vec<CollectionInfo> = serde_wasm_bindgen::from_value(result).unwrap();
        assert!(collections.is_empty());
    }

    #[wasm_bindgen_test]
    async fn test_add_schema_and_get_collections() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();

        let sdl = "type User { name: String, email: String }";
        client.add_schema(sdl).await.unwrap();

        let result = client.get_collections().unwrap();
        let collections: Vec<CollectionInfo> = serde_wasm_bindgen::from_value(result).unwrap();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].name, "User");
        // _docID is auto-added, so we expect name + email + _docID = 3 fields
        assert!(collections[0].fields.len() >= 2);
    }

    #[wasm_bindgen_test]
    async fn test_add_schema_invalid_sdl_fails() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();
        let result = client.add_schema("not valid graphql {{{{").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_add_duplicate_schema_fails() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();

        let sdl = "type Item { name: String }";
        client.add_schema(sdl).await.unwrap();
        // Adding the same schema again should fail (collection already exists)
        let result = client.add_schema(sdl).await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_add_multiple_schemas() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();

        client
            .add_schema("type Book { title: String }")
            .await
            .unwrap();
        client
            .add_schema("type Author { name: String }")
            .await
            .unwrap();

        let result = client.get_collections().unwrap();
        let collections: Vec<CollectionInfo> = serde_wasm_bindgen::from_value(result).unwrap();
        assert_eq!(collections.len(), 2);

        let names: Vec<&str> = collections.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"Book"));
        assert!(names.contains(&"Author"));
    }

    #[wasm_bindgen_test]
    async fn test_query_empty_collection() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();

        client
            .add_schema("type Product { name: String, price: Int }")
            .await
            .unwrap();

        let result = client.query("{ Product { name price } }").await.unwrap();
        let response: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        assert!(response.get("data").is_some());
    }

    #[wasm_bindgen_test]
    async fn test_persist_succeeds() {
        let config = serde_wasm_bindgen::to_value(&ClientConfig::default()).unwrap();
        let mut client = DefraClient::create(config).await.unwrap();

        client
            .add_schema("type Note { text: String }")
            .await
            .unwrap();

        // Explicit persist should succeed
        client.persist().await.unwrap();
    }
}
