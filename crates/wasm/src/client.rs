//! DefraClient - the main WASM client interface.
//!
//! Provides a high-level API for browser applications to interact with DefraDB.
//! This wraps the `db` crate's `DB` type with a JavaScript-friendly interface.

use std::sync::Arc;

use wasm_bindgen::prelude::*;

use db::{AutoCommitMutator, DbCollectionProvider, LensedAutoCommitFetcher, DB};
use query::runner::QueryRunner;
use storage::LevelDbStore;

type WasmRunner = QueryRunner<LensedAutoCommitFetcher<LevelDbStore>>;

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
    runner: Option<WasmRunner>,
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
    pub async fn mutate(&self, graphql: &str) -> std::result::Result<JsValue, JsValue> {
        self.mutate_impl(graphql).await.map_err(|e| e.into())
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
        let store = LevelDbStore::open_with_opfs(db_name)
            .await
            .map_err(|e| WasmError::Storage(format!("Failed to open LevelDB store: {}", e)))?;

        // Create the database
        let db = DB::new(store)
            .map_err(|e| WasmError::Storage(format!("Failed to create database: {}", e)))?;

        // Load existing collections from storage
        db.load_collections()
            .await
            .map_err(|e| WasmError::Storage(format!("Failed to load collections: {}", e)))?;

        let db = Arc::new(db);

        let fetcher = LensedAutoCommitFetcher::new(Arc::clone(&db));
        let provider = DbCollectionProvider::new_arc(Arc::clone(&db));
        let mutator = Arc::new(AutoCommitMutator::new(Arc::clone(&db)));
        let runner = QueryRunner::with_provider(fetcher, provider).with_mutator(mutator);

        Ok(Self {
            db: Some(db),
            runner: Some(runner),
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
        db.store()
            .persist()
            .await
            .map_err(|e| WasmError::Storage(format!("Persist failed: {}", e)))?;
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
            db.create_collection(collection).await.map_err(|e| {
                WasmError::Schema(format!("Failed to create collection '{}': {}", name, e))
            })?;
            added.push(name);
        }

        // Persist to OPFS so schema definitions survive page refresh
        self.persist_impl().await?;

        to_js(&serde_json::json!({
            "collections_added": added,
        }))
    }

    async fn query_impl(&self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty query string".to_string()));
        }

        let runner = self.runner.as_ref().ok_or(WasmError::NotInitialized)?;

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

    async fn mutate_impl(&self, graphql: &str) -> Result<JsValue> {
        self.ensure_open()?;

        if graphql.trim().is_empty() {
            return Err(WasmError::Query("Empty mutation string".to_string()));
        }

        let result = self
            .runner
            .as_ref()
            .ok_or(WasmError::NotInitialized)?
            .execute_mutation(graphql)
            .await;

        match result {
            Ok(data) => {
                let response = serde_json::json!({
                    "data": data,
                    "errors": [],
                });
                to_js(&response)
            }
            Err(e) => Err(WasmError::Query(e.to_string())),
        }
    }

    fn get_collections_impl(&self) -> Result<JsValue> {
        let db = self.ensure_open()?;

        // Get collection names from the database
        let names = db
            .list_collections()
            .map_err(|e| WasmError::Storage(format!("Failed to list collections: {}", e)))?;

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

        // Drop runner first — it holds Arc<DB> refs via fetcher, mutator, and provider
        self.runner = None;

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

    wasm_bindgen_test_configure!(run_in_browser);

    /// Create a client with a unique db name to isolate tests from OPFS state.
    fn test_config(name: &str) -> JsValue {
        serde_wasm_bindgen::to_value(&ClientConfig {
            db_name: Some(name.to_string()),
        })
        .unwrap()
    }

    #[wasm_bindgen_test]
    async fn test_client_creation() {
        let client = DefraClient::create(test_config("test_creation"))
            .await
            .unwrap();
        assert!(!client.closed);
    }

    #[wasm_bindgen_test]
    async fn test_close_is_idempotent() {
        let mut client = DefraClient::create(test_config("test_close_idem"))
            .await
            .unwrap();
        client.close().await.unwrap();
        assert!(client.closed);
        client.close().await.unwrap();
    }

    #[wasm_bindgen_test]
    async fn test_query_after_close_fails() {
        let mut client = DefraClient::create(test_config("test_query_closed"))
            .await
            .unwrap();
        client.close().await.unwrap();
        let result = client.query("{ User { name } }").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_persist_after_close_fails() {
        let mut client = DefraClient::create(test_config("test_persist_closed"))
            .await
            .unwrap();
        client.close().await.unwrap();
        let result = client.persist().await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_mutate_no_schema_fails() {
        let mut client = DefraClient::create(test_config("test_mutate_no_schema"))
            .await
            .unwrap();
        let result = client
            .mutate(r#"mutation { add_User(input: {name: "Alice"}) { _docID } }"#)
            .await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_empty_mutation_fails() {
        let mut client = DefraClient::create(test_config("test_empty_mut"))
            .await
            .unwrap();
        let result = client.mutate("").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_mutate_after_close_fails() {
        let mut client = DefraClient::create(test_config("test_mut_closed"))
            .await
            .unwrap();
        client.close().await.unwrap();
        let result = client
            .mutate(r#"mutation { add_User(input: {name: "Alice"}) { _docID } }"#)
            .await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_empty_query_fails() {
        let client = DefraClient::create(test_config("test_empty_q"))
            .await
            .unwrap();
        let result = client.query("").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_empty_query_whitespace_fails() {
        let client = DefraClient::create(test_config("test_ws_q")).await.unwrap();
        let result = client.query("   ").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_fresh_db_has_no_user_collections() {
        let client = DefraClient::create(test_config("test_fresh_db"))
            .await
            .unwrap();
        let result = client.get_collections().unwrap();
        let collections: Vec<CollectionInfo> = serde_wasm_bindgen::from_value(result).unwrap();
        // A fresh DB should have no user-defined collections (may have system ones)
        let user_types: Vec<_> = collections
            .iter()
            .filter(|c| !c.name.starts_with('_'))
            .collect();
        assert!(
            user_types.is_empty(),
            "Fresh DB should have no user collections, found: {:?}",
            user_types.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[wasm_bindgen_test]
    async fn test_add_schema_and_get_collections() {
        let mut client = DefraClient::create(test_config("test_add_schema"))
            .await
            .unwrap();

        let sdl = "type User { name: String, email: String }";
        client.add_schema(sdl).await.unwrap();

        let result = client.get_collections().unwrap();
        let collections: Vec<CollectionInfo> = serde_wasm_bindgen::from_value(result).unwrap();
        let user_col = collections.iter().find(|c| c.name == "User");
        assert!(user_col.is_some(), "User collection should exist");
        assert!(user_col.unwrap().fields.len() >= 2);
    }

    #[wasm_bindgen_test]
    async fn test_add_schema_invalid_sdl_fails() {
        let mut client = DefraClient::create(test_config("test_bad_sdl"))
            .await
            .unwrap();
        let result = client.add_schema("not valid graphql {{{{").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_add_duplicate_schema_fails() {
        let mut client = DefraClient::create(test_config("test_dup_schema"))
            .await
            .unwrap();

        let sdl = "type Item { name: String }";
        client.add_schema(sdl).await.unwrap();
        let result = client.add_schema(sdl).await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_add_multiple_schemas() {
        let mut client = DefraClient::create(test_config("test_multi_schema"))
            .await
            .unwrap();

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
        let names: Vec<&str> = collections.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"Book"), "Book collection should exist");
        assert!(names.contains(&"Author"), "Author collection should exist");
    }

    #[wasm_bindgen_test]
    async fn test_query_empty_collection() {
        let mut client = DefraClient::create(test_config("test_query_empty"))
            .await
            .unwrap();

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
        let mut client = DefraClient::create(test_config("test_persist_ok"))
            .await
            .unwrap();

        client
            .add_schema("type Note { text: String }")
            .await
            .unwrap();

        client.persist().await.unwrap();
    }

    // --- Mutation integration tests ---

    #[wasm_bindgen_test]
    async fn test_create_and_query_document() {
        let mut client = DefraClient::create(test_config("test_create_query"))
            .await
            .unwrap();
        client
            .add_schema("type User { name: String, age: Int }")
            .await
            .unwrap();

        let result = client
            .mutate(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
            .await
            .unwrap();
        let response: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        assert!(
            response.get("data").is_some(),
            "Mutation should return data"
        );

        let result = client.query("{ User { name age } }").await.unwrap();
        let response: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let data_str = response["data"].to_string();
        assert!(
            data_str.contains("Alice"),
            "Query should find Alice, got: {}",
            data_str
        );
        assert!(
            data_str.contains("30"),
            "Query should find age 30, got: {}",
            data_str
        );
    }

    #[wasm_bindgen_test]
    async fn test_create_multiple_documents() {
        let mut client = DefraClient::create(test_config("test_create_multi"))
            .await
            .unwrap();
        client
            .add_schema("type Person { name: String }")
            .await
            .unwrap();

        client
            .mutate(r#"mutation { add_Person(input: {name: "Bob"}) { _docID } }"#)
            .await
            .unwrap();
        client
            .mutate(r#"mutation { add_Person(input: {name: "Carol"}) { _docID } }"#)
            .await
            .unwrap();

        let result = client.query("{ Person { name } }").await.unwrap();
        let response: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let data_str = response["data"].to_string();
        assert!(
            data_str.contains("Bob"),
            "Should find Bob, got: {}",
            data_str
        );
        assert!(
            data_str.contains("Carol"),
            "Should find Carol, got: {}",
            data_str
        );
    }

    #[wasm_bindgen_test]
    async fn test_create_returns_doc_id() {
        let mut client = DefraClient::create(test_config("test_create_docid"))
            .await
            .unwrap();
        client
            .add_schema("type Widget { label: String }")
            .await
            .unwrap();

        let result = client
            .mutate(r#"mutation { add_Widget(input: {label: "test"}) { _docID } }"#)
            .await
            .unwrap();
        let response: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let data_str = response["data"].to_string();
        assert!(
            data_str.contains("_docID"),
            "Mutation result should include _docID, got: {}",
            data_str
        );
    }

    #[wasm_bindgen_test]
    async fn test_create_persist_reopen_query() {
        // Create a doc, persist, close, reopen, and verify data survives
        let db_name = "test_create_persist_reopen";

        {
            let mut client = DefraClient::create(test_config(db_name)).await.unwrap();
            client
                .add_schema("type Task { title: String }")
                .await
                .unwrap();
            client
                .mutate(r#"mutation { add_Task(input: {title: "Survive"}) { _docID } }"#)
                .await
                .unwrap();
            // mutate_impl auto-persists, but explicit persist for clarity
            client.persist().await.unwrap();
            client.close().await.unwrap();
        }

        // Reopen the same database
        let client = DefraClient::create(test_config(db_name)).await.unwrap();

        let result = client.query("{ Task { title } }").await.unwrap();
        let response: serde_json::Value = serde_wasm_bindgen::from_value(result).unwrap();
        let data_str = response["data"].to_string();
        assert!(
            data_str.contains("Survive"),
            "Data should survive persist→close→reopen, got: {}",
            data_str
        );
    }
}
