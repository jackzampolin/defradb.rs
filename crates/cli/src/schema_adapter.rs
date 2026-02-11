//! Adapter to bridge database schema operations to HTTP's SchemaOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::SchemaOperations;
use schema::CollectionVersion;
use storage::corekv::Store;

/// Adapter that implements SchemaOperations using database.
pub struct SchemaAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> SchemaAdapter<S> {
    /// Create a new adapter wrapping the given database.
    pub fn new(database: Arc<db::DB<S>>) -> Self {
        Self { database }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn SchemaOperations> {
        Arc::new(Self::new(database))
    }
}

#[async_trait]
impl<S: Store + 'static> SchemaOperations for SchemaAdapter<S> {
    async fn add_schema(&self, sdl: &str) -> Result<Vec<CollectionVersion>, String> {
        // Get existing collection names so the parser can resolve external type references
        let known_types: std::collections::HashSet<String> = self
            .database
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // Parse SDL with known types for cross-collection reference resolution
        let collections = query::parse_sdl_with_known_types(sdl, known_types)
            .map_err(|e| format!("failed to parse SDL: {}", e))?;

        // Run global validators (embedding type checks, index field checks, etc.)
        db::definition_validation::validate_new_collections(&collections)
            .map_err(|e| format!("failed to validate schema: {}", e))?;

        let mut created = Vec::new();

        // Create each collection in the database
        for collection in collections {
            let col_clone = collection.clone();
            self.database
                .create_collection(collection)
                .await
                .map_err(|e| format!("failed to create collection: {}", e))?;
            created.push(col_clone);
        }

        Ok(created)
    }
}
