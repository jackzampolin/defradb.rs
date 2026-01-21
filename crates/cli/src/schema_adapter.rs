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
        // Parse SDL into CollectionVersions
        let collections =
            query::sdl_parse::parse_sdl(sdl).map_err(|e| format!("failed to parse SDL: {}", e))?;

        let mut created = Vec::new();

        // Create each collection in the database
        for collection in collections {
            // Clone before moving into create_collection
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
