//! Adapter to bridge database schema operations to HTTP and PG wire protocol.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::SchemaOperations;
use schema::CollectionVersion;
use storage::corekv::Store;

/// Adapter that implements schema operations using the database.
pub struct SchemaAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> SchemaAdapter<S> {
    /// Create a new adapter wrapping the given database.
    pub fn new(database: Arc<db::DB<S>>) -> Self {
        Self { database }
    }

    /// Create an Arc-wrapped adapter for HTTP schema operations.
    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn SchemaOperations> {
        Arc::new(Self::new(database))
    }

    /// Create an Arc-wrapped adapter for PG wire protocol schema operations.
    pub fn new_pg_arc(database: Arc<db::DB<S>>) -> Arc<dyn pg_compat::SchemaManager> {
        Arc::new(Self::new(database))
    }

    async fn add_schema_inner(&self, sdl: &str) -> Result<Vec<CollectionVersion>, String> {
        let known_types: std::collections::HashSet<String> = self
            .database
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        let collections = query::parse_sdl_with_known_types(sdl, known_types)
            .map_err(|e| format!("failed to parse SDL: {}", e))?;

        db::definition_validation::validate_new_collections(&collections)
            .map_err(|e| format!("failed to validate schema: {}", e))?;

        let mut created = Vec::new();
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

#[async_trait]
impl<S: Store + 'static> SchemaOperations for SchemaAdapter<S> {
    async fn add_schema(&self, sdl: &str) -> Result<Vec<CollectionVersion>, String> {
        self.add_schema_inner(sdl).await
    }
}

#[async_trait]
impl<S: Store + 'static> pg_compat::SchemaManager for SchemaAdapter<S> {
    async fn add_schema(&self, sdl: &str) -> Result<(), String> {
        self.add_schema_inner(sdl).await?;
        Ok(())
    }
}
