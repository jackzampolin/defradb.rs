//! Adapter to bridge database schema operations to HTTP and PG wire protocol.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::{AcpOperations, SchemaOperations};
use schema::CollectionVersion;
use storage::corekv::Store;

/// Adapter that implements schema operations using the database.
pub struct SchemaAdapter<S: Store> {
    database: Arc<db::DB<S>>,
    /// Optional ACP handle for schema-time DRI validation (#746).
    ///
    /// When `Some`, `add_schema` validates `@policy(id:, resource:)`
    /// directives against the ACP store before creating collections:
    /// the policy must exist, the resource must exist on it, and the
    /// resource must declare the DPI-required `read`/`update`/`delete`
    /// permissions. When `None` (legacy / tests without ACP wired in),
    /// the validator is skipped.
    acp: Option<Arc<dyn AcpOperations>>,
}

impl<S: Store + 'static> SchemaAdapter<S> {
    /// Create a new adapter wrapping the given database.
    pub fn new(database: Arc<db::DB<S>>) -> Self {
        Self {
            database,
            acp: None,
        }
    }

    /// Create a new adapter with ACP wired in for schema-time DRI validation.
    pub fn new_with_acp(database: Arc<db::DB<S>>, acp: Arc<dyn AcpOperations>) -> Self {
        Self {
            database,
            acp: Some(acp),
        }
    }

    /// Create an Arc-wrapped adapter for HTTP schema operations.
    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn SchemaOperations> {
        Arc::new(Self::new(database))
    }

    /// Create an Arc-wrapped adapter for HTTP schema operations with ACP
    /// validation wired in.
    pub fn new_arc_with_acp(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn AcpOperations>,
    ) -> Arc<dyn SchemaOperations> {
        Arc::new(Self::new_with_acp(database, acp))
    }

    /// Create an Arc-wrapped adapter for PG wire protocol schema operations.
    ///
    /// Use `new_pg_arc_with_acp` instead when the caller has an
    /// `AcpOperations` handle — that variant enables schema-time DRI
    /// validation (#746). This bare constructor exists for tests and
    /// for embedded PG callers that don't have ACP configured.
    pub fn new_pg_arc(database: Arc<db::DB<S>>) -> Arc<dyn pg_compat::SchemaManager> {
        Arc::new(Self::new(database))
    }

    /// Create an Arc-wrapped PG schema manager with ACP wired in for
    /// schema-time DRI validation (#746). Use this when the PG server
    /// has access to an `AcpOperations` handle.
    pub fn new_pg_arc_with_acp(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn AcpOperations>,
    ) -> Arc<dyn pg_compat::SchemaManager> {
        Arc::new(Self::new_with_acp(database, acp))
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

        // #746: validate every @policy directive against the ACP store
        // before creating any collection. If any collection's DRI is
        // invalid (policy missing, resource missing, DPI perm missing),
        // reject the whole schema-add so we don't leave a half-created
        // state.
        if let Some(acp) = &self.acp {
            for collection in &collections {
                if let Some(policy) = &collection.policy {
                    acp.validate_resource_interface(&policy.id, &policy.resource_name)
                        .await
                        .map_err(|e| format!("failed to add collection: {}", e))?;
                }
            }
        }

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
