//! Trait implementations bridging DB to the embedded node's type-erased traits.

use std::sync::Arc;

use identity::Did;
use lens::{LensConfig, TransformId};
use schema::CollectionVersion;

use crate::SchemaOps;

pub(crate) struct DbSchemaOps<S: storage::corekv::Store + 'static> {
    database: Arc<db::DB<S>>,
    query_limits: query::QueryLimits,
    document_acp: Arc<dyn acp::DocumentACP>,
}

impl<S: storage::corekv::Store + 'static> DbSchemaOps<S> {
    pub(crate) fn new(
        database: Arc<db::DB<S>>,
        query_limits: query::QueryLimits,
        document_acp: Arc<dyn acp::DocumentACP>,
    ) -> Self {
        Self {
            database,
            query_limits,
            document_acp,
        }
    }

    /// Resolve the ambient identity into a creator DID. Returns `Ok(None)` when
    /// no identity is present (collection stays public), but fails closed when an
    /// identity string is present yet malformed, so a permissioned collection is
    /// never silently created unregistered.
    fn current_identity() -> anyhow::Result<Option<Did>> {
        match defra_core::current_identity::try_get_scoped_identity()
            .or_else(defra_core::current_identity::get_current_identity)
        {
            Some(raw) => {
                Ok(Some(Did::new(raw).map_err(|e| {
                    anyhow::anyhow!("malformed ambient identity: {}", e)
                })?))
            }
            None => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl<S: storage::corekv::Store + 'static> SchemaOps for DbSchemaOps<S> {
    async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        let creator = Self::current_identity()?;
        self.database
            .check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await
            .map_err(anyhow::Error::new)?;

        let collections =
            query::parse_sdl(sdl).map_err(|e| anyhow::anyhow!("SDL parse error: {}", e))?;

        schema::definition_validation::validate_new_collections(&collections)
            .map_err(|e| anyhow::anyhow!("schema validation error: {}", e))?;

        self.database
            .create_collections_atomic_with_acp_registration(
                collections,
                self.document_acp.clone(),
                creator,
            )
            .await
            .map_err(|e| anyhow::anyhow!("create collection error: {}", e))?;
        Ok(())
    }

    async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::ViewAdd)
            .await
            .map_err(anyhow::Error::new)?;

        let known_types: std::collections::HashSet<String> =
            db::DB::list_collections(&self.database)
                .unwrap_or_default()
                .into_iter()
                .collect();

        let mut collections = query::parse_sdl_with_known_types(target_sdl, known_types)
            .map_err(|e| anyhow::anyhow!("view SDL parse error: {}", e))?;

        let wrapped_query = format!("query {{ {} }}", source_query.trim());
        let selects = query::parse_query_with_limits(&wrapped_query, None, self.query_limits)
            .map_err(|e| anyhow::anyhow!("view query parse error: {}", e))?;
        let select = selects
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid view query: no selections found"))?;
        let query_source = schema::QuerySource::new(query::select_to_go_json(&select));

        let mut materialized_names = Vec::new();
        let mut downsample_names = Vec::new();

        for collection in &mut collections {
            if collection.downsample_interval.is_some() {
                collection.downsample_source = Some(query_source.clone());
                self.database
                    .validate_downsample_collection(collection)
                    .map_err(|e| anyhow::anyhow!("invalid downsample definition: {}", e))?;
            } else {
                collection.query = Some(query_source.clone());
            }

            if collection.query.is_some()
                && collection.is_materialized
                && !collection.is_embedded_only
            {
                materialized_names.push(collection.name.clone());
            }

            if collection.downsample_interval.is_some() {
                downsample_names.push(collection.name.clone());
            }
        }

        let creator = Self::current_identity()?;
        self.database
            .create_collections_atomic_with_acp_registration(
                collections,
                self.document_acp.clone(),
                creator,
            )
            .await
            .map_err(|e| anyhow::anyhow!("create view collection error: {}", e))?;

        if !materialized_names.is_empty() {
            self.database
                .refresh_views(Some(db::RefreshViewsOptions::with_names(
                    materialized_names,
                )))
                .await
                .map_err(|e| anyhow::anyhow!("refresh materialized views error: {}", e))?;
        }

        if !downsample_names.is_empty() {
            self.database
                .bootstrap_downsamples(Some(&downsample_names))
                .await
                .map_err(|e| anyhow::anyhow!("bootstrap downsample collections error: {}", e))?;
        }

        Ok(())
    }

    async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> anyhow::Result<CollectionVersion> {
        db::DB::patch_collection(&self.database, collection_name, patch, None)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn set_active_collection_version(&self, version_id: &str) -> anyhow::Result<()> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await
            .map_err(anyhow::Error::new)?;

        db::DB::set_active_collection_version(&self.database, version_id)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId> {
        db::DB::set_migration(&self.database, config, None)
            .await
            .map_err(anyhow::Error::new)
    }

    fn list_collections(&self) -> anyhow::Result<Vec<String>> {
        db::DB::list_collections(&self.database).map_err(anyhow::Error::new)
    }

    fn get_collection(&self, name: &str) -> anyhow::Result<Option<CollectionVersion>> {
        Ok(db::DB::get_collection(&self.database, name)
            .map_err(anyhow::Error::new)?
            .map(|collection| collection.schema().clone()))
    }

    async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> anyhow::Result<Option<CollectionVersion>> {
        Ok(
            db::DB::get_collection_by_version_id_full(&self.database, version_id)
                .await
                .map_err(anyhow::Error::new)?
                .map(|collection| collection.schema().clone()),
        )
    }

    async fn get_all_collection_versions(&self) -> anyhow::Result<Vec<CollectionVersion>> {
        db::DB::get_all_collection_versions(&self.database)
            .await
            .map_err(anyhow::Error::new)
    }
}
