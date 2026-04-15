//! Trait implementations bridging DB to the embedded node's type-erased traits.

use std::sync::Arc;

use lens::{LensConfig, TransformId};
use schema::CollectionVersion;

use crate::SchemaOps;

#[async_trait::async_trait]
impl<S: storage::corekv::Store + 'static> SchemaOps for Arc<db::DB<S>> {
    async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        let collections =
            query::parse_sdl(sdl).map_err(|e| anyhow::anyhow!("SDL parse error: {}", e))?;

        schema::definition_validation::validate_new_collections(&collections)
            .map_err(|e| anyhow::anyhow!("schema validation error: {}", e))?;

        for collection in collections {
            db::DB::create_collection(self, collection)
                .await
                .map_err(|e| anyhow::anyhow!("create collection error: {}", e))?;
        }
        Ok(())
    }

    async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()> {
        let known_types: std::collections::HashSet<String> = db::DB::list_collections(self)
            .unwrap_or_default()
            .into_iter()
            .collect();

        let mut collections = query::parse_sdl_with_known_types(target_sdl, known_types)
            .map_err(|e| anyhow::anyhow!("view SDL parse error: {}", e))?;

        let wrapped_query = format!("query {{ {} }}", source_query.trim());
        let selects = query::parse_query(&wrapped_query)
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
                self.validate_downsample_collection(collection)
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

        self.create_collections_atomic(collections)
            .await
            .map_err(|e| anyhow::anyhow!("create view collection error: {}", e))?;

        if !materialized_names.is_empty() {
            self.refresh_views(Some(db::RefreshViewsOptions::with_names(
                materialized_names,
            )))
            .await
            .map_err(|e| anyhow::anyhow!("refresh materialized views error: {}", e))?;
        }

        if !downsample_names.is_empty() {
            self.bootstrap_downsamples(Some(&downsample_names))
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
        db::DB::patch_collection(self, collection_name, patch)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn set_active_collection_version(&self, version_id: &str) -> anyhow::Result<()> {
        db::DB::set_active_collection_version(self, version_id)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn set_migration(&self, config: LensConfig) -> anyhow::Result<TransformId> {
        db::DB::set_migration(self, config)
            .await
            .map_err(anyhow::Error::new)
    }

    fn list_collections(&self) -> anyhow::Result<Vec<String>> {
        db::DB::list_collections(self).map_err(anyhow::Error::new)
    }

    fn get_collection(&self, name: &str) -> anyhow::Result<Option<CollectionVersion>> {
        Ok(db::DB::get_collection(self, name)
            .map_err(anyhow::Error::new)?
            .map(|collection| collection.schema().clone()))
    }

    async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> anyhow::Result<Option<CollectionVersion>> {
        Ok(db::DB::get_collection_by_version_id_full(self, version_id)
            .await
            .map_err(anyhow::Error::new)?
            .map(|collection| collection.schema().clone()))
    }

    async fn get_all_collection_versions(&self) -> anyhow::Result<Vec<CollectionVersion>> {
        db::DB::get_all_collection_versions(self)
            .await
            .map_err(anyhow::Error::new)
    }
}

#[cfg(feature = "p2p")]
impl<S: storage::corekv::Store + 'static> crate::CollectionLookup for db::DB<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        match self.get_collection(name) {
            Ok(Some(collection)) => Some(collection.collection_id().to_string()),
            Ok(None) => {
                tracing::debug!(collection_name = %name, "collection not found for P2P lookup");
                None
            }
            Err(e) => {
                tracing::warn!(collection_name = %name, error = %e, "error looking up collection for P2P");
                None
            }
        }
    }
}
