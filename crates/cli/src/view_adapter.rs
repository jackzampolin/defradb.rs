//! Adapter to bridge database view operations to HTTP's ViewOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::ViewOperations;
use identity::Did;
use schema::CollectionVersion;
use storage::corekv::Store;

/// Adapter that implements ViewOperations using the database.
pub struct ViewAdapter<S: Store> {
    database: Arc<db::DB<S>>,
    query_limits: query::QueryLimits,
    document_acp: Option<Arc<dyn acp::DocumentACP>>,
}

impl<S: Store + 'static> ViewAdapter<S> {
    /// Create a new adapter wrapping the given database and query limits.
    pub fn new(database: Arc<db::DB<S>>, query_limits: query::QueryLimits) -> Self {
        Self {
            database,
            query_limits,
            document_acp: None,
        }
    }

    /// Create a new adapter with document ACP registration support.
    pub fn new_with_acp(
        database: Arc<db::DB<S>>,
        query_limits: query::QueryLimits,
        document_acp: Arc<dyn acp::DocumentACP>,
    ) -> Self {
        Self {
            database,
            query_limits,
            document_acp: Some(document_acp),
        }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(
        database: Arc<db::DB<S>>,
        query_limits: query::QueryLimits,
    ) -> Arc<dyn ViewOperations> {
        Arc::new(Self::new(database, query_limits))
    }

    /// Create an Arc-wrapped adapter with document ACP registration support.
    pub fn new_arc_with_acp(
        database: Arc<db::DB<S>>,
        query_limits: query::QueryLimits,
        document_acp: Arc<dyn acp::DocumentACP>,
    ) -> Arc<dyn ViewOperations> {
        Arc::new(Self::new_with_acp(database, query_limits, document_acp))
    }
}

#[async_trait]
impl<S: Store + 'static> ViewOperations for ViewAdapter<S> {
    async fn add_view(
        &self,
        gql_query: &str,
        sdl: &str,
        transform: Option<&str>,
    ) -> Result<Vec<CollectionVersion>, String> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::ViewAdd)
            .await
            .map_err(|e| format!("{}", e))?;

        let known_types: std::collections::HashSet<String> = self
            .database
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        let collections = query::parse_sdl_with_known_types(sdl, known_types)
            .map_err(|e| format!("failed to parse view SDL: {}", e))?;

        let wrapped_query = format!("query {{ {} }}", gql_query);
        let selects = query::parse_query_with_limits(&wrapped_query, None, self.query_limits)
            .map_err(|e| format!("failed to parse view query: {}", e))?;
        if selects.is_empty() {
            return Err("invalid view query: no selections found".to_string());
        }
        let select_json = query::select_to_go_json(&selects[0]);

        if let Some(t) = transform {
            let lens_store = self.database.lens_store();
            for cid in t.split(',') {
                let cid = cid.trim();
                if !cid.is_empty() {
                    let tid = lens::TransformId::new(cid);
                    if !lens_store.has_transform(&tid) {
                        return Err("lens CID not found".to_string());
                    }
                }
            }
        }

        let mut query_source = schema::QuerySource::new(select_json);
        if let Some(t) = transform {
            query_source = query_source.with_transform(t);
        }

        let view_collections: Vec<_> = collections
            .into_iter()
            .map(|mut col_version| {
                if col_version.downsample_interval.is_some() {
                    col_version.downsample_source = Some(query_source.clone());
                } else {
                    col_version.query = Some(query_source.clone());
                }
                col_version
            })
            .collect();

        for collection in &view_collections {
            if collection.downsample_interval.is_some() {
                self.database
                    .validate_downsample_collection(collection)
                    .map_err(|e| format!("invalid downsample definition: {}", e))?;
            }
        }

        let creator = defra_core::current_identity::try_get_scoped_identity()
            .or_else(defra_core::current_identity::get_current_identity)
            .and_then(|did| Did::new(did).ok());
        let created_versions = if let Some(document_acp) = &self.document_acp {
            self.database
                .create_collections_atomic_with_acp_registration(
                    view_collections,
                    document_acp.clone(),
                    creator,
                )
                .await
        } else {
            self.database
                .create_collections_atomic(view_collections)
                .await
        }
        .map_err(|e| format!("failed to create view collection: {}", e))?;

        let materialized_names: Vec<String> = created_versions
            .iter()
            .filter(|col| col.query.is_some() && col.is_materialized && !col.is_embedded_only)
            .map(|col| col.name.clone())
            .collect();
        let downsample_names: Vec<String> = created_versions
            .iter()
            .filter(|col| col.downsample_interval.is_some())
            .map(|col| col.name.clone())
            .collect();

        if !materialized_names.is_empty() {
            self.database
                .refresh_views(Some(db::RefreshViewsOptions::with_names(
                    materialized_names,
                )))
                .await
                .map_err(|e| format!("failed to refresh materialized views: {}", e))?;
        }

        if !downsample_names.is_empty() {
            self.database
                .bootstrap_downsamples(Some(&downsample_names))
                .await
                .map_err(|e| format!("failed to bootstrap downsample collections: {}", e))?;
        }

        Ok(created_versions)
    }

    async fn refresh_views(&self, names: Option<Vec<String>>) -> Result<(), String> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::ViewRefresh)
            .await
            .map_err(|e| format!("{}", e))?;

        let options = names.map(db::RefreshViewsOptions::with_names);
        self.database
            .refresh_views(options)
            .await
            .map_err(|e| format!("failed to refresh views: {}", e))
    }

    async fn gc_downsample_histories(&self, names: Option<Vec<String>>) -> Result<(), String> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::ViewGc)
            .await
            .map_err(|e| format!("{}", e))?;

        let options = names.map(db::downsample::GcDownsampleHistoriesOptions::with_names);
        self.database
            .gc_downsample_histories(options)
            .await
            .map_err(|e| format!("failed to GC downsample histories: {}", e))
    }
}
