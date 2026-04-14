//! CachedViewFetcher for reading from materialized view caches.
//!
//! This plan node reads pre-computed results from a materialized view's cache
//! instead of executing the view's query.

use async_trait::async_trait;
use std::sync::Arc;

use crate::fetcher::DocFetcher;
use crate::plan::view_cache::unmarshal_view_item;
use crate::planner::{Doc, ExecInfo, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::Result;

/// CachedViewFetcher reads documents from a materialized view's cache.
///
/// When a view is marked as materialized (is_materialized = true), query results
/// are cached. This node reads from that cache instead of executing the view query.
pub struct CachedViewFetcher {
    /// Collection root ID for constructing ViewCacheKey prefixes
    collection_id: u32,
    /// Document mapping for deserializing cached items
    document_mapping: DocumentMapping,
    /// Cached documents loaded during init
    docs: Vec<Doc>,
    /// Current position in docs
    position: usize,
    /// Whether init has been called
    initialized: bool,
    /// Current document value
    current_doc: Doc,
    /// Document fetcher for loading cache entries
    fetcher: Option<Arc<dyn DocFetcher>>,
    /// Execution statistics
    exec_info: ExecInfo,
}

impl CachedViewFetcher {
    /// Create a new CachedViewFetcher for the given collection.
    pub fn new(collection_id: u32, document_mapping: DocumentMapping) -> Self {
        Self {
            collection_id,
            document_mapping,
            docs: Vec::new(),
            position: 0,
            initialized: false,
            current_doc: Doc::default(),
            fetcher: None,
            exec_info: ExecInfo::default(),
        }
    }

    /// Set the fetcher for loading cached view items.
    pub fn with_fetcher(mut self, fetcher: Arc<dyn DocFetcher>) -> Self {
        self.fetcher = Some(fetcher);
        self
    }

    /// Get the collection ID
    pub fn collection_id(&self) -> u32 {
        self.collection_id
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for CachedViewFetcher {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.exec_info = ExecInfo::default();
        self.docs.clear();

        if let Some(ref fetcher) = self.fetcher {
            let items = fetcher.get_view_cache_items(self.collection_id).await?;

            for bytes in items {
                match unmarshal_view_item(&bytes, &self.document_mapping) {
                    Ok(doc) => {
                        self.docs.push(doc);
                    }
                    Err(e) => {
                        tracing::warn!("failed to unmarshal view cache item: {}", e);
                    }
                }
            }
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(query_types::error::QueryError::execution(
                "CachedViewFetcher.next() called before init()",
            ));
        }

        self.exec_info.iterations += 1;

        if self.position >= self.docs.len() {
            return Ok(false);
        }

        self.current_doc = self.docs[self.position].deep_clone();
        self.position += 1;
        self.exec_info.docs_fetched += 1;

        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.docs.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // CachedViewFetcher is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "scanNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert("filter".to_string(), serde_json::Value::Null);
        obj.insert(
            "collectionName".to_string(),
            serde_json::Value::String(String::new()),
        );
        obj.insert(
            "collectionID".to_string(),
            serde_json::Value::String(String::new()),
        );
        obj.insert(
            "prefixes".to_string(),
            serde_json::json!([format!("/{}", self.collection_id)]),
        );
        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );
        obj.insert(
            "docFetches".to_string(),
            serde_json::json!(self.exec_info.docs_fetched),
        );
        obj.insert(
            "fieldFetches".to_string(),
            serde_json::json!(self.exec_info.fields_fetched),
        );
        obj.insert(
            "indexFetches".to_string(),
            serde_json::json!(self.exec_info.indexes_fetched),
        );
        serde_json::Value::Object(obj)
    }
}
