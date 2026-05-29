//! LensNode applies a WASM lens transform to view query results.
//!
//! In the view pipeline: SourceQuery → LensNode (transform) → ViewNode (field mapping)
//! The LensNode collects all source documents, runs them through the registered
//! lens transform, and yields the transformed documents.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::planner::{index_selection::CursorSeek, Doc, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::Result;

use lens::{LensDoc, TransformId, TransformStore};

/// LensNode wraps a source plan and applies a lens transform to each document.
///
/// Documents from the source plan are converted to JSON objects, passed through
/// the WASM lens transform, and converted back to Doc format.
///
/// When the lens renames fields (e.g. `name` → `fullName`), the output JSON
/// has the target schema's field names. The `output_mapping` is used to convert
/// the transformed JSON back to Doc format, and should reflect the post-transform
/// field names (typically the view's target mapping).
pub struct LensNode {
    source: Box<dyn PlanNode>,
    source_mapping: DocumentMapping,
    output_mapping: DocumentMapping,
    lens_store: Arc<dyn TransformStore>,
    transform_id: TransformId,
    /// Transformed documents buffered after running the lens
    transformed_docs: Vec<Doc>,
    /// Current index into transformed_docs
    current_index: usize,
}

impl LensNode {
    pub fn new(
        source: Box<dyn PlanNode>,
        source_mapping: DocumentMapping,
        output_mapping: DocumentMapping,
        lens_store: Arc<dyn TransformStore>,
        transform_id: TransformId,
    ) -> Self {
        Self {
            source,
            source_mapping,
            output_mapping,
            lens_store,
            transform_id,
            transformed_docs: Vec::new(),
            current_index: 0,
        }
    }

    /// Convert a Doc to a LensDoc (JSON object) using the source mapping.
    fn doc_to_lens_doc(doc: &Doc, mapping: &DocumentMapping) -> LensDoc {
        let mut json_doc = serde_json::Map::new();
        for rk in &mapping.render_keys {
            if rk.index < doc.fields().len() {
                if let Some(ref value) = doc.fields()[rk.index] {
                    json_doc.insert(rk.key.clone(), value.clone());
                }
            }
        }
        json_doc
    }

    /// Convert a LensDoc back to a Doc using the source mapping.
    ///
    /// After transform, the lens may have added/removed/renamed fields.
    /// We create a Doc large enough to hold all fields and populate it
    /// by matching JSON keys to mapping render keys.
    fn lens_doc_to_doc(lens_doc: &LensDoc, mapping: &DocumentMapping) -> Doc {
        let mut doc = Doc::new(mapping.next_index());
        for rk in &mapping.render_keys {
            if let Some(value) = lens_doc.get(&rk.key) {
                doc.set(rk.index, value.clone());
            }
        }
        // Also set fields by underlying name for fields the transform added
        for (key, value) in lens_doc {
            if let Some(indexes) = mapping.indexes_of_name(key) {
                for &idx in indexes {
                    if doc.fields().get(idx).map(|f| f.is_none()).unwrap_or(true) {
                        doc.set(idx, value.clone());
                    }
                }
            }
        }
        doc
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for LensNode {
    async fn init(&mut self) -> Result<()> {
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;

        // Collect all source documents
        let mut source_docs = Vec::new();
        while self.source.next().await? {
            let doc = self.source.value();
            let lens_doc = Self::doc_to_lens_doc(doc, &self.source_mapping);
            source_docs.push(lens_doc);
        }

        // Split transform IDs for chained transforms (comma-separated)
        let transform_ids: Vec<TransformId> = self
            .transform_id
            .0
            .split(',')
            .map(|s| TransformId::new(s.trim()))
            .collect();

        // Apply each transform in sequence, feeding output into the next
        let mut current_docs = source_docs;
        for tid in &transform_ids {
            let doc_stream: std::pin::Pin<Box<dyn futures::Stream<Item = LensDoc> + Send>> =
                Box::pin(futures::stream::iter(current_docs));

            let result_stream = self.lens_store.transform(tid, doc_stream).map_err(|e| {
                query_types::error::QueryError::execution(format!("lens transform failed: {}", e))
            })?;

            let results: Vec<_> = result_stream.collect().await;
            current_docs = results.into_iter().filter_map(|r| r.ok()).collect();
        }

        // Convert final transformed documents to Doc format
        self.transformed_docs = current_docs
            .iter()
            .map(|lens_doc| Self::lens_doc_to_doc(lens_doc, &self.output_mapping))
            .collect();

        self.current_index = 0;
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if self.current_index < self.transformed_docs.len() {
            self.current_index += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn value(&self) -> &Doc {
        &self.transformed_docs[self.current_index - 1]
    }

    async fn close(&mut self) -> Result<()> {
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.output_mapping
    }

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn set_cursor_fetch_limit(&mut self, limit: u64) -> bool {
        self.source.set_cursor_fetch_limit(limit)
    }

    fn page_info(&self) -> Option<crate::plan::CursorPageInfo> {
        self.source.page_info()
    }

    fn kind(&self) -> &'static str {
        "lensNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let child_explain = self.source.explain();
        // Wrap the source plan in selectTopNode (Go's view pipeline convention).
        // LensNode is the innermost view-related node when present.
        serde_json::json!({ "selectTopNode": child_explain })
    }

    fn explain_debug_inner(&self) -> serde_json::Value {
        let child_explain = self.source.explain_debug();
        serde_json::json!({ "selectTopNode": child_explain })
    }
}
