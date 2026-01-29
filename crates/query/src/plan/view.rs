//! ViewNode for querying non-materialized views
//!
//! A view executes its underlying query and remaps the source document fields
//! to the view's own document mapping.

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// ViewNode wraps a source plan node and converts documents between mappings.
///
/// Non-materialized views don't store data - they execute the underlying query
/// on-demand and remap the result fields to the view's schema.
pub struct ViewNode {
    source: Box<dyn PlanNode>,
    source_mapping: DocumentMapping,
    target_mapping: DocumentMapping,
    current_doc: Doc,
}

impl ViewNode {
    pub fn new(
        source: Box<dyn PlanNode>,
        source_mapping: DocumentMapping,
        target_mapping: DocumentMapping,
    ) -> Self {
        Self {
            source,
            source_mapping,
            target_mapping,
            current_doc: Doc::default(),
        }
    }
}

/// Convert a document from one mapping to another by matching field names.
///
/// This mirrors Go's `convertBetweenMaps` in internal/planner/view.go.
fn convert_between_maps(src_map: &DocumentMapping, dst_map: &DocumentMapping, src: &Doc) -> Doc {
    let mut dst = Doc::new(dst_map.next_index());

    // Build a lookup from source index to render key name
    let mut src_render_keys_by_index = std::collections::HashMap::new();
    for rk in &src_map.render_keys {
        src_render_keys_by_index.insert(rk.index, rk.key.as_str());
    }

    for (underlying_name, src_indexes) in src_map.indexes_by_name_iter() {
        for &src_index in src_indexes {
            if src_index >= src.fields().len() {
                continue;
            }

            // Determine the destination field name:
            // use render key if available, otherwise the underlying name
            let dst_name = src_render_keys_by_index
                .get(&src_index)
                .copied()
                .unwrap_or(underlying_name);

            if let Some(dst_indexes) = dst_map.indexes_of_name(dst_name) {
                for &dst_index in dst_indexes {
                    if let Some(value) = &src.fields()[src_index] {
                        dst.set(dst_index, value.clone());
                    }
                }
            }
        }
    }

    dst
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for ViewNode {
    async fn init(&mut self) -> Result<()> {
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        let has_next = self.source.next().await?;
        if has_next {
            self.current_doc = convert_between_maps(
                &self.source_mapping,
                &self.target_mapping,
                self.source.value(),
            );
        }
        Ok(has_next)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.target_mapping
    }

    fn kind(&self) -> &'static str {
        "viewNode"
    }
}
