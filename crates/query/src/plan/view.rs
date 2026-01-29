//! ViewNode for querying non-materialized views
//!
//! A view executes its underlying query and remaps the source document fields
//! to the view's own document mapping.

use async_trait::async_trait;
use serde_json::Value as JsonValue;

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
                        // Filter nested JSON values to only include fields
                        // defined in the target child mapping
                        let filtered = filter_nested_json(value, dst_map, dst_index);
                        dst.set(dst_index, filtered);
                    }
                }
            }
        }
    }

    dst
}

/// Build a rename map from source field name → target render key name.
///
/// The child mapping stores both the underlying field name (in indexes_by_name)
/// and the render key name (which may be an alias). For example, a field with
/// `fullName: name` has underlying name "name" and render key "fullName".
fn build_field_rename_map(child_mapping: &DocumentMapping) -> std::collections::HashMap<String, String> {
    let mut rename_map = std::collections::HashMap::new();
    for rk in &child_mapping.render_keys {
        // Find the underlying field name for this render key's index
        for (name, indexes) in child_mapping.indexes_by_name_iter() {
            if indexes.contains(&rk.index) {
                // Map source field name → render key (output) name
                rename_map.insert(name.to_string(), rk.key.clone());
                break;
            }
        }
    }
    rename_map
}

/// Filter a JSON value to only include fields defined in the target child mapping.
///
/// When a view's source query fetches ALL fields for nested relations (e.g. books),
/// we need to strip fields not defined in the view schema and rename aliased fields.
fn filter_nested_json(
    value: &JsonValue,
    target_map: &DocumentMapping,
    target_index: usize,
) -> JsonValue {
    let child_mapping = match target_map.child_at(target_index) {
        Some(cm) => cm,
        None => return value.clone(),
    };

    let rename_map = build_field_rename_map(child_mapping);

    if rename_map.is_empty() {
        return value.clone();
    }

    match value {
        JsonValue::Array(arr) => {
            let filtered: Vec<JsonValue> = arr
                .iter()
                .map(|item| filter_json_object(item, &rename_map, child_mapping))
                .collect();
            JsonValue::Array(filtered)
        }
        JsonValue::Object(_) => filter_json_object(value, &rename_map, child_mapping),
        _ => value.clone(),
    }
}

/// Filter a JSON object: keep only mapped fields, rename aliased ones, recurse into nested.
fn filter_json_object(
    value: &JsonValue,
    rename_map: &std::collections::HashMap<String, String>,
    child_mapping: &DocumentMapping,
) -> JsonValue {
    match value {
        JsonValue::Object(obj) => {
            let mut filtered = serde_json::Map::new();
            for (key, val) in obj {
                if let Some(output_name) = rename_map.get(key.as_str()) {
                    // Check for deeper nested child mappings
                    if let Some(field_index) = child_mapping.try_find_index_from_render_key(output_name) {
                        let nested_val = filter_nested_json(val, child_mapping, field_index);
                        filtered.insert(output_name.clone(), nested_val);
                    } else {
                        filtered.insert(output_name.clone(), val.clone());
                    }
                }
            }
            JsonValue::Object(filtered)
        }
        _ => value.clone(),
    }
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
