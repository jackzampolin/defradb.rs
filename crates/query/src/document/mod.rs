//! Document types and mapping for query results

mod convert;

use serde_json::{Map, Value as JsonValue};

use crate::planner::Doc;

pub use convert::{
    document_to_plan_doc, document_to_plan_doc_with_status, documents_to_plan_docs,
    documents_with_status_to_plan_docs, DELETED_FIELD_NAME,
};
pub use query_model::document::{DocumentMapping, RenderKey, DOC_ID_FIELD_INDEX};

/// Render a plan document to JSON using the provided document mapping.
pub(crate) fn render_doc_to_json(mapping: &DocumentMapping, doc: &Doc) -> JsonValue {
    let mut obj = Map::new();

    let typename_info = mapping
        .first_index_of_name("__typename")
        .and_then(|idx| mapping.type_name().map(|name| (idx, name.to_string())));
    let deleted_index = mapping.first_index_of_name("_deleted");

    for render_key in &mapping.render_keys {
        let value = if Some(render_key.index) == deleted_index && render_key.key == "_deleted" {
            JsonValue::Bool(doc.is_deleted())
        } else if let Some((typename_idx, ref typename)) = typename_info {
            if render_key.index == typename_idx {
                JsonValue::String(typename.clone())
            } else {
                doc.fields()
                    .get(render_key.index)
                    .cloned()
                    .flatten()
                    .unwrap_or(JsonValue::Null)
            }
        } else {
            doc.fields()
                .get(render_key.index)
                .cloned()
                .flatten()
                .unwrap_or(JsonValue::Null)
        };
        obj.insert(render_key.key.clone(), value);
    }

    JsonValue::Object(obj)
}
