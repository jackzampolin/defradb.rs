//! View cache serialization utilities for materialized views.
//!
//! Materialized views store query results in a cache. These utilities handle
//! serializing and deserializing view items (Docs) to/from the cache format.

use crate::planner::Doc;
use query_types::document::DocumentMapping;
use query_types::error::{QueryError, Result};
use serde_json::Value as JsonValue;

/// Serialize a Doc for view cache storage.
///
/// The format is a JSON array of field values in mapping order.
/// This matches Go's ViewCacheItem serialization format.
pub fn marshal_view_item(doc: &Doc, mapping: &DocumentMapping) -> Result<Vec<u8>> {
    let rendered = mapping.render_doc_to_json(doc);
    serde_json::to_vec(&rendered)
        .map_err(|e| QueryError::internal(format!("failed to serialize view cache item: {}", e)))
}

/// Deserialize cached bytes back to a Doc.
///
/// Expects a JSON object with field values keyed by field name.
/// Reconstructs a Doc with values at the correct mapping indexes.
pub fn unmarshal_view_item(bytes: &[u8], mapping: &DocumentMapping) -> Result<Doc> {
    let json: JsonValue = serde_json::from_slice(bytes).map_err(|e| {
        QueryError::internal(format!("failed to deserialize view cache item: {}", e))
    })?;

    let obj = json
        .as_object()
        .ok_or_else(|| QueryError::internal("view cache item is not a JSON object"))?;

    let mut doc = Doc::new(mapping.next_index());

    // Restore values at correct indexes using render key names
    for rk in &mapping.render_keys {
        if let Some(value) = obj.get(&rk.key) {
            doc.set(rk.index, value.clone());
        }
    }

    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_marshal_unmarshal_roundtrip() {
        // Create a mapping with some fields
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add(2, "age");
        mapping.add_render_key(0, "_docID");
        mapping.add_render_key(1, "name");
        mapping.add_render_key(2, "age");

        // Create a doc with values
        let mut doc = Doc::new(3);
        doc.set(0, json!("bae-123"));
        doc.set(1, json!("Alice"));
        doc.set(2, json!(30));

        // Marshal
        let bytes = marshal_view_item(&doc, &mapping).unwrap();

        // Unmarshal
        let restored = unmarshal_view_item(&bytes, &mapping).unwrap();

        // Verify
        assert_eq!(restored.get(0), Some(&json!("bae-123")));
        assert_eq!(restored.get(1), Some(&json!("Alice")));
        assert_eq!(restored.get(2), Some(&json!(30)));
    }

    #[test]
    fn test_unmarshal_handles_null_fields() {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping.add_render_key(0, "_docID");
        mapping.add_render_key(1, "name");

        let json_bytes = br#"{"_docID":"bae-456","name":null}"#;
        let doc = unmarshal_view_item(json_bytes, &mapping).unwrap();

        assert_eq!(doc.get(0), Some(&json!("bae-456")));
        assert_eq!(doc.get(1), Some(&json!(null)));
    }
}
