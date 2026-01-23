//! Lens document type.
//!
//! Matches Go's internal/lens/lens.go LensDoc type.

/// A document that will be sent to/from a Lens transform.
///
/// This is a JSON object mapping field names to values.
/// Matches Go's `LensDoc = map[string]any`.
pub type LensDoc = serde_json::Map<String, serde_json::Value>;

/// Reserved field name for document ID.
pub const DOC_ID_FIELD: &str = "_docID";

/// Reserved field name for deleted status.
pub const DELETED_FIELD: &str = "_deleted";

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_lens_doc_creation() {
        let mut doc = LensDoc::new();
        doc.insert("name".to_string(), json!("Alice"));
        doc.insert("age".to_string(), json!(30));
        doc.insert(DOC_ID_FIELD.to_string(), json!("bafkrei_doc1"));

        assert_eq!(doc.get("name").unwrap(), &json!("Alice"));
        assert_eq!(doc.get("age").unwrap(), &json!(30));
        assert_eq!(doc.get(DOC_ID_FIELD).unwrap(), &json!("bafkrei_doc1"));
    }

    #[test]
    fn test_lens_doc_serialization() {
        let mut doc = LensDoc::new();
        doc.insert("field1".to_string(), json!("value1"));
        doc.insert("nested".to_string(), json!({"inner": "value"}));

        let serialized = serde_json::to_string(&doc).unwrap();
        let deserialized: LensDoc = serde_json::from_str(&serialized).unwrap();

        assert_eq!(doc, deserialized);
    }
}
