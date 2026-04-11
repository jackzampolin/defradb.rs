//! Document types and operations

use crate::types::DocId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// A document in DefraDB - a collection of key-value pairs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    /// Document identifier
    #[serde(rename = "_docID")]
    pub id: Option<DocId>,

    /// Document fields as key-value pairs
    #[serde(flatten)]
    pub fields: HashMap<String, JsonValue>,
}

impl Document {
    /// Create a new empty document
    pub fn new() -> Self {
        Self {
            id: None,
            fields: HashMap::new(),
        }
    }

    /// Create a document with an ID
    pub fn with_id(id: DocId) -> Self {
        Self {
            id: Some(id),
            fields: HashMap::new(),
        }
    }

    /// Create a document from fields
    pub fn from_fields(fields: HashMap<String, JsonValue>) -> Self {
        Self { id: None, fields }
    }

    /// Set a field value
    pub fn set_field(&mut self, key: impl Into<String>, value: JsonValue) {
        self.fields.insert(key.into(), value);
    }

    /// Get a field value
    pub fn get_field(&self, key: &str) -> Option<&JsonValue> {
        self.fields.get(key)
    }

    /// Remove a field
    pub fn remove_field(&mut self, key: &str) -> Option<JsonValue> {
        self.fields.remove(key)
    }

    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.keys().map(|k| k.as_str()).collect()
    }
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

/// A partial document update - only specified fields are updated
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentUpdate {
    /// Document identifier to update
    pub id: DocId,

    /// Fields to update
    pub fields: HashMap<String, JsonValue>,
}

impl DocumentUpdate {
    /// Create a new document update
    pub fn new(id: DocId) -> Self {
        Self {
            id,
            fields: HashMap::new(),
        }
    }

    /// Add a field to update
    pub fn set_field(&mut self, key: impl Into<String>, value: JsonValue) {
        self.fields.insert(key.into(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_document_creation() {
        let mut doc = Document::new();
        doc.set_field("name", json!("Alice"));
        doc.set_field("age", json!(30));

        assert_eq!(doc.get_field("name"), Some(&json!("Alice")));
        assert_eq!(doc.get_field("age"), Some(&json!(30)));
    }

    #[test]
    fn test_document_serialization() {
        let mut doc = Document::new();
        doc.id = Some(DocId::new("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").unwrap());
        doc.set_field("name", json!("Bob"));

        let serialized = serde_json::to_string(&doc).unwrap();
        let deserialized: Document = serde_json::from_str(&serialized).unwrap();

        assert_eq!(doc, deserialized);
    }
}
