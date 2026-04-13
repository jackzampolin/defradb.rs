//! Document data types yielded by query plan nodes.

use serde_json::Value as JsonValue;

/// Document status (active or deleted)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DocStatus {
    #[default]
    Active,
    Deleted,
}

/// Document fields as a vector of optional JSON values
pub type DocFields = Vec<Option<JsonValue>>;

/// A document yielded by plan nodes
#[derive(Debug, Clone, Default)]
pub struct Doc {
    /// Whether this doc should be hidden from final output
    pub hidden: bool,
    /// Field values indexed by position
    fields: DocFields,
    /// Document status
    pub status: DocStatus,
    /// Schema version ID (for migrations)
    pub schema_version_id: Option<String>,
    /// Number of stored fields from the original document (for fieldFetches metrics)
    pub stored_field_count: usize,
}

impl Doc {
    /// Create a new document with the given number of fields
    pub fn new(num_fields: usize) -> Self {
        Self {
            hidden: false,
            fields: vec![None; num_fields],
            status: DocStatus::Active,
            schema_version_id: None,
            stored_field_count: 0,
        }
    }

    /// Create a new document from existing fields
    pub fn with_fields(fields: DocFields) -> Self {
        Self {
            hidden: false,
            fields,
            status: DocStatus::Active,
            schema_version_id: None,
            stored_field_count: 0,
        }
    }

    /// Get the document ID (field at index 0)
    pub fn doc_id(&self) -> Option<&str> {
        self.fields
            .first()
            .and_then(|f| f.as_ref())
            .and_then(|v| v.as_str())
    }

    /// Set the document ID (field at index 0)
    pub fn set_doc_id(&mut self, doc_id: impl Into<String>) {
        if self.fields.is_empty() {
            self.fields.push(None);
        }
        self.fields[0] = Some(JsonValue::String(doc_id.into()));
    }

    /// Get a field value by index
    pub fn get(&self, index: usize) -> Option<&JsonValue> {
        self.fields.get(index).and_then(|f| f.as_ref())
    }

    /// Set a field value by index
    pub fn set(&mut self, index: usize, value: JsonValue) {
        if index >= self.fields.len() {
            self.fields.resize(index + 1, None);
        }
        self.fields[index] = Some(value);
    }

    /// Get all fields as a slice
    pub fn fields(&self) -> &[Option<JsonValue>] {
        &self.fields
    }

    /// Get the number of fields
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Clone the document
    pub fn deep_clone(&self) -> Self {
        Self {
            hidden: self.hidden,
            fields: self.fields.clone(),
            status: self.status,
            schema_version_id: self.schema_version_id.clone(),
            stored_field_count: self.stored_field_count,
        }
    }

    /// Mark as deleted
    pub fn mark_deleted(&mut self) {
        self.status = DocStatus::Deleted;
    }

    /// Check if deleted
    pub fn is_deleted(&self) -> bool {
        self.status == DocStatus::Deleted
    }
}
