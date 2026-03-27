//! Index-related types for collection schemas.
//!
//! Matches Go's client/index.go and client/encrypted_index.go

use serde::{Deserialize, Serialize};

/// Describes a field within an index.
/// Matches Go's IndexedFieldDescription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedFieldDescription {
    /// Name of the field being indexed.
    #[serde(rename = "Name", default)]
    pub name: String,

    /// Whether the field is indexed in descending order.
    #[serde(rename = "Descending", default)]
    pub descending: bool,
}

/// Describes a secondary index on a collection.
/// Matches Go's IndexDescription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDescription {
    /// Name of the index.
    #[serde(rename = "Name", default)]
    pub name: String,

    /// Local identifier for this index.
    #[serde(rename = "ID", default)]
    pub id: u32,

    /// Fields that are being indexed.
    #[serde(rename = "Fields", default)]
    pub fields: Vec<IndexedFieldDescription>,

    /// Whether the index enforces uniqueness.
    #[serde(rename = "Unique", default)]
    pub unique: bool,
}

impl IndexDescription {
    /// Create a new index description.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            id: 0,
            fields: Vec::new(),
            unique: false,
        }
    }

    /// Add a field to the index.
    pub fn with_field(mut self, name: impl Into<String>, descending: bool) -> Self {
        self.fields.push(IndexedFieldDescription {
            name: name.into(),
            descending,
        });
        self
    }

    /// Set the index as unique.
    pub fn as_unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Type of encrypted index.
/// Matches Go's EncryptedIndexType.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EncryptedIndexType {
    /// Equality-based searchable encryption.
    #[serde(rename = "equality")]
    #[default]
    Equality,
}

/// Describes an encrypted index for searchable encryption.
/// Matches Go's EncryptedIndexDescription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EncryptedIndexDescription {
    /// Name of the field being indexed.
    #[serde(rename = "FieldName")]
    pub field_name: String,

    /// Type of searchable encryption.
    #[serde(rename = "Type", default)]
    pub index_type: EncryptedIndexType,
}

impl EncryptedIndexDescription {
    /// Create a new encrypted index description.
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            index_type: EncryptedIndexType::Equality,
        }
    }
}

/// Describes a BM25 full-text search index on a collection field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullTextIndexDescription {
    /// Name of the field being indexed.
    #[serde(rename = "FieldName")]
    pub field_name: String,

    /// Language for tokenization and stemming (default: "english").
    #[serde(rename = "Language", default = "default_language")]
    pub language: String,

    /// BM25 term frequency saturation parameter (default: 1.2).
    #[serde(rename = "K1", default = "default_k1")]
    pub k1: f64,

    /// BM25 document length normalization parameter (default: 0.75).
    #[serde(rename = "B", default = "default_b")]
    pub b: f64,
}

fn default_language() -> String {
    "english".to_string()
}

fn default_k1() -> f64 {
    1.2
}

fn default_b() -> f64 {
    0.75
}

impl FullTextIndexDescription {
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            language: default_language(),
            k1: default_k1(),
            b: default_b(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_builder() {
        let index = IndexDescription::new("user_email_idx")
            .with_field("email", false)
            .as_unique();

        assert_eq!(index.name, "user_email_idx");
        assert!(index.unique);
        assert_eq!(index.fields.len(), 1);
        assert_eq!(index.fields[0].name, "email");
        assert!(!index.fields[0].descending);
    }

    #[test]
    fn test_index_serialization() {
        let index = IndexDescription::new("test_idx")
            .with_field("name", false)
            .with_field("created_at", true);

        let json = serde_json::to_string(&index).unwrap();
        assert!(json.contains("\"Name\""));
        assert!(json.contains("\"Fields\""));

        let parsed: IndexDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(index, parsed);
    }

    #[test]
    fn test_encrypted_index_serialization() {
        let enc_idx = EncryptedIndexDescription::new("ssn");
        let json = serde_json::to_string(&enc_idx).unwrap();

        assert!(json.contains("\"FieldName\""));
        assert!(json.contains("\"Type\""));
        assert!(json.contains("equality"));

        let parsed: EncryptedIndexDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(enc_idx, parsed);
    }
}
