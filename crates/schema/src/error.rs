//! Schema error types

use thiserror::Error;

/// Result type alias for schema operations
pub type Result<T> = std::result::Result<T, SchemaError>;

/// Schema-specific errors
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("duplicate field name: {0}")]
    DuplicateFieldName(String),

    #[error("invalid CRDT type for field kind: {field_name} cannot use {crdt_type} (only numeric fields support counters)")]
    InvalidCrdtForKind {
        field_name: String,
        crdt_type: String,
    },

    #[error("invalid relation: field {field_name} references unknown collection {collection_id}")]
    InvalidRelation {
        field_name: String,
        collection_id: String,
    },

    #[error("relation primary conflict: exactly one side of relation {relation_name} must be marked primary")]
    RelationPrimaryConflict { relation_name: String },

    #[error("duplicate collection name: {0}")]
    DuplicateCollectionName(String),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("invalid field ID: {0}")]
    InvalidFieldId(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("missing required field: {0}")]
    MissingRequiredField(String),

    #[error("CID generation failed: {0}")]
    CidGeneration(String),

    #[error("duplicate field ID: {0}")]
    DuplicateFieldId(String),
}

impl From<serde_json::Error> for SchemaError {
    fn from(err: serde_json::Error) -> Self {
        SchemaError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SchemaError::DuplicateFieldName("name".into());
        assert_eq!(err.to_string(), "duplicate field name: name");
    }

    #[test]
    fn test_invalid_crdt_error() {
        let err = SchemaError::InvalidCrdtForKind {
            field_name: "title".into(),
            crdt_type: "PnCounter".into(),
        };
        assert!(err.to_string().contains("title"));
        assert!(err.to_string().contains("PnCounter"));
    }
}
