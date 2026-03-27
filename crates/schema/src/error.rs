//! Schema error types

use thiserror::Error;

/// Result type alias for schema operations
pub type Result<T> = std::result::Result<T, SchemaError>;

/// Schema-specific errors
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaError {
    #[error("duplicate field. Name: {0}")]
    DuplicateFieldName(String),

    #[error("CRDT type {crdt_type} can't be assigned to field kind {field_kind}")]
    InvalidCrdtForKind {
        crdt_type: String,
        field_kind: String,
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

    #[error("internal error: {0}")]
    InternalError(String),

    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    #[error("one-to-one relation must have a unique index. Object: {object}, Field: {field}")]
    OneToOneRequiresUniqueIndex { object: String, field: String },

    #[error("index with name already exists. Name: {0}")]
    DuplicateIndexName(String),

    #[error("invalid downsample configuration: {0}")]
    InvalidDownsample(String),
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
        assert_eq!(err.to_string(), "duplicate field. Name: name");
    }

    #[test]
    fn test_invalid_crdt_error() {
        let err = SchemaError::InvalidCrdtForKind {
            crdt_type: "pncounter".into(),
            field_kind: "String".into(),
        };
        assert_eq!(
            err.to_string(),
            "CRDT type pncounter can't be assigned to field kind String"
        );
    }
}
