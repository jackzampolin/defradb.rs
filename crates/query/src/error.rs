//! Query error types

use thiserror::Error;

/// Result type alias for query operations
pub type Result<T> = std::result::Result<T, QueryError>;

/// Query error types
#[derive(Debug, Error)]
pub enum QueryError {
    /// GraphQL parse error
    #[error("parse error: {0}")]
    Parse(String),

    /// Invalid filter condition
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    /// Unknown field referenced in query
    #[error("unknown field: {0}")]
    UnknownField(String),

    /// Collection not found
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    /// Invalid aggregate target
    #[error("invalid aggregate target: {0}")]
    InvalidAggregateTarget(String),

    /// Type mismatch in filter or assignment
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Storage layer error
    #[error("storage error: {0}")]
    Storage(#[from] storage::corekv::Error),

    /// Schema validation error
    #[error("schema error: {0}")]
    Schema(#[from] schema::SchemaError),

    /// Plan execution failed
    #[error("plan execution failed: {0}")]
    Execution(String),

    /// Document not found
    #[error("document not found: {0}")]
    DocumentNotFound(String),

    /// Invalid document ID format
    #[error("invalid document id: {0}")]
    InvalidDocId(String),

    /// Field is required but missing
    #[error("required field missing: {0}")]
    RequiredFieldMissing(String),

    /// Invalid mutation input
    #[error("invalid mutation input: {0}")]
    InvalidMutationInput(String),

    /// Internal error (should not happen)
    #[error("internal error: {0}")]
    Internal(String),
}

impl QueryError {
    /// Create a parse error
    pub fn parse(msg: impl Into<String>) -> Self {
        Self::Parse(msg.into())
    }

    /// Create an invalid filter error
    pub fn invalid_filter(msg: impl Into<String>) -> Self {
        Self::InvalidFilter(msg.into())
    }

    /// Create an unknown field error
    pub fn unknown_field(name: impl Into<String>) -> Self {
        Self::UnknownField(name.into())
    }

    /// Create a collection not found error
    pub fn collection_not_found(name: impl Into<String>) -> Self {
        Self::CollectionNotFound(name.into())
    }

    /// Create an execution error
    pub fn execution(msg: impl Into<String>) -> Self {
        Self::Execution(msg.into())
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = QueryError::parse("unexpected token");
        assert_eq!(err.to_string(), "parse error: unexpected token");

        let err = QueryError::unknown_field("foo");
        assert_eq!(err.to_string(), "unknown field: foo");

        let err = QueryError::TypeMismatch {
            expected: "String".into(),
            actual: "Int".into(),
        };
        assert_eq!(err.to_string(), "type mismatch: expected String, got Int");
    }

    #[test]
    fn test_error_constructors() {
        let _ = QueryError::invalid_filter("bad condition");
        let _ = QueryError::collection_not_found("users");
        let _ = QueryError::execution("plan failed");
        let _ = QueryError::internal("unexpected state");
    }
}
