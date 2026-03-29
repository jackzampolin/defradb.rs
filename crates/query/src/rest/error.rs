//! Error types for REST operations.

use crate::error::QueryError;

/// Result type for REST operations.
pub type RestResult<T> = std::result::Result<T, RestError>;

/// Error type for REST operations.
#[derive(Debug, Clone)]
pub enum RestError {
    /// Collection not found.
    CollectionNotFound(String),
    /// Document not found.
    DocumentNotFound(String),
    /// Invalid document ID format.
    InvalidDocId(String),
    /// Invalid input data.
    InvalidInput(String),
    /// Permission denied (ACP check failed).
    PermissionDenied(String),
    /// Storage or execution error.
    Internal(String),
}

impl std::fmt::Display for RestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionNotFound(name) => write!(f, "collection not found: {}", name),
            Self::DocumentNotFound(id) => write!(f, "document not found: {}", id),
            Self::InvalidDocId(id) => write!(f, "invalid document ID: {}", id),
            Self::InvalidInput(msg) => write!(f, "invalid input: {}", msg),
            Self::PermissionDenied(msg) => write!(f, "permission denied: {}", msg),
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for RestError {}

impl RestError {
    pub fn collection_not_found(name: impl Into<String>) -> Self {
        Self::CollectionNotFound(name.into())
    }

    pub fn document_not_found(id: impl Into<String>) -> Self {
        Self::DocumentNotFound(id.into())
    }

    pub fn invalid_doc_id(id: impl Into<String>) -> Self {
        Self::InvalidDocId(id.into())
    }

    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self::PermissionDenied(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}

impl From<QueryError> for RestError {
    fn from(err: QueryError) -> Self {
        match err {
            // Not found errors
            QueryError::CollectionNotFound(name) => Self::CollectionNotFound(name),
            QueryError::DocumentNotFound(id) => Self::DocumentNotFound(id),
            // Invalid input errors (user-fixable, should be 400 Bad Request)
            QueryError::InvalidDocId(id) => Self::InvalidDocId(id),
            QueryError::InvalidMutationInput(msg) => Self::InvalidInput(msg),
            QueryError::Parse(msg) => Self::InvalidInput(format!("parse error: {}", msg)),
            QueryError::InvalidFilter(msg) => {
                Self::InvalidInput(format!("invalid filter: {}", msg))
            }
            QueryError::FilterFieldNotSelected { field, collection } => {
                Self::InvalidInput(format!(
                    "filter field '{}' must be in select list for '{}'",
                    field, collection
                ))
            }
            QueryError::UnknownField(name) => Self::InvalidInput(name.to_string()),
            QueryError::TypeMismatch { expected, actual } => Self::InvalidInput(format!(
                "type mismatch: expected {}, got {}",
                expected, actual
            )),
            QueryError::RequiredFieldMissing(field) => {
                Self::InvalidInput(format!("required field missing: {}", field))
            }
            QueryError::InvalidAggregateTarget(msg) => {
                Self::InvalidInput(format!("invalid aggregate target: {}", msg))
            }
            // Permission errors (should be 403 Forbidden)
            QueryError::PermissionDenied(msg) => Self::PermissionDenied(msg),
            QueryError::AcpRegistrationFailed { doc_id, message } => Self::PermissionDenied(
                format!("ACP registration failed for '{}': {}", doc_id, message),
            ),
            // True internal errors (500 Internal Server Error)
            other => Self::Internal(other.to_string()),
        }
    }
}
