//! Error types for the ACP crate

use thiserror::Error;

/// Result type alias for ACP operations
pub type Result<T> = std::result::Result<T, Error>;

/// ACP-specific error types
#[derive(Debug, Error)]
pub enum Error {
    /// Permission denied for the requested operation
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Document is not registered with ACP
    #[error("document not registered: {0}")]
    DocumentNotRegistered(String),

    /// Document is already registered with ACP
    #[error("document already registered: {0}")]
    DocumentAlreadyRegistered(String),

    /// Invalid relation name
    #[error("invalid relation: {0}")]
    InvalidRelation(String),

    /// Not the owner of the document (UNAUTHORIZED)
    #[error("UNAUTHORIZED: not document owner, cannot {operation}")]
    NotOwner { operation: String },

    /// Actor is not a manager of the relation
    #[error("cannot {operation}: actor is not a manager of relation")]
    NotManager { operation: String },

    /// Invalid policy configuration
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    /// Storage operation failed (generic)
    #[error("storage error: {0}")]
    Storage(String),

    /// Storage read operation failed
    #[error("storage read error: {operation} - {details}")]
    StorageRead { operation: String, details: String },

    /// Storage write operation failed
    #[error("storage write error: {operation} - {details}")]
    StorageWrite { operation: String, details: String },

    /// Storage transaction failed
    #[error("storage transaction error: {operation} - {details}")]
    StorageTransaction { operation: String, details: String },

    /// Storage iteration failed
    #[error("storage iteration error: {operation} - {details}")]
    StorageIteration { operation: String, details: String },

    /// Serialization/deserialization error
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Policy not found
    #[error("policy not found: {0}")]
    PolicyNotFound(String),

    /// Relation not found in policy
    #[error("relation not found: {relation} in resource {resource}")]
    RelationNotFound { resource: String, relation: String },

    /// Cycle detected in permission evaluation
    #[error("cycle detected in permission evaluation: {0}")]
    CycleDetected(String),

    /// Invalid expression
    #[error("invalid expression: {0}")]
    InvalidExpression(String),

    /// Resource not found in policy
    #[error("resource not found: {0}")]
    ResourceNotFound(String),

    /// Invalid EntitySet subject reference
    #[error("invalid EntitySet reference: resource '{resource}' relation '{relation}' does not exist in policy")]
    InvalidEntitySetReference { resource: String, relation: String },

    /// Subject restriction violation
    #[error("subject restriction violated: {message}")]
    SubjectRestrictionViolation { message: String },

    /// DPI compliance violation: missing owner relation
    #[error("DPI violation: resource '{resource}' must have an 'owner' relation")]
    DpiMissingOwner { resource: String },

    /// DPI compliance violation: expression doesn't start with owner
    #[error("DPI violation: permission '{relation}' on resource '{resource}' must include 'owner' in its expression")]
    DpiExpressionMissingOwner { resource: String, relation: String },

    /// DPI compliance violation: disallowed operation
    #[error("DPI violation: resource '{resource}' relation '{relation}' uses disallowed operation '{operation}' (only union allowed)")]
    DpiDisallowedOperation {
        resource: String,
        relation: String,
        operation: String,
    },
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<zanzibar::error::Error> for Error {
    fn from(e: zanzibar::error::Error) -> Self {
        match e {
            zanzibar::error::Error::PolicyNotFound(s) => Error::PolicyNotFound(s),
            zanzibar::error::Error::RelationNotFound { resource, relation } => {
                Error::RelationNotFound { resource, relation }
            }
            zanzibar::error::Error::ResourceNotFound(s) => Error::ResourceNotFound(s),
            zanzibar::error::Error::InvalidExpression(s) => Error::InvalidExpression(s),
            zanzibar::error::Error::InvalidPolicy(s) => Error::InvalidPolicy(s),
            zanzibar::error::Error::Serialization(s) => Error::Serialization(s),
            zanzibar::error::Error::InvalidDid(s) => Error::Storage(format!("invalid DID: {}", s)),
            zanzibar::error::Error::InvalidEntitySetReference { resource, relation } => {
                Error::InvalidEntitySetReference { resource, relation }
            }
            zanzibar::error::Error::SubjectRestrictionViolation { message } => {
                Error::SubjectRestrictionViolation { message }
            }
            zanzibar::error::Error::DpiMissingOwner { resource } => {
                Error::DpiMissingOwner { resource }
            }
            zanzibar::error::Error::DpiExpressionMissingOwner { resource, relation } => {
                Error::DpiExpressionMissingOwner { resource, relation }
            }
            zanzibar::error::Error::DpiDisallowedOperation {
                resource,
                relation,
                operation,
            } => Error::DpiDisallowedOperation {
                resource,
                relation,
                operation,
            },
            zanzibar::error::Error::InvalidRelationshipField { field, reason } => Error::Storage(
                format!("invalid relationship field '{}': {}", field, reason),
            ),
            _ => Error::Storage(e.to_string()),
        }
    }
}

impl Error {
    /// Create a storage read error with context.
    pub fn storage_read(operation: impl Into<String>, err: impl std::fmt::Display) -> Self {
        Self::StorageRead {
            operation: operation.into(),
            details: err.to_string(),
        }
    }

    /// Create a storage write error with context.
    pub fn storage_write(operation: impl Into<String>, err: impl std::fmt::Display) -> Self {
        Self::StorageWrite {
            operation: operation.into(),
            details: err.to_string(),
        }
    }

    /// Create a storage transaction error with context.
    pub fn storage_txn(operation: impl Into<String>, err: impl std::fmt::Display) -> Self {
        Self::StorageTransaction {
            operation: operation.into(),
            details: err.to_string(),
        }
    }

    /// Create a storage iteration error with context.
    pub fn storage_iter(operation: impl Into<String>, err: impl std::fmt::Display) -> Self {
        Self::StorageIteration {
            operation: operation.into(),
            details: err.to_string(),
        }
    }
}
