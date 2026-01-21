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

    /// Not the owner of the document
    #[error("not document owner: only owner can {operation}")]
    NotOwner { operation: String },

    /// Invalid policy configuration
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    /// Storage operation failed
    #[error("storage error: {0}")]
    Storage(String),

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
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}
