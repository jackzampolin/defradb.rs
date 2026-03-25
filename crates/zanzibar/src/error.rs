use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid DID: {0}")]
    InvalidDid(String),

    #[error("policy not found: {0}")]
    PolicyNotFound(String),

    #[error("relation not found: {relation} in resource {resource}")]
    RelationNotFound { resource: String, relation: String },

    #[error("resource not found: {0}")]
    ResourceNotFound(String),

    #[error("invalid expression: {0}")]
    InvalidExpression(String),

    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    #[error("invalid EntitySet reference: resource '{resource}' relation '{relation}' does not exist in policy")]
    InvalidEntitySetReference { resource: String, relation: String },

    #[error("subject restriction violated: {message}")]
    SubjectRestrictionViolation { message: String },

    #[error("DPI violation: resource '{resource}' must have an 'owner' relation")]
    DpiMissingOwner { resource: String },

    #[error("DPI violation: permission '{relation}' on resource '{resource}' must include 'owner' in its expression")]
    DpiExpressionMissingOwner { resource: String, relation: String },

    #[error("DPI violation: resource '{resource}' relation '{relation}' uses disallowed operation '{operation}' (only union allowed)")]
    DpiDisallowedOperation {
        resource: String,
        relation: String,
        operation: String,
    },

    #[error("invalid relationship field '{field}': {reason}")]
    InvalidRelationshipField { field: String, reason: String },

    #[error("serialization error: {0}")]
    Serialization(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}
