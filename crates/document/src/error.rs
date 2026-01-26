//! Document error types

use thiserror::Error;

/// Document result type
pub type Result<T> = std::result::Result<T, Error>;

/// Document errors
#[derive(Debug, Error)]
pub enum Error {
    #[error("malformed document ID")]
    MalformedDocID,

    #[error("invalid document ID version: {0}")]
    InvalidDocIDVersion(u16),

    #[error("field not found: {0}")]
    FieldNotFound(String),

    #[error("field name cannot be empty")]
    EmptyFieldName,

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("invalid field value for field '{field}': {message}")]
    InvalidFieldValue { field: String, message: String },

    #[error("document is missing required field: {0}")]
    MissingRequiredField(String),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("JSON number out of range: {0}")]
    JsonNumberOutOfRange(String),

    #[error("non-finite float value not supported in JSON: {0}")]
    NonFiniteFloat(String),

    #[error("CBOR encode error: {0}")]
    CborEncode(String),

    #[error("CBOR decode error: {0}")]
    CborDecode(String),

    #[error("incompatible CRDT type {crdt_type:?} for value type {value_type}")]
    IncompatibleCrdtType {
        crdt_type: schema::CType,
        value_type: String,
    },

    #[error("CID error: {0}")]
    Cid(#[from] cid::Error),

    #[error("multibase decode error: {0}")]
    MultibaseDecode(#[from] multibase::Error),

    #[error("UUID parse error: {0}")]
    UuidParse(#[from] uuid::Error),

    #[error("schema error: {0}")]
    Schema(#[from] schema::SchemaError),
}
