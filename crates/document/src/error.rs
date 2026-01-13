// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("invalid field value for field '{field}': {message}")]
    InvalidFieldValue { field: String, message: String },

    #[error("document is missing required field: {0}")]
    MissingRequiredField(String),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("CBOR encode error: {0}")]
    CborEncode(String),

    #[error("CBOR decode error: {0}")]
    CborDecode(String),

    #[error("CID error: {0}")]
    Cid(#[from] cid::Error),

    #[error("multibase decode error: {0}")]
    MultibaseDecode(#[from] multibase::Error),

    #[error("UUID parse error: {0}")]
    UuidParse(#[from] uuid::Error),

    #[error("schema error: {0}")]
    Schema(#[from] schema::SchemaError),
}
