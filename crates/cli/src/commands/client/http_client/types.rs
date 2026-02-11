//! Shared request/response types for HTTP client

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// GraphQL request body
#[derive(Debug, Serialize)]
pub struct GraphQLRequest {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<JsonValue>,
    #[serde(rename = "operationName", skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_id: Option<String>,
}

/// GraphQL response from server
#[derive(Debug, Deserialize)]
pub struct GraphQLResponse {
    pub data: Option<JsonValue>,
    #[serde(default)]
    pub errors: Vec<GraphQLError>,
}

/// GraphQL error
#[derive(Debug, Deserialize)]
pub struct GraphQLError {
    pub message: String,
}

/// Transaction begin request
#[derive(Debug, Serialize)]
pub struct TxBeginRequest {
    pub readonly: bool,
}

/// Transaction begin response
#[derive(Debug, Deserialize)]
pub struct TxBeginResponse {
    pub txn_id: String,
}

/// Transaction commit/discard request
#[derive(Debug, Serialize)]
pub struct TxRequest {
    pub txn_id: String,
}

/// Transaction success response
#[derive(Debug, Deserialize)]
pub struct TxSuccessResponse {
    pub status: String,
}

/// Server error response
#[derive(Debug, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl GraphQLResponse {
    /// Check if the response has errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Format errors as a string
    pub fn error_message(&self) -> String {
        self.errors
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }
}
