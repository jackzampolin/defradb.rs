//! HTTP error types.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use query::rest::RestError;
use serde::Serialize;
use thiserror::Error;

/// HTTP-layer errors.
#[derive(Debug, Clone, Error)]
pub enum HttpError {
    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    /// 401 Unauthorized - Used for NAC permission denials.
    /// Matches Go DefraDB's CollectionMiddleware which returns 401 for
    /// `ErrNotAuthorizedToPerformOperation`.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// 403 Forbidden - Used for invalid/expired tokens.
    /// Matches Go DefraDB's AuthMiddleware which returns 403 for token errors.
    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("query execution failed: {0}")]
    QueryExecution(String),
}

/// Error response body matching Go DefraDB format.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            HttpError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            HttpError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            HttpError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            HttpError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            HttpError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            HttpError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            HttpError::NotImplemented(msg) => (StatusCode::NOT_IMPLEMENTED, msg.clone()),
            HttpError::QueryExecution(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
        };

        (status, Json(ErrorResponse { error: message })).into_response()
    }
}

impl From<RestError> for HttpError {
    fn from(err: RestError) -> Self {
        match err {
            RestError::CollectionNotFound(name) => {
                HttpError::NotFound(format!("Collection '{}' not found", name))
            }
            // Go DefraDB returns 400 Bad Request for document not found
            // (combines with "not authorized" for ambiguity in permission errors)
            RestError::DocumentNotFound(id) => {
                HttpError::BadRequest(format!("document not found or not authorized: {}", id))
            }
            RestError::InvalidDocId(id) => {
                HttpError::BadRequest(format!("Invalid document ID: {}", id))
            }
            RestError::InvalidInput(msg) => HttpError::BadRequest(msg),
            // Use Unauthorized (401) for permission denied to match Go DefraDB behavior
            RestError::PermissionDenied(msg) => HttpError::Unauthorized(msg),
            RestError::Internal(msg) => HttpError::Internal(msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, HttpError>;
