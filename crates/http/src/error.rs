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

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("internal error: {0}")]
    Internal(String),

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
            HttpError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            HttpError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            HttpError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
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
            RestError::DocumentNotFound(id) => {
                HttpError::NotFound(format!("Document '{}' not found", id))
            }
            RestError::InvalidDocId(id) => {
                HttpError::BadRequest(format!("Invalid document ID: {}", id))
            }
            RestError::InvalidInput(msg) => HttpError::BadRequest(msg),
            RestError::PermissionDenied(msg) => HttpError::Forbidden(msg),
            RestError::Internal(msg) => HttpError::Internal(msg),
        }
    }
}

pub type Result<T> = std::result::Result<T, HttpError>;
