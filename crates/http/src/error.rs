//! HTTP error types.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use db::Error as DbError;
use query::error::{QueryError, TransactionError};
use query::rest::RestError;
use serde::Serialize;
use thiserror::Error;

/// HTTP-layer errors.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum HttpError {
    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("unprocessable entity: {0}")]
    UnprocessableEntity(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("not acceptable: {0}")]
    NotAcceptable(String),

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
            HttpError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            HttpError::UnprocessableEntity(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            HttpError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            HttpError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            HttpError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            HttpError::NotAcceptable(msg) => (StatusCode::NOT_ACCEPTABLE, msg.clone()),
            HttpError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            HttpError::NotImplemented(msg) => (StatusCode::NOT_IMPLEMENTED, msg.clone()),
            HttpError::QueryExecution(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
        };

        (status, Json(ErrorResponse { error: message })).into_response()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorStatus {
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    UnprocessableEntity,
    ServiceUnavailable,
}

fn http_error_from_status(status: ErrorStatus, message: String) -> HttpError {
    match status {
        ErrorStatus::Unauthorized => HttpError::Unauthorized(message),
        ErrorStatus::Forbidden => HttpError::Forbidden(message),
        ErrorStatus::NotFound => HttpError::NotFound(message),
        ErrorStatus::Conflict => HttpError::Conflict(message),
        ErrorStatus::UnprocessableEntity => HttpError::UnprocessableEntity(message),
        ErrorStatus::ServiceUnavailable => HttpError::ServiceUnavailable(message),
    }
}

fn message_contains_any(message: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| message.contains(needle))
}

fn classify_backend_message(message: &str) -> Option<ErrorStatus> {
    let message = message.to_ascii_lowercase();

    // Fallback precedence is intentional: typed errors should be preferred,
    // and ambiguous auth/not-found messages stay privacy-preserving 404s.
    if message_contains_any(
        &message,
        &["p2p disabled", "p2p is disabled", "database is closed"],
    ) {
        return Some(ErrorStatus::ServiceUnavailable);
    }

    if message_contains_any(
        &message,
        &[
            "not found",
            "does not exist",
            "does not exists",
            "not registered",
        ],
    ) {
        return Some(ErrorStatus::NotFound);
    }

    if message_contains_any(
        &message,
        &[
            "already exists",
            "already registered",
            "multiple active collection versions",
            "transaction conflict",
            "unique constraint",
            "violates unique index",
        ],
    ) {
        return Some(ErrorStatus::Conflict);
    }

    if message_contains_any(
        &message,
        &[
            "developer mode",
            "nac is disabled",
            "missing permission",
            "missing required permission",
            "permission denied",
        ],
    ) {
        return Some(ErrorStatus::Forbidden);
    }

    if message_contains_any(
        &message,
        &[
            "not authorized to perform operation",
            "unauthorized",
            "not authorized",
        ],
    ) {
        return Some(ErrorStatus::Unauthorized);
    }

    if message_contains_any(
        &message,
        &[
            "acp not available",
            "already disabled",
            "already enabled",
            "can not have policy without acp",
            "cannot delete",
            "can not delete",
            "has a policy",
            "invalid collection name",
            "invalid document",
            "invalid entityset",
            "invalid lens configuration",
            "invalid patch",
            "invalid policy",
            "invalid relation",
            "materialized view and acp",
            "migration between non-adjacent",
            "not materialized",
            "subject restriction",
            "unsafe policy transition",
        ],
    ) {
        return Some(ErrorStatus::UnprocessableEntity);
    }

    None
}

fn http_error_from_query_error(err: &QueryError) -> HttpError {
    let message = err.to_string();

    match err {
        QueryError::CollectionNotFound(_) | QueryError::DocumentNotFound(_) => {
            HttpError::NotFound(message)
        }
        QueryError::Storage(source) if source.is_not_found() => HttpError::NotFound(message),
        QueryError::Storage(source)
            if source.is_txn_conflict() || source.is_unique_constraint_violation() =>
        {
            HttpError::Conflict(message)
        }
        QueryError::TransactionConflict(_) => HttpError::Conflict(message),
        QueryError::PermissionDenied(_)
        | QueryError::AcpRegistrationFailed { .. }
        | QueryError::AcpCheckFailed { .. }
        | QueryError::AcpRegistrationCheckFailed { .. } => HttpError::Unauthorized(message),
        QueryError::Execution(message) => http_error_from_backend_message(message.clone()),
        _ => HttpError::BadRequest(message),
    }
}

fn http_status_from_db_error(err: &DbError) -> HttpError {
    let message = err.to_string();

    match err {
        DbError::CollectionNotFound(_)
        | DbError::CollectionVersionNotFound(_)
        | DbError::DocumentNotFound(_)
        | DbError::TransactionNotFound(_) => HttpError::NotFound(message),
        DbError::Storage(source) if source.is_not_found() => HttpError::NotFound(message),
        DbError::CollectionAlreadyExists(_) => HttpError::Conflict(message),
        DbError::Storage(source)
            if source.is_txn_conflict() || source.is_unique_constraint_violation() =>
        {
            HttpError::Conflict(message)
        }
        DbError::InvalidPatch(_)
        | DbError::InvalidDocument(_)
        | DbError::InvalidCollectionName(_)
        | DbError::CollectionVersionIDEmpty
        | DbError::ExplicitTxnMustUseForce
        | DbError::UnsafePolicyTransition(_)
        | DbError::JsonPatch(_) => HttpError::UnprocessableEntity(message),
        DbError::DatabaseClosed => HttpError::ServiceUnavailable(message),
        DbError::Query(source) => http_error_from_query_error(source),
        DbError::Other(message) | DbError::Acp(message) | DbError::Lens(message) => {
            http_error_from_backend_message(message.clone())
        }
        _ => HttpError::BadRequest(message),
    }
}

pub(crate) fn http_error_from_backend_message(message: String) -> HttpError {
    match classify_backend_message(&message) {
        Some(status) => http_error_from_status(status, message),
        None => HttpError::BadRequest(message),
    }
}

impl From<DbError> for HttpError {
    fn from(err: DbError) -> Self {
        http_status_from_db_error(&err)
    }
}

impl From<TransactionError> for HttpError {
    fn from(err: TransactionError) -> Self {
        let message = err.to_string();

        match err {
            TransactionError::NotFound(_) => HttpError::NotFound(message),
            TransactionError::AlreadyFinalized(_) => HttpError::Conflict(message),
            TransactionError::Execution(message) => http_error_from_backend_message(message),
            TransactionError::LockPoisoned(_) => HttpError::Internal(message),
            TransactionError::NotSupported(_) => HttpError::BadRequest(message),
        }
    }
}

impl From<RestError> for HttpError {
    fn from(err: RestError) -> Self {
        match err {
            RestError::CollectionNotFound(name) => {
                HttpError::NotFound(format!("Collection '{}' not found", name))
            }
            RestError::DocumentNotFound(id) => {
                HttpError::NotFound(format!("document not found or not authorized: {}", id))
            }
            RestError::InvalidDocId(id) => {
                HttpError::BadRequest(format!("Invalid document ID: {}", id))
            }
            RestError::InvalidInput(msg) => HttpError::BadRequest(msg),
            RestError::PermissionDenied(msg) => HttpError::Unauthorized(msg),
            RestError::Internal(msg) => match classify_backend_message(&msg) {
                Some(status) => http_error_from_status(status, msg),
                None => HttpError::Internal(msg),
            },
        }
    }
}

pub type Result<T> = std::result::Result<T, HttpError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn status(error: HttpError) -> StatusCode {
        error.into_response().status()
    }

    #[test]
    fn http_error_variants_map_to_status_codes() {
        let cases = [
            (HttpError::BadRequest("bad".into()), StatusCode::BAD_REQUEST),
            (
                HttpError::Unauthorized("auth".into()),
                StatusCode::UNAUTHORIZED,
            ),
            (HttpError::Forbidden("forbid".into()), StatusCode::FORBIDDEN),
            (HttpError::NotFound("missing".into()), StatusCode::NOT_FOUND),
            (HttpError::Conflict("conflict".into()), StatusCode::CONFLICT),
            (
                HttpError::UnprocessableEntity("semantic".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                HttpError::ServiceUnavailable("down".into()),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(status(error), expected);
        }
    }

    #[test]
    fn db_errors_map_to_status_buckets() {
        let cases = [
            (
                DbError::Acp("not authorized to perform operation".into()),
                StatusCode::UNAUTHORIZED,
            ),
            (
                DbError::Acp("resource is missing required permission".into()),
                StatusCode::FORBIDDEN,
            ),
            (
                DbError::CollectionNotFound("Users".into()),
                StatusCode::NOT_FOUND,
            ),
            (
                DbError::CollectionAlreadyExists("Users".into()),
                StatusCode::CONFLICT,
            ),
            (
                DbError::Storage(storage::Error::TxnConflict),
                StatusCode::CONFLICT,
            ),
            (
                DbError::UnsafePolicyTransition("collection policy changed".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (DbError::DatabaseClosed, StatusCode::SERVICE_UNAVAILABLE),
            (DbError::Other("unknown".into()), StatusCode::BAD_REQUEST),
        ];

        for (error, expected) in cases {
            assert_eq!(status(HttpError::from(error)), expected);
        }
    }

    #[test]
    fn rest_document_not_found_maps_to_404() {
        assert_eq!(
            status(HttpError::from(RestError::DocumentNotFound(
                "bae-123".into()
            ))),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn backend_message_precedence_keeps_ambiguous_auth_not_found_as_404() {
        assert_eq!(
            status(http_error_from_backend_message(
                "document not found or not authorized".into()
            )),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn transaction_errors_map_to_status_buckets() {
        let cases = [
            (TransactionError::not_found("1"), StatusCode::NOT_FOUND),
            (
                TransactionError::already_finalized("1"),
                StatusCode::CONFLICT,
            ),
            (
                TransactionError::execution("transaction conflict. Please retry"),
                StatusCode::CONFLICT,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(status(HttpError::from(error)), expected);
        }
    }
}
