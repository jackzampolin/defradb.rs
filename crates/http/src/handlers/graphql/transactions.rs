//! Transaction endpoint handlers.
//!
//! # NAC Permission Model
//!
//! Transaction lifecycle endpoints (begin, commit, discard) have no NAC checks,
//! matching FFI behavior. Permissions are enforced per-operation when executing
//! queries within the transaction (via `graphql_transactional` handler).

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::router::AppState;

/// Query parameters for beginning a transaction (Go-compatible).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TxBeginQuery {
    /// Whether to create a read-only transaction.
    #[serde(default)]
    pub read_only: bool,
}

/// Response from beginning a transaction (Go-compatible).
/// Uses numeric `id` field to match Go DefraDB's `CreateTxResponse`.
#[derive(Debug, Clone, Serialize)]
pub struct TxBeginResponse {
    /// Transaction ID as numeric value (Go uses uint64).
    pub id: u64,
}

/// Path parameter for transaction operations.
#[derive(Debug, Clone, Deserialize)]
pub struct TxPathParam {
    pub id: String,
}

/// Begin a new transaction (Go-compatible).
///
/// POST /api/v0/tx?read_only=true
///
/// Go DefraDB uses query parameter `read_only` (not request body).
/// Returns `{"id": uint64}` to match Go's `CreateTxResponse`.
///
/// No NAC check — permissions are enforced per-operation within the transaction.
pub async fn tx_begin(
    State(state): State<AppState>,
    Query(query): Query<TxBeginQuery>,
) -> Result<impl IntoResponse, HttpError> {
    Ok(match state.executor.begin_txn(query.read_only).await {
        Ok(handle) => {
            // Parse handle to u64 to match Go's numeric ID format
            let id: u64 = handle.to_string().parse().unwrap_or(0);
            tracing::info!(
                txn_id = id,
                readonly = query.read_only,
                "Transaction started"
            );
            (StatusCode::OK, Json(TxBeginResponse { id })).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to begin transaction");
            HttpError::from(e).into_response()
        }
    })
}

/// Commit a transaction (Go-compatible).
///
/// POST /api/v0/tx/{id}
///
/// Go DefraDB uses path parameter for transaction ID and returns empty body on success.
///
/// No NAC check — permissions are enforced per-operation within the transaction.
pub async fn tx_commit(
    State(state): State<AppState>,
    Path(params): Path<TxPathParam>,
) -> Result<impl IntoResponse, HttpError> {
    // Parse transaction ID as u64 (Go format)
    let _txn_id: u64 = params
        .id
        .parse()
        .map_err(|_| HttpError::BadRequest("invalid transaction id".to_string()))?;

    let handle = params
        .id
        .parse()
        .map_err(|_| HttpError::BadRequest("invalid transaction id".to_string()))?;

    match state.executor.commit_txn(&handle).await {
        Ok(()) => {
            tracing::info!(txn_id = %handle, "Transaction committed");
            // Go returns 200 OK with empty body
            Ok(StatusCode::OK.into_response())
        }
        Err(e) => {
            tracing::error!(txn_id = %handle, error = %e, "Failed to commit transaction");
            Ok(HttpError::from(e).into_response())
        }
    }
}

/// Discard/rollback a transaction (Go-compatible).
///
/// DELETE /api/v0/tx/{id}
///
/// Go DefraDB uses DELETE method with path parameter for transaction ID.
/// Returns empty body on success.
///
/// No NAC check — permissions are enforced per-operation within the transaction.
pub async fn tx_discard(
    State(state): State<AppState>,
    Path(params): Path<TxPathParam>,
) -> Result<impl IntoResponse, HttpError> {
    // Parse transaction ID as u64 (Go format)
    let _txn_id: u64 = params
        .id
        .parse()
        .map_err(|_| HttpError::BadRequest("invalid transaction id".to_string()))?;

    let handle = params
        .id
        .parse()
        .map_err(|_| HttpError::BadRequest("invalid transaction id".to_string()))?;

    match state.executor.rollback_txn(&handle).await {
        Ok(()) => {
            tracing::info!(txn_id = %handle, "Transaction discarded");
            // Go returns 200 OK with empty body
            Ok(StatusCode::OK.into_response())
        }
        Err(e) => {
            tracing::error!(txn_id = %handle, error = %e, "Failed to discard transaction");
            Ok(HttpError::from(e).into_response())
        }
    }
}
