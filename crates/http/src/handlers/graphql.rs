//! GraphQL and transaction HTTP handlers.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use query::executor::{QueryRequest, QueryResponse};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Health check response.
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "Healthy")
}

/// Version information response.
#[derive(Debug, Clone, Serialize)]
pub struct VersionResponse {
    pub version: String,
    pub commit: String,
}

/// Version endpoint handler.
pub async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: option_env!("GIT_COMMIT").unwrap_or("unknown").to_string(),
    })
}

/// GraphQL POST request handler.
///
/// Accepts JSON body: { query, operationName?, variables? }
/// Identity is extracted from the Authorization header and used for ACP checks.
pub async fn graphql(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(mut request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    // Wire identity from Authorization header into the request
    request.identity = identity.into_did();
    let response = state.executor.execute(request).await;
    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL POST query returned errors");
    }
    Json(response)
}

/// GraphQL GET request query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphqlQueryParams {
    pub query: String,
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,
    pub variables: Option<String>,
}

/// GraphQL GET request handler.
///
/// Accepts query parameters: ?query=...&operationName=...&variables=...
/// Identity is extracted from the Authorization header and used for ACP checks.
pub async fn graphql_get(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(params): Query<GraphqlQueryParams>,
) -> Json<QueryResponse> {
    let variables: Option<JsonValue> = match params.variables {
        Some(v) => match serde_json::from_str(&v) {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                tracing::warn!(error = %e, "Invalid JSON in variables query parameter");
                return Json(QueryResponse::error(format!(
                    "invalid JSON in 'variables' parameter: {}",
                    e
                )));
            }
        },
        None => None,
    };

    let request = QueryRequest {
        query: params.query,
        operation_name: params.operation_name,
        variables,
        identity: identity.into_did(),
    };

    let response = state.executor.execute(request).await;
    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL GET query returned errors");
    }
    Json(response)
}

/// Schema endpoint handler.
///
/// Returns the GraphQL schema as plain text.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    match state.executor.schema().await {
        Ok(sdl) => Ok((StatusCode::OK, sdl).into_response()),
        Err(e) => {
            tracing::error!(error = %e, "Schema retrieval failed");
            Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(crate::error::ErrorResponse {
                    error: "Failed to retrieve schema".to_string(),
                }),
            )
                .into_response())
        }
    }
}

// ============================================================================
// Transaction Endpoints
// ============================================================================

/// Request body for beginning a transaction.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TxBeginRequest {
    #[serde(default)]
    pub readonly: bool,
}

/// Response from beginning a transaction.
#[derive(Debug, Clone, Serialize)]
pub struct TxBeginResponse {
    pub txn_id: String,
}

/// Request body for commit/rollback operations.
#[derive(Debug, Clone, Deserialize)]
pub struct TxRequest {
    pub txn_id: String,
}

/// Response for successful transaction operations.
#[derive(Debug, Clone, Serialize)]
pub struct TxSuccessResponse {
    pub status: String,
}

/// Begin a new transaction.
pub async fn tx_begin(
    State(state): State<AppState>,
    Json(request): Json<TxBeginRequest>,
) -> impl IntoResponse {
    match state.executor.begin_txn(request.readonly).await {
        Ok(handle) => {
            tracing::info!(txn_id = %handle, readonly = request.readonly, "Transaction started");
            (
                StatusCode::OK,
                Json(TxBeginResponse {
                    txn_id: handle.to_string(),
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to begin transaction");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(crate::error::ErrorResponse {
                    error: format!("Failed to begin transaction: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// Commit a transaction.
pub async fn tx_commit(
    State(state): State<AppState>,
    Json(request): Json<TxRequest>,
) -> impl IntoResponse {
    let handle = match request.txn_id.parse() {
        Ok(h) => h,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Invalid transaction ID: {}", request.txn_id),
                }),
            )
                .into_response();
        }
    };

    match state.executor.commit_txn(&handle).await {
        Ok(()) => {
            tracing::info!(txn_id = %handle, "Transaction committed");
            (
                StatusCode::OK,
                Json(TxSuccessResponse {
                    status: "committed".to_string(),
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(txn_id = %handle, error = %e, "Failed to commit transaction");
            (
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Failed to commit transaction: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// Rollback a transaction.
pub async fn tx_rollback(
    State(state): State<AppState>,
    Json(request): Json<TxRequest>,
) -> impl IntoResponse {
    let handle = match request.txn_id.parse() {
        Ok(h) => h,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Invalid transaction ID: {}", request.txn_id),
                }),
            )
                .into_response();
        }
    };

    match state.executor.rollback_txn(&handle).await {
        Ok(()) => {
            tracing::info!(txn_id = %handle, "Transaction rolled back");
            (
                StatusCode::OK,
                Json(TxSuccessResponse {
                    status: "rolled_back".to_string(),
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(txn_id = %handle, error = %e, "Failed to rollback transaction");
            (
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Failed to rollback transaction: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// Extended GraphQL request with optional transaction ID.
#[derive(Debug, Clone, Deserialize)]
pub struct TransactionalQueryRequest {
    pub query: String,
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,
    pub variables: Option<JsonValue>,
    pub txn_id: Option<String>,
}

/// GraphQL POST request handler with optional transaction support.
/// Identity is extracted from the Authorization header and used for ACP checks.
pub async fn graphql_transactional(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<TransactionalQueryRequest>,
) -> Json<QueryResponse> {
    let query_request = QueryRequest {
        query: request.query,
        operation_name: request.operation_name,
        variables: request.variables,
        identity: identity.into_did(),
    };

    let response = match request.txn_id {
        Some(txn_id_str) => {
            let handle = match txn_id_str.parse() {
                Ok(h) => h,
                Err(_) => {
                    return Json(QueryResponse::error(format!(
                        "Invalid transaction ID: {}",
                        txn_id_str
                    )));
                }
            };
            state.executor.execute_in_txn(query_request, &handle).await
        }
        None => state.executor.execute(query_request).await,
    };

    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL query returned errors");
    }
    Json(response)
}
