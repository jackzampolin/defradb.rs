//! GraphQL and transaction HTTP handlers.
//!
//! # NAC Permission Model
//!
//! GraphQL endpoint permissions are checked based on the operation type:
//! - Query operations require `DocumentRead` permission
//! - Mutation operations require `DocumentUpdate` permission
//!
//! This matches Go DefraDB's per-operation permission model more closely,
//! where each operation type has its own permission requirement.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use query::executor::{QueryRequest, QueryResponse};
use query::{parse_request, ParsedOperation};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Determine the required NAC permission based on GraphQL operation type.
///
/// - Query operations require `DocumentRead` permission
/// - Mutation operations require `DocumentUpdate` permission
/// - Parse failures default to `DocumentUpdate` (fail-secure)
///
/// This matches Go DefraDB's per-operation permission model where different
/// operation types have different permission requirements.
fn permission_for_query(query: &str) -> NodePermission {
    match parse_request(query) {
        Ok(ParsedOperation::Query(_)) => NodePermission::DocumentRead,
        Ok(ParsedOperation::Mutation(_)) => NodePermission::DocumentUpdate,
        // Parse failures default to the more restrictive permission
        Err(_) => NodePermission::DocumentUpdate,
    }
}

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
///
/// # NAC Permission Model
///
/// Permission is determined by parsing the query:
/// - Query operations require `DocumentRead` permission
/// - Mutation operations require `DocumentUpdate` permission
///
/// This matches Go DefraDB's per-operation permission model where each
/// operation type has its own permission requirement.
pub async fn graphql(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(mut request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, HttpError> {
    // NAC check: Determine required permission based on operation type
    let required_permission = permission_for_query(&request.query);
    require_permission(&state, &identity, required_permission).await?;

    // Wire identity from Authorization header into the request
    request.identity = identity.into_did();
    let response = state.executor.execute(request).await;
    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL POST query returned errors");
    }
    Ok(Json(response))
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
///
/// Requires `DocumentRead` permission when NAC is enabled (GET is read-only).
pub async fn graphql_get(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(params): Query<GraphqlQueryParams>,
) -> Result<Json<QueryResponse>, HttpError> {
    // NAC check: GET is read-only queries, so require DocumentRead permission
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;

    let variables: Option<JsonValue> = match params.variables {
        Some(v) => match serde_json::from_str(&v) {
            Ok(parsed) => Some(parsed),
            Err(e) => {
                tracing::warn!(error = %e, "Invalid JSON in variables query parameter");
                return Ok(Json(QueryResponse::error(format!(
                    "invalid JSON in 'variables' parameter: {}",
                    e
                ))));
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
    Ok(Json(response))
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
//
// # NAC Transaction Security Model
//
// Transaction endpoints require `DocumentUpdate` permission when NAC is enabled.
// This is more restrictive than Go DefraDB, which doesn't have explicit handler-level
// NAC checks for transaction endpoints (permission checks happen during operations
// within the transaction).
//
// This approach was chosen for defense-in-depth:
// - Prevents unauthorized users from starting/managing transactions
// - Operations within transactions are still subject to per-operation permission checks
//
// # Go DefraDB Comparison
//
// Go DefraDB checks NAC permissions at the DB layer during individual operations.
// Transaction management (begin/commit/rollback) doesn't have explicit handler-level
// permission checks. This Rust implementation adds upfront checks for extra security.

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
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_begin(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<TxBeginRequest>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: transactions can modify data, require DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    Ok(match state.executor.begin_txn(request.readonly).await {
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
    })
}

/// Commit a transaction.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_commit(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<TxRequest>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: committing transactions requires DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    let handle = match request.txn_id.parse() {
        Ok(h) => h,
        Err(_) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Invalid transaction ID: {}", request.txn_id),
                }),
            )
                .into_response());
        }
    };

    Ok(match state.executor.commit_txn(&handle).await {
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
    })
}

/// Rollback a transaction.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_rollback(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<TxRequest>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: rolling back transactions requires DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    let handle = match request.txn_id.parse() {
        Ok(h) => h,
        Err(_) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Invalid transaction ID: {}", request.txn_id),
                }),
            )
                .into_response());
        }
    };

    Ok(match state.executor.rollback_txn(&handle).await {
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
    })
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
///
/// # NAC Permission Model
///
/// Permission is determined by parsing the query:
/// - Query operations require `DocumentRead` permission
/// - Mutation operations require `DocumentUpdate` permission
///
/// This matches Go DefraDB's per-operation permission model.
pub async fn graphql_transactional(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<TransactionalQueryRequest>,
) -> Result<Json<QueryResponse>, HttpError> {
    // NAC check: Determine required permission based on operation type
    let required_permission = permission_for_query(&request.query);
    require_permission(&state, &identity, required_permission).await?;

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
                    return Ok(Json(QueryResponse::error(format!(
                        "Invalid transaction ID: {}",
                        txn_id_str
                    ))));
                }
            };
            state.executor.execute_in_txn(query_request, &handle).await
        }
        None => state.executor.execute(query_request).await,
    };

    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL query returned errors");
    }
    Ok(Json(response))
}
