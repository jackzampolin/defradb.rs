//! HTTP request handlers.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use query::executor::{QueryRequest, QueryResponse};

use crate::router::AppState;

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
pub async fn graphql(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
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
pub async fn graphql_get(
    State(state): State<AppState>,
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
pub async fn schema(State(state): State<AppState>) -> impl IntoResponse {
    match state.executor.schema().await {
        Ok(sdl) => (StatusCode::OK, sdl).into_response(),
        Err(e) => {
            // Log full error details server-side
            tracing::error!(error = %e, "Schema retrieval failed");

            // Provide categorized user-facing message based on error type
            let user_message = categorize_schema_error(&e);

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(crate::error::ErrorResponse {
                    error: user_message,
                }),
            )
                .into_response()
        }
    }
}

/// Categorize schema errors for user-facing messages.
fn categorize_schema_error(e: &query::error::QueryError) -> String {
    use query::error::QueryError;
    match e {
        QueryError::Storage(_) => {
            "Schema unavailable due to storage error. Please try again.".to_string()
        }
        QueryError::Schema(_) => "Schema validation error. Check schema definition.".to_string(),
        QueryError::Parse(_) => "Schema parse error. Check schema syntax.".to_string(),
        _ => "Failed to retrieve schema. Check server logs for details.".to_string(),
    }
}

// ============================================================================
// Transaction Endpoints
// ============================================================================

/// Request body for beginning a transaction.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TxBeginRequest {
    /// If true, the transaction is read-only.
    #[serde(default)]
    pub readonly: bool,
}

/// Response from beginning a transaction.
#[derive(Debug, Clone, Serialize)]
pub struct TxBeginResponse {
    /// The transaction ID.
    pub txn_id: String,
}

/// Request body for commit/rollback operations.
#[derive(Debug, Clone, Deserialize)]
pub struct TxRequest {
    /// The transaction ID.
    pub txn_id: String,
}

/// Response for successful transaction operations.
#[derive(Debug, Clone, Serialize)]
pub struct TxSuccessResponse {
    /// Status message.
    pub status: String,
}

/// Begin a new transaction.
///
/// POST /api/v0/tx/begin
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
///
/// POST /api/v0/tx/commit
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
///
/// POST /api/v0/tx/rollback
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
    /// The GraphQL query string.
    pub query: String,

    /// Optional operation name.
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,

    /// Optional variables.
    pub variables: Option<JsonValue>,

    /// Optional transaction ID for executing within a transaction.
    pub txn_id: Option<String>,
}

/// GraphQL POST request handler with optional transaction support.
///
/// If txn_id is provided, executes within the specified transaction.
/// Otherwise, executes with auto-commit semantics.
pub async fn graphql_transactional(
    State(state): State<AppState>,
    Json(request): Json<TransactionalQueryRequest>,
) -> Json<QueryResponse> {
    let query_request = QueryRequest {
        query: request.query,
        operation_name: request.operation_name,
        variables: request.variables,
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use serde_json::json;
    use std::sync::Arc;

    use crate::mock::{FailingMockExecutor, MockQueryExecutor};

    #[tokio::test]
    async fn test_health_check() {
        let response = health_check().await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_version() {
        let response = version().await;
        assert!(!response.version.is_empty());
    }

    #[tokio::test]
    async fn test_graphql_post() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = QueryRequest::new("{ users { name } }");

        let response = graphql(State(state), Json(request)).await;
        assert!(response.data.is_some());
        assert!(!response.has_errors());
    }

    #[tokio::test]
    async fn test_graphql_get_basic() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let params = GraphqlQueryParams {
            query: "{ users { name } }".to_string(),
            operation_name: None,
            variables: None,
        };

        let response = graphql_get(State(state), Query(params)).await;
        assert!(response.data.is_some());
        assert!(!response.has_errors());
    }

    #[tokio::test]
    async fn test_graphql_get_with_variables() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let params = GraphqlQueryParams {
            query: "{ users { name } }".to_string(),
            operation_name: Some("GetUsers".to_string()),
            variables: Some(json!({"limit": 10}).to_string()),
        };

        let response = graphql_get(State(state), Query(params)).await;
        assert!(response.data.is_some());
        assert!(!response.has_errors());
    }

    #[tokio::test]
    async fn test_graphql_get_invalid_variables_json() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let params = GraphqlQueryParams {
            query: "{ users { name } }".to_string(),
            operation_name: None,
            variables: Some("{invalid json".to_string()),
        };

        let response = graphql_get(State(state), Query(params)).await;
        assert!(response.has_errors());
        assert!(response.data.is_none());
        assert!(response.errors[0].message.contains("invalid JSON"));
    }

    #[tokio::test]
    async fn test_schema_success() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };

        let response = schema(State(state)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_error() {
        let state = AppState {
            executor: Arc::new(FailingMockExecutor::with_schema_error("schema unavailable")),
        };

        let response = schema(State(state)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ==========================================================================
    // Transaction endpoint tests
    // ==========================================================================

    #[tokio::test]
    async fn test_tx_begin() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TxBeginRequest { readonly: false };

        let response = tx_begin(State(state), Json(request)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tx_begin_readonly() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TxBeginRequest { readonly: true };

        let response = tx_begin(State(state), Json(request)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tx_begin_failure() {
        let state = AppState {
            executor: Arc::new(FailingMockExecutor::with_schema_error("unused")),
        };
        let request = TxBeginRequest { readonly: false };

        let response = tx_begin(State(state), Json(request)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_tx_commit() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TxRequest {
            txn_id: "mock-txn-001".to_string(),
        };

        let response = tx_commit(State(state), Json(request)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tx_rollback() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TxRequest {
            txn_id: "mock-txn-001".to_string(),
        };

        let response = tx_rollback(State(state), Json(request)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tx_commit_invalid_id() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        // Empty string is invalid
        let request = TxRequest {
            txn_id: "".to_string(),
        };

        let response = tx_commit(State(state), Json(request)).await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_graphql_transactional_without_txn() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TransactionalQueryRequest {
            query: "{ users { name } }".to_string(),
            operation_name: None,
            variables: None,
            txn_id: None,
        };

        let response = graphql_transactional(State(state), Json(request)).await;
        assert!(response.data.is_some());
        assert!(!response.has_errors());
    }

    #[tokio::test]
    async fn test_graphql_transactional_with_txn() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TransactionalQueryRequest {
            query: "{ users { name } }".to_string(),
            operation_name: None,
            variables: None,
            txn_id: Some("mock-txn-001".to_string()),
        };

        let response = graphql_transactional(State(state), Json(request)).await;
        assert!(response.data.is_some());
        assert!(!response.has_errors());
    }

    #[tokio::test]
    async fn test_graphql_transactional_invalid_txn_id() {
        let state = AppState {
            executor: Arc::new(MockQueryExecutor::new()),
        };
        let request = TransactionalQueryRequest {
            query: "{ users { name } }".to_string(),
            operation_name: None,
            variables: None,
            txn_id: Some("".to_string()), // Empty string is invalid
        };

        let response = graphql_transactional(State(state), Json(request)).await;
        assert!(response.has_errors());
        assert!(response.errors[0]
            .message
            .contains("Invalid transaction ID"));
    }
}
