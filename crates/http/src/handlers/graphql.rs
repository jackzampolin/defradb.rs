//! GraphQL and transaction HTTP handlers.
//!
//! # NAC Permission Model
//!
//! GraphQL endpoint permissions are checked based on the operation type:
//! - Query operations require `DocumentRead` permission
//! - Mutation operations require `DocumentUpdate` permission
//! - Subscription operations require `DocumentRead` permission
//!
//! This matches Go DefraDB's per-operation permission model more closely,
//! where each operation type has its own permission requirement.
//!
//! # Subscriptions via WebSocket
//!
//! GraphQL subscriptions are supported via WebSocket connections.
//! Connect to `/api/v0/graphql/ws` to establish a subscription.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use futures::{SinkExt, StreamExt};

/// Go DefraDB transaction header name.
const TX_HEADER_NAME: &str = "x-defradb-tx";
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use events::EventName;
use query::executor::{QueryRequest, QueryResponse};
use query::{parse_request, ParsedOperation};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Determine the required NAC permission based on GraphQL operation type.
///
/// - Query operations require `DocumentRead` permission
/// - Subscription operations require `DocumentRead` permission
/// - Mutation operations require `DocumentUpdate` permission
/// - Parse failures default to `DocumentUpdate` (fail-secure)
///
/// This matches Go DefraDB's per-operation permission model where different
/// operation types have different permission requirements.
fn permission_for_query(query: &str) -> NodePermission {
    match parse_request(query) {
        Ok(ParsedOperation::Query { .. }) => NodePermission::DocumentRead,
        Ok(ParsedOperation::Subscription { .. }) => NodePermission::DocumentRead,
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
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_begin(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(query): Query<TxBeginQuery>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: transactions can modify data, require DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

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
            (
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Failed to begin transaction: {}", e),
                }),
            )
                .into_response()
        }
    })
}

/// Begin a new concurrent transaction (Go-compatible).
///
/// POST /api/v0/tx/concurrent?read_only=true
///
/// Concurrent transactions allow multiple transactions to run in parallel.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_begin_concurrent(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(query): Query<TxBeginQuery>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: transactions can modify data, require DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    // For now, concurrent transactions use the same implementation as regular transactions.
    // The distinction is primarily semantic in Go DefraDB.
    Ok(match state.executor.begin_txn(query.read_only).await {
        Ok(handle) => {
            let id: u64 = handle.to_string().parse().unwrap_or(0);
            tracing::info!(
                txn_id = id,
                readonly = query.read_only,
                concurrent = true,
                "Concurrent transaction started"
            );
            (StatusCode::OK, Json(TxBeginResponse { id })).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to begin concurrent transaction");
            (
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: format!("Failed to begin transaction: {}", e),
                }),
            )
                .into_response()
        }
    })
}

/// Commit a transaction (Go-compatible).
///
/// POST /api/v0/tx/{id}
///
/// Go DefraDB uses path parameter for transaction ID and returns empty body on success.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_commit(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxPathParam>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: committing transactions requires DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

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
            Ok((
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: "invalid transaction id".to_string(),
                }),
            )
                .into_response())
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
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn tx_discard(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxPathParam>,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: discarding transactions requires DocumentUpdate permission
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

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
            Ok((
                StatusCode::BAD_REQUEST,
                Json(crate::error::ErrorResponse {
                    error: "invalid transaction id".to_string(),
                }),
            )
                .into_response())
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
///
/// # Transaction ID
///
/// Transaction ID can be provided in two ways (Go-compatible):
/// 1. `x-defradb-tx` header (preferred by Go DefraDB clients)
/// 2. `txn_id` field in request body
///
/// Header takes precedence if both are provided.
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
    headers: HeaderMap,
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

    // Check for transaction ID in header first (Go-compatible), then body
    let txn_id = headers
        .get(TX_HEADER_NAME)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or(request.txn_id);

    let response = match txn_id {
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

/// GraphQL subscription payload (query + variables).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubscriptionPayload {
    pub query: String,
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,
    pub variables: Option<JsonValue>,
}

/// WebSocket message for GraphQL subscriptions (graphql-ws protocol).
#[derive(Debug, Deserialize, Serialize)]
pub struct SubscriptionMessage {
    /// Message type: "subscribe", "complete", "ping", "pong"
    #[serde(rename = "type")]
    pub msg_type: String,
    /// Subscription ID (for subscribe/complete)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// GraphQL payload (for subscribe)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<SubscriptionPayload>,
}

/// WebSocket data message sent to client.
#[derive(Debug, Serialize)]
pub struct SubscriptionData {
    /// Message type: "next", "error", "complete"
    #[serde(rename = "type")]
    pub msg_type: String,
    /// Subscription ID
    pub id: String,
    /// Query response payload
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<QueryResponse>,
}

/// WebSocket upgrade handler for GraphQL subscriptions.
///
/// This endpoint handles WebSocket connections for GraphQL subscriptions.
/// Clients should connect to `/api/v0/graphql/ws` and send subscription
/// messages using the graphql-ws protocol format.
pub async fn graphql_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_subscription_socket(socket, state))
}

/// Handle an established WebSocket connection for subscriptions.
async fn handle_subscription_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    // Get the event bus
    let event_bus = match &state.event_bus {
        Some(bus) => bus.clone(),
        None => {
            tracing::error!("WebSocket subscription attempted but no event bus configured");
            let error_msg = SubscriptionData {
                msg_type: "error".to_string(),
                id: "".to_string(),
                payload: Some(QueryResponse::error(
                    "Subscriptions not enabled: no event bus configured",
                )),
            };
            let _ = sender
                .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                .await;
            return;
        }
    };

    // Process incoming messages
    while let Some(msg_result) = receiver.next().await {
        let msg = match msg_result {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                tracing::debug!("WebSocket closed by client");
                break;
            }
            Ok(Message::Ping(data)) => {
                let _ = sender.send(Message::Pong(data)).await;
                continue;
            }
            Ok(_) => continue,
            Err(e) => {
                tracing::warn!(error = %e, "WebSocket receive error");
                break;
            }
        };

        // Parse the subscription message
        let sub_msg: SubscriptionMessage = match serde_json::from_str(&msg) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "Invalid subscription message format");
                continue;
            }
        };

        match sub_msg.msg_type.as_str() {
            "subscribe" => {
                let sub_id = sub_msg.id.unwrap_or_else(|| "default".to_string());
                let payload = match sub_msg.payload {
                    Some(p) => p,
                    None => {
                        let error_msg = SubscriptionData {
                            msg_type: "error".to_string(),
                            id: sub_id,
                            payload: Some(QueryResponse::error("Missing payload in subscribe message")),
                        };
                        let _ = sender
                            .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                            .await;
                        continue;
                    }
                };

                // Parse to verify it's a subscription
                let parsed = match parse_request(&payload.query) {
                    Ok(p) => p,
                    Err(e) => {
                        let error_msg = SubscriptionData {
                            msg_type: "error".to_string(),
                            id: sub_id,
                            payload: Some(QueryResponse::error(format!("Parse error: {}", e))),
                        };
                        let _ = sender
                            .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                            .await;
                        continue;
                    }
                };

                let collection_name = match parsed {
                    ParsedOperation::Subscription { ref select } => select.collection_name.clone(),
                    _ => {
                        let error_msg = SubscriptionData {
                            msg_type: "error".to_string(),
                            id: sub_id,
                            payload: Some(QueryResponse::error(
                                "Expected subscription operation",
                            )),
                        };
                        let _ = sender
                            .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                            .await;
                        continue;
                    }
                };

                // Subscribe to events and start streaming
                let mut subscription = event_bus.subscribe(&[EventName::Update]);
                let executor = state.executor.clone();
                let query = payload.query.clone();
                let variables = payload.variables.clone();

                // Execute initial query
                let initial_request = QueryRequest {
                    query: query.clone(),
                    operation_name: None,
                    variables: variables.clone(),
                    identity: None,
                };
                let initial_response = executor.execute(initial_request).await;

                // Send initial result
                let initial_msg = SubscriptionData {
                    msg_type: "next".to_string(),
                    id: sub_id.clone(),
                    payload: Some(initial_response),
                };
                if sender
                    .send(Message::Text(serde_json::to_string(&initial_msg).unwrap()))
                    .await
                    .is_err()
                {
                    break;
                }

                // Stream updates until connection closes
                loop {
                    match subscription.recv().await {
                        Some(message) => {
                            if let Some(update_data) = message.as_update() {
                                // Check if update is relevant to this subscription
                                if update_data.collection_id == collection_name
                                    || update_data.collection_id.is_empty()
                                {
                                    // Re-execute query
                                    let request = QueryRequest {
                                        query: query.clone(),
                                        operation_name: None,
                                        variables: variables.clone(),
                                        identity: None,
                                    };
                                    let response = executor.execute(request).await;

                                    let data_msg = SubscriptionData {
                                        msg_type: "next".to_string(),
                                        id: sub_id.clone(),
                                        payload: Some(response),
                                    };
                                    if sender
                                        .send(Message::Text(
                                            serde_json::to_string(&data_msg).unwrap(),
                                        ))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        None => {
                            // Event bus closed
                            break;
                        }
                    }
                }

                // Subscription ended - exit the main loop too
                break;
            }
            "ping" => {
                let pong = serde_json::json!({"type": "pong"});
                let _ = sender
                    .send(Message::Text(serde_json::to_string(&pong).unwrap()))
                    .await;
            }
            "complete" => {
                // Client wants to end subscription
                tracing::debug!(id = ?sub_msg.id, "Subscription complete requested");
                break;
            }
            _ => {
                tracing::debug!(msg_type = %sub_msg.msg_type, "Unknown message type");
            }
        }
    }

    tracing::debug!("WebSocket subscription connection closed");
}
