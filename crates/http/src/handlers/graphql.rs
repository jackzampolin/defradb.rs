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
use std::time::Duration;

/// Go DefraDB transaction header name.
const TX_HEADER_NAME: &str = "x-defradb-tx";

/// Connection init timeout per graphql-ws spec (5 seconds).
const CONNECTION_INIT_TIMEOUT: Duration = Duration::from_secs(5);

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use events::EventName;
use identity::{Did, Identity};
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

/// Connection parameters from connection_init (may contain identity).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ConnectionParams {
    /// Optional authorization token for identity.
    #[serde(rename = "Authorization")]
    pub authorization: Option<String>,
    /// Alternative: bearer token directly.
    #[serde(rename = "authToken")]
    pub auth_token: Option<String>,
}

/// Raw WebSocket message for GraphQL subscriptions (graphql-ws protocol).
/// Uses generic payload to handle both connection_init and subscribe messages.
#[derive(Debug, Deserialize, Serialize)]
pub struct RawSubscriptionMessage {
    /// Message type: "connection_init", "subscribe", "complete", "ping", "pong"
    #[serde(rename = "type")]
    pub msg_type: String,
    /// Subscription ID (for subscribe/complete)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Payload - varies by message type (connection params or subscription query)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
}

impl RawSubscriptionMessage {
    /// Extract connection params from connection_init payload.
    pub fn get_connection_params(&self) -> Option<ConnectionParams> {
        self.payload
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Extract subscription payload from subscribe message.
    pub fn get_subscription_payload(&self) -> Option<SubscriptionPayload> {
        self.payload
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// Legacy message type alias (used in response messages).
pub type SubscriptionMessage = RawSubscriptionMessage;

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
///
/// # NAC Permission Check
///
/// Before upgrading, this checks that the client has DocumentRead permission
/// if NAC is enabled. This prevents unauthorized users from establishing
/// WebSocket connections even though per-query permission checks also occur.
pub async fn graphql_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<impl IntoResponse, HttpError> {
    // NAC check: Subscriptions require DocumentRead permission
    // Check before WebSocket upgrade to prevent unauthorized connections
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;

    Ok(ws.on_upgrade(move |socket| handle_subscription_socket(socket, state)))
}

/// Keep-alive interval for WebSocket connections.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Subscription state tracking for active subscriptions.
struct SubscriptionState {
    /// Identity extracted from connection_init (if any).
    identity: Option<Did>,
    /// The subscription query.
    query: String,
    /// Query variables.
    variables: Option<JsonValue>,
    /// Collection name being subscribed to.
    collection_name: String,
    /// Event bus subscription handle.
    subscription: events::Subscription,
}

/// Handle an established WebSocket connection for subscriptions.
///
/// Implements the graphql-ws protocol:
/// 1. Wait for connection_init message (with timeout)
/// 2. Send connection_ack
/// 3. Accept multiple subscribe messages (supports multiple subscriptions per connection)
/// 4. Stream updates with keep-alive until complete or close
async fn handle_subscription_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    // Get the event bus
    let event_bus = match &state.event_bus {
        Some(bus) => bus.clone(),
        None => {
            tracing::error!("WebSocket subscription attempted but no event bus configured");
            let error_msg = serde_json::json!({
                "type": "error",
                "payload": {"message": "Subscriptions not enabled: no event bus configured"}
            });
            let _ = sender
                .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                .await;
            return;
        }
    };

    // === Phase 1: Wait for connection_init with timeout ===
    let connection_identity = match wait_for_connection_init(&mut receiver, CONNECTION_INIT_TIMEOUT).await {
        Ok(identity) => {
            // Send connection_ack
            let ack = serde_json::json!({"type": "connection_ack"});
            if sender
                .send(Message::Text(serde_json::to_string(&ack).unwrap()))
                .await
                .is_err()
            {
                tracing::debug!("Failed to send connection_ack, closing connection");
                return;
            }
            tracing::debug!(identity = ?identity, "WebSocket connection initialized");
            identity
        }
        Err(e) => {
            tracing::warn!(error = %e, "WebSocket handshake failed");
            let error_msg = serde_json::json!({
                "type": "connection_error",
                "payload": {"message": e}
            });
            let _ = sender
                .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                .await;
            return;
        }
    };

    // === Phase 2: Process subscription messages with multiple subscription support ===
    // Track active subscriptions by their client-provided ID
    let mut active_subscriptions: std::collections::HashMap<String, SubscriptionState> =
        std::collections::HashMap::new();

    // Keep-alive timer
    let mut keep_alive_interval = tokio::time::interval(KEEP_ALIVE_INTERVAL);
    keep_alive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Use select! to handle WebSocket messages, event bus updates, and keep-alive concurrently
        tokio::select! {
            // Keep-alive tick
            _ = keep_alive_interval.tick() => {
                let ka = serde_json::json!({"type": "ka"});
                if sender.send(Message::Text(serde_json::to_string(&ka).unwrap())).await.is_err() {
                    tracing::debug!("Failed to send keep-alive, closing connection");
                    break;
                }
                tracing::trace!("Sent keep-alive");
            }

            // WebSocket message received
            msg_result = receiver.next() => {
                let msg = match msg_result {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Close(_))) => {
                        tracing::debug!("WebSocket closed by client");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = sender.send(Message::Pong(data)).await;
                        continue;
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "WebSocket receive error");
                        break;
                    }
                    None => {
                        tracing::debug!("WebSocket stream ended");
                        break;
                    }
                };

                // Parse the subscription message
                let sub_msg: RawSubscriptionMessage = match serde_json::from_str(&msg) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(error = %e, "Invalid subscription message format");
                        continue;
                    }
                };

                match sub_msg.msg_type.as_str() {
                    "connection_init" => {
                        // Already initialized - per spec, we can ignore duplicate connection_init
                        tracing::debug!("Ignoring duplicate connection_init");
                        continue;
                    }
                    "connection_terminate" => {
                        // Client requested clean termination
                        tracing::debug!("Client requested connection termination");
                        break;
                    }
                    "subscribe" => {
                        let sub_id = sub_msg.id.clone().unwrap_or_else(|| "default".to_string());

                        // Check for duplicate subscription ID
                        if active_subscriptions.contains_key(&sub_id) {
                            let error_msg = SubscriptionData {
                                msg_type: "error".to_string(),
                                id: sub_id,
                                payload: Some(QueryResponse::error("Subscriber for this ID already exists")),
                            };
                            let _ = sender
                                .send(Message::Text(serde_json::to_string(&error_msg).unwrap()))
                                .await;
                            continue;
                        }

                        let payload = match sub_msg.get_subscription_payload() {
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

                        // Parse to verify it's a subscription and extract the Select
                        let collection_name = match parse_request(&payload.query) {
                            Ok(ParsedOperation::Subscription { select }) => {
                                select.collection_name.clone()
                            }
                            Ok(_) => {
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

                        // Subscribe to events
                        let subscription = event_bus.subscribe(&[EventName::Update]);

                        // Store subscription state
                        let sub_state = SubscriptionState {
                            identity: connection_identity.clone(),
                            query: payload.query.clone(),
                            variables: payload.variables.clone(),
                            collection_name: collection_name.clone(),
                            subscription,
                        };

                        // Execute initial query with identity
                        let initial_request = QueryRequest {
                            query: sub_state.query.clone(),
                            operation_name: None,
                            variables: sub_state.variables.clone(),
                            identity: sub_state.identity.clone(),
                        };
                        let initial_response = state.executor.execute(initial_request).await;

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

                        active_subscriptions.insert(sub_id.clone(), sub_state);
                        tracing::debug!(sub_id = %sub_id, collection = %collection_name, "Subscription started");
                    }
                    "ping" => {
                        let pong = serde_json::json!({"type": "pong"});
                        let _ = sender
                            .send(Message::Text(serde_json::to_string(&pong).unwrap()))
                            .await;
                    }
                    "complete" => {
                        // Client wants to end a specific subscription
                        if let Some(sub_id) = sub_msg.id {
                            if active_subscriptions.remove(&sub_id).is_some() {
                                tracing::debug!(sub_id = %sub_id, "Subscription completed by client");
                            }
                        }
                    }
                    _ => {
                        tracing::debug!(msg_type = %sub_msg.msg_type, "Unknown message type");
                    }
                }
            }
        }

        // Check all active subscriptions for updates
        // We need to do this outside of select! to avoid borrow issues
        let mut subscriptions_to_remove: Vec<String> = Vec::new();

        for (sub_id, sub_state) in active_subscriptions.iter_mut() {
            // Check for dropped messages (resync needed)
            let dropped = sub_state.subscription.check_and_reset_dropped();
            if dropped > 0 {
                tracing::warn!(
                    sub_id = %sub_id,
                    dropped = dropped,
                    "Messages were dropped, client may need to resync"
                );
                // Re-execute the full query to give the client fresh state
                let request = QueryRequest {
                    query: sub_state.query.clone(),
                    operation_name: None,
                    variables: sub_state.variables.clone(),
                    identity: sub_state.identity.clone(),
                };
                let response = state.executor.execute(request).await;

                let data_msg = SubscriptionData {
                    msg_type: "next".to_string(),
                    id: sub_id.clone(),
                    payload: Some(response),
                };
                if sender
                    .send(Message::Text(serde_json::to_string(&data_msg).unwrap()))
                    .await
                    .is_err()
                {
                    subscriptions_to_remove.push(sub_id.clone());
                    continue;
                }
            }

            // Try to receive any pending events (non-blocking)
            while let Ok(message) = sub_state.subscription.try_recv() {
                if let Some(update_data) = message.as_update() {
                    // Skip relay events to avoid duplicates (Issue #6)
                    if update_data.is_relay {
                        tracing::trace!(
                            doc_id = %update_data.doc_id,
                            sub_id = %sub_id,
                            "Skipping relay event for subscription"
                        );
                        continue;
                    }

                    // Check if update is relevant to this subscription
                    // SECURITY FIX: Removed empty collection_id fallback that could leak data
                    if update_data.collection_id == sub_state.collection_name {
                        // Re-execute query with identity preserved
                        let request = QueryRequest {
                            query: sub_state.query.clone(),
                            operation_name: None,
                            variables: sub_state.variables.clone(),
                            identity: sub_state.identity.clone(),
                        };
                        let response = state.executor.execute(request).await;

                        let data_msg = SubscriptionData {
                            msg_type: "next".to_string(),
                            id: sub_id.clone(),
                            payload: Some(response),
                        };
                        if sender
                            .send(Message::Text(serde_json::to_string(&data_msg).unwrap()))
                            .await
                            .is_err()
                        {
                            subscriptions_to_remove.push(sub_id.clone());
                            break;
                        }
                    } else if update_data.collection_id.is_empty() {
                        // Log warning instead of silently matching
                        tracing::warn!(
                            doc_id = %update_data.doc_id,
                            "Received event with empty collection_id, ignoring"
                        );
                    }
                }
            }
        }

        // Remove failed subscriptions
        for sub_id in subscriptions_to_remove {
            active_subscriptions.remove(&sub_id);
        }

        // Small yield to prevent busy-loop when there are no events
        if !active_subscriptions.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Send complete messages for all active subscriptions before closing
    for sub_id in active_subscriptions.keys() {
        let complete_msg = serde_json::json!({
            "type": "complete",
            "id": sub_id
        });
        let _ = sender
            .send(Message::Text(serde_json::to_string(&complete_msg).unwrap()))
            .await;
    }

    tracing::debug!(
        subscriptions_closed = active_subscriptions.len(),
        "WebSocket subscription connection closed"
    );
}

/// Wait for connection_init message from client.
///
/// Returns the identity DID extracted from connectionParams if present.
/// Token validation is simplified for WebSocket (signature verified, but no audience check
/// since we don't have HTTP Host header in this context).
async fn wait_for_connection_init(
    receiver: &mut futures::stream::SplitStream<WebSocket>,
    timeout: Duration,
) -> Result<Option<Did>, String> {
    let init_future = async {
        while let Some(msg_result) = receiver.next().await {
            let msg = match msg_result {
                Ok(Message::Text(text)) => text,
                Ok(Message::Ping(_)) => continue, // Ignore pings during handshake
                Ok(Message::Close(_)) => return Err("Connection closed before init".to_string()),
                Ok(_) => continue,
                Err(e) => return Err(format!("Receive error: {}", e)),
            };

            // Parse the message
            let sub_msg: RawSubscriptionMessage = match serde_json::from_str(&msg) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "Invalid message format during handshake");
                    continue;
                }
            };

            if sub_msg.msg_type == "connection_init" {
                // Extract and parse token from connectionParams if present
                let identity = sub_msg.get_connection_params().and_then(|params| {
                    // Get the raw token (strip Bearer prefix if present)
                    let token = params.authorization
                        .as_ref()
                        .map(|auth| auth.strip_prefix("Bearer ").unwrap_or(auth).trim())
                        .or(params.auth_token.as_deref());

                    let token = token?;
                    if token.is_empty() {
                        return None;
                    }

                    // Parse token and extract DID (validates signature)
                    // Skip audience verification since we don't have HTTP Host header
                    match identity::from_token(token.as_bytes()) {
                        Ok(token_identity) => {
                            match token_identity.did() {
                                Ok(did) => {
                                    tracing::debug!(did = %did, "Extracted identity from WebSocket connection");
                                    Some(did)
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "Failed to extract DID from token");
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Failed to parse auth token from connectionParams");
                            None
                        }
                    }
                });

                tracing::debug!(has_identity = identity.is_some(), "Received connection_init");
                return Ok(identity);
            } else {
                // Per graphql-ws spec, must send connection_init first
                return Err(format!(
                    "Expected connection_init, got {}",
                    sub_msg.msg_type
                ));
            }
        }
        Err("Connection closed without init".to_string())
    };

    match tokio::time::timeout(timeout, init_future).await {
        Ok(result) => result,
        Err(_) => Err("Connection init timeout".to_string()),
    }
}
