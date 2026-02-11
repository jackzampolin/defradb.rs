//! GraphQL query handlers.

use std::convert::Infallible;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse,
    },
    Json,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use query::executor::{QueryRequest, QueryResponse};
use query::subscription::{
    extract_doc_id_from_query, is_commits_subscription, response_has_data,
    subscription_to_commits_query_with_cid, subscription_to_query_with_doc_id,
};
use query::{parse_request, ParsedOperation};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

use super::TX_HEADER_NAME;

/// Determine the required NAC permission based on GraphQL operation type.
///
/// - Query operations require `DocumentRead` permission
/// - Mutation operations require `DocumentUpdate` permission
/// - Parse failures default to `DocumentUpdate` (fail-secure)
///
/// This matches Go DefraDB's per-operation permission model where different
/// operation types have different permission requirements.
pub(crate) fn permission_for_query(query: &str) -> NodePermission {
    match parse_request(query) {
        Ok(ParsedOperation::Query { .. }) => NodePermission::DocumentRead,
        Ok(ParsedOperation::Subscription { .. }) => NodePermission::DocumentRead,
        Ok(ParsedOperation::Introspection { .. }) => NodePermission::DocumentRead,
        Ok(ParsedOperation::Mutation { .. }) => NodePermission::DocumentUpdate,
        // Parse failures default to the more restrictive permission
        Err(_) => NodePermission::DocumentUpdate,
    }
}

/// Check if a request has an `Accept: text/event-stream` header.
fn wants_sse(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false)
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

/// Extended GraphQL request with optional transaction ID.
#[derive(Debug, Clone, Deserialize)]
pub struct TransactionalQueryRequest {
    pub query: String,
    #[serde(rename = "operationName")]
    pub operation_name: Option<String>,
    pub variables: Option<JsonValue>,
    pub txn_id: Option<String>,
}

/// GraphQL POST request handler with optional transaction and SSE subscription support.
/// Identity is extracted from the Authorization header and used for ACP checks.
///
/// # Subscription Support (Go-compatible)
///
/// If the query is a subscription and the request has `Accept: text/event-stream`,
/// the handler streams results as SSE events (matching Go DefraDB's `/graphql` handler).
/// If the query is a subscription without SSE Accept header, returns 406 Not Acceptable.
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
) -> Result<axum::response::Response, HttpError> {
    // NAC check: Determine required permission based on operation type
    let required_permission = permission_for_query(&request.query);
    require_permission(&state, &identity, required_permission).await?;

    // Check if this is a subscription query
    if matches!(
        parse_request(&request.query),
        Ok(ParsedOperation::Subscription { .. })
    ) {
        if !wants_sse(&headers) {
            return Err(HttpError::NotAcceptable(
                "invalid subscription transport".to_string(),
            ));
        }
        return graphql_sse(state, identity, request)
            .await
            .map(|sse| sse.into_response());
    }

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
                    )))
                    .into_response());
                }
            };
            state.executor.execute_in_txn(query_request, &handle).await
        }
        None => state.executor.execute(query_request).await,
    };

    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL query returned errors");
    }
    Ok(Json(response).into_response())
}

/// SSE subscription handler.
///
/// Streams subscription results as Server-Sent Events, matching Go DefraDB's
/// `/graphql` SSE behavior. Each update event triggers the subscription query
/// to be re-executed against the current database state.
///
/// SSE event format:
/// - `event: next` + `data: {json}` for each result
/// - `event: complete` + `data: {}` when the stream ends
async fn graphql_sse(
    state: AppState,
    identity: ExtractIdentity,
    request: TransactionalQueryRequest,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, HttpError> {
    let event_bus = state.event_bus.as_ref().ok_or_else(|| {
        HttpError::ServiceUnavailable("event bus is not available for subscriptions".to_string())
    })?;

    let mut subscription = event_bus.subscribe(&[events::EventName::Update]);
    let query_str = request.query;
    let did = identity.into_did();

    let subscription_doc_id = extract_doc_id_from_query(&query_str);
    let is_commits = is_commits_subscription(&query_str);

    let executor = state.executor.clone();

    let stream = async_stream::stream! {
        while let Some(message) = subscription.recv().await {
            if let Some(update) = message.as_update() {
                let event_doc_id = update.doc_id.clone();

                // Check subscription docID filter
                if let Some(ref sub_doc) = subscription_doc_id {
                    if event_doc_id != *sub_doc {
                        continue;
                    }
                }

                // Convert subscription to a scoped query
                let query_text = if is_commits {
                    let cid_str = update.cid.to_string();
                    subscription_to_commits_query_with_cid(&query_str, &cid_str)
                } else {
                    subscription_to_query_with_doc_id(&query_str, &event_doc_id)
                };

                let mut req = QueryRequest::new(query_text);
                if did.is_some() {
                    req = req.with_identity(did.clone());
                }
                let response = executor.execute(req).await;

                // Skip empty results (filter excluded the document)
                if !response_has_data(&response) {
                    continue;
                }

                if let Ok(json) = serde_json::to_string(&response) {
                    yield Ok(Event::default().event("next").data(json));
                }
            }
        }

        // Stream ended — send complete event
        yield Ok(Event::default().event("complete").data("{}"));
    };

    Ok(Sse::new(stream))
}

/// GraphQL WebSocket handler for subscriptions.
///
/// Subscriptions over WebSocket are not yet implemented.
/// This handler returns 501 Not Implemented.
pub async fn graphql_ws_handler() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "GraphQL subscriptions over WebSocket are not yet implemented",
    )
}
