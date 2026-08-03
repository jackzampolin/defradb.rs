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

fn empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    Ok(opt.filter(|s| !s.is_empty()))
}
use query::subscription::{
    is_subscription_operation_with_limits, response_has_data, subscription_accepts_doc_id,
    subscription_doc_ids_with_limits, subscription_to_scoped_query,
};
use query::{parse_request_with_limits, ParsedOperation, QueryLimits};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::query_context::{
    execute_in_txn_with_context, execute_with_context, execute_with_resolved_context,
};
use crate::router::{AppState, NodePermission};

use super::TX_HEADER_NAME;

fn record_response_metrics(response: &QueryResponse) {
    if response.is_transaction_conflict() {
        telemetry::record_escaped_conflict("graphql");
    }
}

/// Check if a query references `encrypted_` fields and P2P is disabled.
///
/// Go DefraDB only generates `encrypted_<Collection>` GraphQL fields when P2P
/// is enabled. Returns a schema validation error matching Go's format if the
/// query references encrypted fields but P2P is not available.
///
/// Parses the query into an AST to avoid false positives from `encrypted_`
/// appearing inside string literals (e.g. filter values).
fn check_encrypted_fields(state: &AppState, query: &str) -> Result<(), HttpError> {
    if state.p2p.is_some() {
        return Ok(());
    }
    let parsed = match parse_request_with_limits(query, None, None, state.query_limits) {
        Ok(p) => p,
        Err(_) => return Ok(()),
    };
    let encrypted_select = match &parsed {
        ParsedOperation::Query { selects, .. } => selects.iter().find(|s| s.is_encrypted),
        ParsedOperation::Subscription { select, .. } => {
            if select.is_encrypted {
                Some(select.as_ref())
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some(select) = encrypted_select {
        return Err(HttpError::BadRequest(format!(
            "Cannot query field \"encrypted_{}\" on type \"Query\".",
            select.collection_name
        )));
    }
    Ok(())
}

/// Fast check for whether a query could be a subscription.
///
/// Returns `false` if the query definitely starts with `mutation` or `query` or `{`,
/// which means it cannot be a subscription. Returns `true` if the query might be a
/// subscription (starts with `subscription` or is ambiguous), requiring a full parse.
fn might_be_subscription(query: &str) -> bool {
    let trimmed = query.trim_start();
    // mutation/query/anonymous queries are definitely not subscriptions
    !(trimmed.starts_with("mutation") || trimmed.starts_with("query") || trimmed.starts_with('{'))
}

/// Check if a request has an `Accept: text/event-stream` header.
fn wants_sse(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false)
}

/// Determine the NAC permission required for a GraphQL query string.
///
/// - Query operations → `DocumentRead`
/// - Delete mutations → `DocumentDelete`
/// - Other mutations → `DocumentUpdate`
/// - Subscriptions → `DocumentRead`
/// - Introspection → `DocumentRead`
fn graphql_required_permission(query: &str, limits: QueryLimits) -> NodePermission {
    match parse_request_with_limits(query, None, None, limits) {
        Ok(ParsedOperation::Mutation { mutations, .. }) => {
            if mutations
                .iter()
                .any(|m| m.mutation_type == query::MutationType::Delete)
            {
                NodePermission::DocumentDelete
            } else {
                NodePermission::DocumentUpdate
            }
        }
        _ => NodePermission::DocumentRead,
    }
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
/// - Delete mutations require `DocumentDelete` permission
/// - Other mutations require `DocumentUpdate` permission
///
/// This matches Go DefraDB's per-operation permission model where each
/// operation type has its own permission requirement.
pub async fn graphql(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(mut request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, HttpError> {
    // Validate encrypted field queries require P2P
    check_encrypted_fields(&state, &request.query)?;

    // Enforce NAC permission before executing the query
    let permission = graphql_required_permission(&request.query, state.query_limits);
    require_permission(&state, &identity, permission).await?;

    // Wire identity from Authorization header into the request
    request.identity = identity.did().cloned();
    let response = execute_with_context(&state, &identity, request).await;
    record_response_metrics(&response);
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
    // Validate encrypted field queries require P2P
    check_encrypted_fields(&state, &params.query)?;

    // GET is always a read operation
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
        identity: identity.did().cloned(),
    };

    let response = execute_with_context(&state, &identity, request).await;
    record_response_metrics(&response);
    if response.has_errors() {
        tracing::warn!(errors = ?response.errors, "GraphQL GET query returned errors");
    }
    Ok(Json(response))
}

/// Extended GraphQL request with optional transaction ID.
#[derive(Debug, Clone, Deserialize)]
pub struct TransactionalQueryRequest {
    pub query: String,
    #[serde(
        rename = "operationName",
        default,
        deserialize_with = "empty_string_as_none"
    )]
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
/// - Delete mutations require `DocumentDelete` permission
/// - Other mutations require `DocumentUpdate` permission
///
/// This matches Go DefraDB's per-operation permission model.
pub async fn graphql_transactional(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    headers: HeaderMap,
    Json(request): Json<TransactionalQueryRequest>,
) -> Result<axum::response::Response, HttpError> {
    // Validate encrypted field queries require P2P
    check_encrypted_fields(&state, &request.query)?;

    // Enforce NAC permission before executing the query
    let permission = graphql_required_permission(&request.query, state.query_limits);
    require_permission(&state, &identity, permission).await?;

    // Check if this is a subscription query.
    // Fast path: skip the full GraphQL parse for mutations/queries (the common case).
    // Only do the full parse when the query might be a subscription.
    if might_be_subscription(&request.query)
        && is_subscription_operation_with_limits(
            &request.query,
            request.variables.as_ref(),
            request.operation_name.as_deref(),
            state.query_limits,
        )
    {
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
        identity: identity.did().cloned(),
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
            execute_in_txn_with_context(&state, &identity, query_request, handle).await
        }
        None => execute_with_context(&state, &identity, query_request).await,
    };

    record_response_metrics(&response);
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
    let operation_name = request.operation_name;
    let variables = request.variables;
    let did = identity.did().cloned();
    let acting_did = did.as_ref().map(|d| d.as_str().to_string());

    // Resolve signing config and DAC bypass once at subscription setup time
    let signing_config = crate::query_context::resolve_signing_config(&state, &identity);
    let dac_bypass = crate::query_context::resolve_dac_bypass(&state, &identity).await;

    let subscription_doc_ids_filter = subscription_doc_ids_with_limits(
        &query_str,
        variables.as_ref(),
        operation_name.as_deref(),
        state.query_limits,
    );

    let executor = state.executor.clone();

    let stream = async_stream::stream! {
        while let Some(message) = subscription.recv().await {
            if let Some(update) = message.as_update() {
                let event_doc_id = update.doc_id.clone();

                if !subscription_accepts_doc_id(
                    subscription_doc_ids_filter.as_deref(),
                    &event_doc_id,
                ) {
                    continue;
                }

                let cid_str = update.cid.to_string();
                let query_text = match subscription_to_scoped_query(
                    &query_str,
                    &event_doc_id,
                    &cid_str,
                    operation_name.as_deref(),
                ) {
                    Ok(query) => query,
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to scope subscription query");
                        continue;
                    }
                };

                let mut req = QueryRequest::new(query_text);
                if did.is_some() {
                    req = req.with_identity(did.clone());
                }
                if let Some(ref op_name) = operation_name {
                    req = req.with_operation_name(op_name.clone());
                }
                if let Some(ref vars) = variables {
                    req = req.with_variables(vars.clone());
                }
                let response = execute_with_resolved_context(
                    executor.clone(),
                    req,
                    signing_config.clone(),
                    dac_bypass,
                    acting_did.clone(),
                ).await;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use query::executor::QueryExecutor;

    use super::*;
    use crate::{router::AppStateBuilder, MockQueryExecutor};

    fn state_with_limits(query_limits: QueryLimits) -> AppState {
        AppStateBuilder::new(Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>)
            .with_query_limits(query_limits)
            .build()
    }

    fn deep_encrypted_query(depth: usize) -> String {
        let mut query = "query { encrypted_User ".to_string();
        for level in 0..depth {
            query.push_str(&format!("{{ f{level} "));
        }
        query.push_str("{ leaf }");
        for _ in 0..depth {
            query.push('}');
        }
        query.push_str(" }");
        query
    }

    #[test]
    fn encrypted_field_preparse_uses_configured_query_depth_limit() {
        let query = deep_encrypted_query(30);
        assert!(query::parse_request(&query).is_err());

        let default_state = state_with_limits(QueryLimits::default());
        assert!(check_encrypted_fields(&default_state, &query).is_ok());

        let raised_state = state_with_limits(QueryLimits {
            max_query_depth: 100,
            ..QueryLimits::default()
        });
        let err = check_encrypted_fields(&raised_state, &query).unwrap_err();

        match err {
            HttpError::BadRequest(message) => {
                assert!(message.contains("Cannot query field \"encrypted_User\""));
            }
            other => panic!("expected encrypted field validation error, got {other:?}"),
        }
    }

    #[test]
    fn response_metrics_count_only_typed_conflicts() {
        let before = telemetry::conflict_metrics_snapshot().escaped_to_clients;
        record_response_metrics(&QueryResponse::error("validation failed"));
        record_response_metrics(&QueryResponse::transaction_conflict("conflict"));
        let after = telemetry::conflict_metrics_snapshot().escaped_to_clients;

        assert_eq!(after - before, 1);
    }
}
