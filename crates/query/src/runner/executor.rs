//! QueryExecutor trait implementation for QueryRunner.

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::future::Future;
use tracing::instrument;

use acp::nac::NodePermission;
use identity::Did;

use crate::error::{Result, TransactionError};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::query_parse::{
    parse_request_with_variables, validate_parsed_operation, ParsedOperation,
};
use crate::txn::{GetTransactionResult, TransactionHandle, TransactionRegistry};

use super::{DocFetcher, QueryRunner};

/// Await a future with an optional timeout (native only, WASM always awaits directly).
#[cfg(not(target_arch = "wasm32"))]
async fn await_with_timeout<F: Future<Output = Result<JsonValue>>>(
    future: F,
    timeout_secs: u64,
) -> Result<JsonValue> {
    if timeout_secs > 0 {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        match tokio::time::timeout(timeout, future).await {
            Ok(r) => r,
            Err(_) => Err(crate::error::QueryError::execution(format!(
                "query execution timed out after {} seconds",
                timeout_secs
            ))),
        }
    } else {
        future.await
    }
}

#[cfg(target_arch = "wasm32")]
async fn await_with_timeout<F: Future<Output = Result<JsonValue>>>(
    future: F,
    _timeout_secs: u64,
) -> Result<JsonValue> {
    future.await
}

/// Map a parsed operation to the required NAC permission.
fn permission_for_operation(parsed: &ParsedOperation) -> NodePermission {
    match parsed {
        ParsedOperation::Query { .. } => NodePermission::DocumentRead,
        ParsedOperation::Subscription { .. } => NodePermission::DocumentRead,
        ParsedOperation::Introspection { .. } => NodePermission::DocumentRead,
        ParsedOperation::Mutation { mutations, .. } => {
            if mutations
                .iter()
                .any(|m| m.mutation_type == crate::mapper::MutationType::Delete)
            {
                NodePermission::DocumentDelete
            } else {
                NodePermission::DocumentUpdate
            }
        }
    }
}

/// Check NAC permission and return a denial response if not authorized.
///
/// Go enforces NAC at the data layer — denied queries return HTTP 200 with
/// empty data (not a GraphQL error). We match that by returning an empty
/// JSON object as data when NAC denies a request.
async fn check_nac<F: DocFetcher + 'static, R: crate::txn::TransactionRegistry>(
    runner: &QueryRunner<F, R>,
    identity: &Option<Did>,
    parsed: &ParsedOperation,
) -> Option<QueryResponse> {
    let nac = runner.nac.as_ref()?;
    let did = match identity {
        Some(d) => d.clone(),
        None => Did::wildcard(),
    };
    let permission = permission_for_operation(parsed);
    if !nac.check_permission(&did, permission).await {
        return Some(QueryResponse::success(serde_json::json!({})));
    }
    None
}

/// Convert JSON variables from request format to parser format.
/// Variables in requests are `Option<JsonValue>` (a JSON object), but the
/// parser expects `Option<HashMap<String, JsonValue>>`.
fn convert_variables(variables: &Option<JsonValue>) -> Option<HashMap<String, JsonValue>> {
    variables.as_ref().and_then(|v| {
        if let JsonValue::Object(map) = v {
            Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        } else {
            None // Non-object variables are ignored (invalid per GraphQL spec)
        }
    })
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryExecutor for QueryRunner<F, R> {
    #[instrument(
        name = "query.execute_request",
        skip(self, request),
        fields(query_len = request.query.len())
    )]
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        // Convert variables from JSON to HashMap format for the parser
        let variables = convert_variables(&request.variables);

        // First, parse the request to determine if it's a query or mutation
        let parsed = match parse_request_with_variables(
            &request.query,
            variables.as_ref(),
            request.operation_name.as_deref(),
        ) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                };
            }
        };

        // Validate that all referenced collections exist before execution
        if let Err(e) = validate_parsed_operation(&parsed, self.effective_provider().as_ref()).await
        {
            return QueryResponse {
                data: None,
                errors: vec![QueryResponseError {
                    message: e.to_string(),
                    path: None,
                    locations: None,
                }],
            };
        }

        // NAC check uses the raw request identity (not default fallback).
        // The default_identity is for document ACP, not node access control.
        if let Some(denial) = check_nac(self, &request.identity, &parsed).await {
            return denial;
        }

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        // Route to appropriate handler based on operation type
        // Pass identity and variables through for ACP permission checks and variable substitution
        let execution = async {
            match parsed {
                ParsedOperation::Query {
                    mut selects,
                    explain,
                    exhaustive,
                } => {
                    if exhaustive {
                        for s in &mut selects {
                            s.exhaustive = true;
                        }
                    }
                    if let Some(explain_type) = explain {
                        self.explain_query_with_identity_and_vars(
                            &request.query,
                            identity,
                            explain_type,
                            variables.as_ref(),
                        )
                        .await
                    } else {
                        self.execute_selects_internal(selects, self.fetcher.as_ref(), identity)
                            .await
                    }
                }
                ParsedOperation::Mutation {
                    mutations, explain, ..
                } => {
                    if let Some(explain_type) = explain {
                        // Return mutation plan instead of executing
                        self.explain_mutation_with_identity(&request.query, identity, explain_type)
                            .await
                    } else {
                        // Use pre-parsed mutations to avoid redundant re-parsing
                        match self.mutator.as_ref() {
                            Some(mutator) => {
                                self.execute_parsed_mutations(
                                    mutations,
                                    mutator.clone(),
                                    identity,
                                    None,
                                )
                                .await
                            }
                            None => Err(crate::error::QueryError::execution(
                                "mutations require a mutator; call with_mutator() first",
                            )),
                        }
                    }
                }
                ParsedOperation::Subscription { .. } => {
                    // Subscriptions require SSE transport - they cannot be executed via regular request/response
                    Err(crate::error::QueryError::parse(
                        "Subscriptions must be executed via Server-Sent Events (SSE). \
                         Send the request with Accept: text/event-stream header.",
                    ))
                }
                ParsedOperation::Introspection { query } => {
                    // Introspection queries are executed against the GraphQL schema
                    self.execute_introspection(&query).await
                }
            }
        };

        let result = await_with_timeout(execution, self.query_timeout).await;

        match result {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    error = %e,
                    "Query execution failed"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                }
            }
        }
    }

    #[instrument(
        name = "query.execute_in_txn",
        skip(self, request),
        fields(query_len = request.query.len(), txn_id = %handle)
    )]
    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        // Look up the transaction in the registry
        let txn_ctx = match self.registry.get(handle) {
            GetTransactionResult::Found(ctx) => ctx,
            GetTransactionResult::NotFound => {
                return QueryResponse::error(format!(
                    "transaction '{}' not found or has been committed/rolled back",
                    handle
                ));
            }
            GetTransactionResult::LockPoisoned => {
                return QueryResponse::error(format!(
                    "transaction registry lock poisoned - system may be in corrupted state (transaction '{}')",
                    handle
                ));
            }
        };

        // Convert variables from JSON to HashMap format for the parser
        let variables = convert_variables(&request.variables);

        // Parse the request to determine if it's a query or mutation
        let parsed = match parse_request_with_variables(
            &request.query,
            variables.as_ref(),
            request.operation_name.as_deref(),
        ) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                };
            }
        };

        #[cfg(not(target_arch = "wasm32"))]
        let txn_provider = txn_ctx.collection_provider();

        // Validate that all referenced collections exist before execution.
        // Use the transaction-scoped provider if available so uncommitted schemas are visible.
        {
            #[cfg(not(target_arch = "wasm32"))]
            let validation_provider: &dyn crate::fetcher::CollectionProvider =
                if let Some(ref p) = txn_provider {
                    p.as_ref()
                } else {
                    self.collection_provider.as_ref()
                };

            #[cfg(target_arch = "wasm32")]
            let validation_provider: &dyn crate::fetcher::CollectionProvider =
                self.collection_provider.as_ref();

            if let Err(e) = validate_parsed_operation(&parsed, validation_provider).await {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                };
            }
        }

        // NAC check uses the raw request identity (not default fallback).
        if let Some(denial) = check_nac(self, &request.identity, &parsed).await {
            return denial;
        }

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        // Route to appropriate handler based on operation type
        let execution = async {
            match parsed {
                ParsedOperation::Query {
                    mut selects,
                    explain,
                    exhaustive,
                } => {
                    if exhaustive {
                        for s in &mut selects {
                            s.exhaustive = true;
                        }
                    }
                    let fetcher = txn_ctx.doc_fetcher();
                    if let Some(explain_type) = explain {
                        self.explain_query_with_identity_and_vars(
                            &request.query,
                            identity,
                            explain_type,
                            variables.as_ref(),
                        )
                        .await
                    } else {
                        self.execute_selects_internal(selects, fetcher.as_ref(), identity)
                            .await
                    }
                }
                ParsedOperation::Mutation { explain, .. } => {
                    if let Some(explain_type) = explain {
                        // Return mutation plan instead of executing
                        self.explain_mutation_with_identity(&request.query, identity, explain_type)
                            .await
                    } else {
                        // Check if this is a read-only transaction
                        if txn_ctx.is_readonly() {
                            return Err(crate::error::QueryError::execution(
                                "cannot execute mutation in read-only transaction",
                            ));
                        }

                        // Get the transaction-scoped mutator
                        let mutator = match txn_ctx.doc_mutator() {
                            Some(m) => m,
                            None => {
                                return Err(crate::error::QueryError::execution(
                                    "mutations not supported in this transaction context",
                                ));
                            }
                        };

                        let txn_fetcher = txn_ctx.doc_fetcher();
                        self.execute_mutation_internal_with_vars(
                            &request.query,
                            mutator,
                            identity,
                            variables.as_ref(),
                            Some(txn_fetcher),
                        )
                        .await
                    }
                }
                ParsedOperation::Subscription { .. } => {
                    // Subscriptions require SSE transport
                    Err(crate::error::QueryError::parse(
                        "Subscriptions must be executed via Server-Sent Events (SSE). \
                         Send the request with Accept: text/event-stream header.",
                    ))
                }
                ParsedOperation::Introspection { query } => {
                    // Introspection queries are executed against the GraphQL schema
                    self.execute_introspection(&query).await
                }
            }
        };

        let deferred_acp_mutations = txn_ctx.deferred_acp_mutations();

        // If the transaction provides a collection provider or deferred ACP
        // projection, set them as task-local overrides so this execution sees
        // the transaction's uncommitted schema and ACP state.
        #[cfg(not(target_arch = "wasm32"))]
        let result = if let Some(provider) = txn_provider {
            if let Some(deferred_acp_mutations) = deferred_acp_mutations {
                super::TXN_COLLECTION_PROVIDER
                    .scope(
                        provider,
                        crate::txn::scope_deferred_acp_mutations(
                            deferred_acp_mutations,
                            await_with_timeout(execution, self.query_timeout),
                        ),
                    )
                    .await
            } else {
                super::TXN_COLLECTION_PROVIDER
                    .scope(provider, await_with_timeout(execution, self.query_timeout))
                    .await
            }
        } else if let Some(deferred_acp_mutations) = deferred_acp_mutations {
            crate::txn::scope_deferred_acp_mutations(
                deferred_acp_mutations,
                await_with_timeout(execution, self.query_timeout),
            )
            .await
        } else {
            await_with_timeout(execution, self.query_timeout).await
        };

        #[cfg(target_arch = "wasm32")]
        let result = await_with_timeout(execution, self.query_timeout).await;

        match result {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    txn_id = %handle,
                    error = %e,
                    "Query execution failed in transaction"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                }
            }
        }
    }

    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.registry.begin(readonly).await
    }

    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.commit(handle).await
    }

    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.rollback(handle).await
    }

    async fn schema(&self) -> Result<String> {
        let collections = self.collections_map().await?;
        let mut schema_str = String::new();
        for collection in collections.values() {
            schema_str.push_str(&format!("type {} {{\n", collection.name));
            for field in &collection.fields {
                let gql_type = field.kind.graphql_type_name();
                schema_str.push_str(&format!("  {}: {}\n", field.name, gql_type));
            }
            schema_str.push_str("}\n\n");
        }
        Ok(schema_str)
    }
}
