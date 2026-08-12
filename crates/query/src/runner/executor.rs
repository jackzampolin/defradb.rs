//! QueryExecutor trait implementation for QueryRunner.

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::future::Future;
use tracing::{instrument, warn};

use acp::nac::NodePermission;
use identity::Did;

use crate::error::{Result, TransactionError};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::query_parse::{parse_request_with_limits, validate_parsed_operation, ParsedOperation};
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
    let did = match identity {
        Some(d) => d.clone(),
        None => Did::wildcard(),
    };
    let permission = permission_for_operation(parsed);
    if !runner.nac.check_permission(&did, permission).await {
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
        let parsed = match parse_request_with_limits(
            &request.query,
            variables.as_ref(),
            request.operation_name.as_deref(),
            self.query_limits,
        ) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                        extensions: None,
                    }],
                };
            }
        };

        if matches!(&parsed, ParsedOperation::Query { .. }) {
            match self.registry.begin_implicit_read().await {
                Ok(handle) => {
                    let response = self.execute_in_txn(request.clone(), &handle).await;
                    let apply_read_effects = response.errors.is_empty();
                    if let Err(error) = self
                        .registry
                        .finish_implicit_read(&handle, apply_read_effects)
                        .await
                    {
                        if apply_read_effects {
                            return QueryResponse::error(format!(
                                "failed to finalize implicit read transaction: {}",
                                error
                            ));
                        }
                        warn!(
                            txn_id = %handle,
                            error = %error,
                            "Failed to close implicit read-only transaction after query execution"
                        );
                    }
                    return response;
                }
                Err(TransactionError::NotSupported(_)) => {}
                Err(error) => {
                    return QueryResponse::error(format!(
                        "failed to create implicit read-only transaction: {}",
                        error
                    ));
                }
            }
        }

        // Validate that all referenced collections exist before execution
        if let Err(e) = validate_parsed_operation(&parsed, self.effective_provider().as_ref()).await
        {
            return QueryResponse {
                data: None,
                errors: vec![QueryResponseError {
                    message: e.to_string(),
                    path: None,
                    locations: None,
                    extensions: None,
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
                    errors: vec![QueryResponseError::from_query_error(e)],
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
        // Look up the transaction in the registry. `GetTransactionResult` is
        // `#[non_exhaustive]` (defined in the `query-plan` crate), so matches
        // from this crate require a wildcard arm.
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
            _ => {
                return QueryResponse::error(format!(
                    "unknown transaction registry result for '{}'",
                    handle
                ));
            }
        };
        let action_lock = txn_ctx.action_lock();
        let _action_guard = match action_lock.as_ref() {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };

        // Convert variables from JSON to HashMap format for the parser
        let variables = convert_variables(&request.variables);

        // Parse the request to determine if it's a query or mutation
        let parsed = match parse_request_with_limits(
            &request.query,
            variables.as_ref(),
            request.operation_name.as_deref(),
            self.query_limits,
        ) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                        extensions: None,
                    }],
                };
            }
        };

        let txn_provider = txn_ctx.collection_provider();

        // Validate that all referenced collections exist before execution.
        // Use the transaction-scoped provider if available so uncommitted schemas are visible.
        {
            let validation_provider: &dyn crate::fetcher::CollectionProvider =
                if let Some(ref p) = txn_provider {
                    p.as_ref()
                } else {
                    self.collection_provider.as_ref()
                };

            if let Err(e) = validate_parsed_operation(&parsed, validation_provider).await {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                        extensions: None,
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

        // Scope both transaction overlays around execution so schema resolution
        // and ACP checks see uncommitted state.
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
                    errors: vec![QueryResponseError::from_query_error(e)],
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

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use document::Document;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fetcher::{DocFetcher, FetchByIdsResult};
    use crate::test_utils::{MockFetcher, MockTxnRegistry};
    use crate::txn::{GetTransactionResult, TransactionContext, TransactionRegistry};

    struct ChangingFetcher {
        call_count: std::sync::atomic::AtomicUsize,
    }

    impl ChangingFetcher {
        fn new() -> Self {
            Self {
                call_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn make_doc(&self, name: String) -> Document {
            let mut doc = Document::new();
            doc.set_id(document::DocID::new_v0_from_seed(&name));
            doc.set("name", serde_json::Value::String(name));
            doc
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for ChangingFetcher {
        /// A mock has no storage, so a document's short id is its 1-based position
        /// in the collection, mirroring how the real allocator hands them out.
        async fn stream_by_doc_short_ids(
            &self,
            collection_name: &str,
            doc_short_ids: &[u64],
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            let all = self
                .get_all_with_deleted(collection_name, show_deleted)
                .await?;
            let picked = doc_short_ids
                .iter()
                .filter_map(|id| all.get(id.checked_sub(1)? as usize).cloned())
                .collect();
            Ok(Box::new(crate::doc_stream::VecStream::new(picked)))
        }
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            let call = self
                .call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            Ok(vec![self.make_doc(format!("auto-{call}"))])
        }

        /// In-memory mock: there is no storage to stream from.
        async fn stream_all_with_deleted(
            &self,
            collection_name: &str,
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            Ok(Box::new(crate::doc_stream::VecStream::new(
                self.get_all_with_deleted(collection_name, show_deleted)
                    .await?,
            )))
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.get_all(collection_name).await?;
            Ok(FetchByIdsResult::all_found(docs))
        }

        async fn get_by_field_value(
            &self,
            collection_name: &str,
            _field_name: &str,
            _value: &str,
        ) -> Result<Vec<Document>> {
            self.get_all(collection_name).await
        }
    }

    fn make_users_doc(name: &str) -> Document {
        let mut doc = Document::new();
        doc.set_id(document::DocID::new_v0_from_seed(name));
        doc.set("name", serde_json::Value::String(name.to_string()));
        doc
    }

    struct SerialOnlyFetcher {
        in_flight: AtomicUsize,
    }

    impl SerialOnlyFetcher {
        fn new() -> Self {
            Self {
                in_flight: AtomicUsize::new(0),
            }
        }
    }

    struct InFlightGuard<'a>(&'a AtomicUsize);

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for SerialOnlyFetcher {
        /// A mock has no storage, so a document's short id is its 1-based position
        /// in the collection, mirroring how the real allocator hands them out.
        async fn stream_by_doc_short_ids(
            &self,
            collection_name: &str,
            doc_short_ids: &[u64],
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            let all = self
                .get_all_with_deleted(collection_name, show_deleted)
                .await?;
            let picked = doc_short_ids
                .iter()
                .filter_map(|id| all.get(id.checked_sub(1)? as usize).cloned())
                .collect();
            Ok(Box::new(crate::doc_stream::VecStream::new(picked)))
        }
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = InFlightGuard(&self.in_flight);
            if active > 1 {
                return Err(crate::error::QueryError::execution(
                    "transaction action overlapped",
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(vec![make_users_doc("serial")])
        }

        /// In-memory mock: there is no storage to stream from.
        async fn stream_all_with_deleted(
            &self,
            collection_name: &str,
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            Ok(Box::new(crate::doc_stream::VecStream::new(
                self.get_all_with_deleted(collection_name, show_deleted)
                    .await?,
            )))
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.get_all(collection_name).await?;
            Ok(FetchByIdsResult::all_found(docs))
        }

        async fn get_by_field_value(
            &self,
            collection_name: &str,
            _field_name: &str,
            _value: &str,
        ) -> Result<Vec<Document>> {
            self.get_all(collection_name).await
        }
    }

    struct SerialTxnContext {
        id: String,
        fetcher: Arc<dyn DocFetcher>,
        action_lock: Arc<async_lock::Mutex<()>>,
    }

    impl TransactionContext for SerialTxnContext {
        fn id(&self) -> &str {
            &self.id
        }

        fn is_readonly(&self) -> bool {
            true
        }

        fn doc_fetcher(&self) -> Arc<dyn DocFetcher> {
            self.fetcher.clone()
        }

        fn action_lock(&self) -> Option<Arc<async_lock::Mutex<()>>> {
            Some(self.action_lock.clone())
        }
    }

    struct SerialTxnRegistry {
        ctx: Arc<SerialTxnContext>,
    }

    impl SerialTxnRegistry {
        fn new(fetcher: SerialOnlyFetcher) -> Self {
            Self {
                ctx: Arc::new(SerialTxnContext {
                    id: "serial-txn".to_string(),
                    fetcher: Arc::new(fetcher),
                    action_lock: Arc::new(async_lock::Mutex::new(())),
                }),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl TransactionRegistry for SerialTxnRegistry {
        async fn begin(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            Ok(TransactionHandle::new(self.ctx.id.clone()))
        }

        fn get(&self, handle: &TransactionHandle) -> GetTransactionResult {
            if handle.as_str() == self.ctx.id {
                GetTransactionResult::Found(self.ctx.clone())
            } else {
                GetTransactionResult::NotFound
            }
        }

        async fn commit(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Ok(())
        }

        async fn rollback(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn execute_in_txn_serializes_concurrent_actions_on_same_handle() {
        let collections = crate::parse_sdl("type Users { name: String }").expect("schema");
        let txn_fetcher = SerialOnlyFetcher::new();

        let runner = QueryRunner::with_registry(
            MockFetcher::new(),
            collections,
            SerialTxnRegistry::new(txn_fetcher),
        );
        let handle = runner.begin_txn(true).await.expect("begin txn");

        let (first, second) = tokio::join!(
            runner.execute_in_txn(QueryRequest::new("query { Users { name } }"), &handle),
            runner.execute_in_txn(QueryRequest::new("query { Users { name } }"), &handle)
        );

        assert!(
            !first.has_errors(),
            "first transaction action failed: {:?}",
            first.errors
        );
        assert!(
            !second.has_errors(),
            "second transaction action failed: {:?}",
            second.errors
        );
    }

    #[tokio::test]
    async fn execute_uses_implicit_read_txn_when_registry_is_available() {
        let collections = crate::parse_sdl("type Users { name: String }").expect("schema");

        let txn_fetcher = MockFetcher::new();
        txn_fetcher.add_doc("Users", make_users_doc("txn-snapshot"));

        let runner = QueryRunner::with_registry(
            ChangingFetcher::new(),
            collections,
            MockTxnRegistry::new(txn_fetcher),
        );

        let response = runner
            .execute(QueryRequest::new(
                "{ first: Users { name } second: Users { name } }",
            ))
            .await;

        assert!(
            !response.has_errors(),
            "unexpected errors: {:?}",
            response.errors
        );
        assert_eq!(
            response.data,
            Some(json!({
                "first": [{"name": "txn-snapshot"}],
                "second": [{"name": "txn-snapshot"}],
            }))
        );
    }
}
