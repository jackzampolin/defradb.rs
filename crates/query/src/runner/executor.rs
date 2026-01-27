//! QueryExecutor trait implementation for QueryRunner.

use async_trait::async_trait;

use crate::error::{Result, TransactionError};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::query_parse::{parse_request, ParsedOperation};
use crate::txn::{GetTransactionResult, TransactionHandle, TransactionRegistry};

use super::{DocFetcher, QueryRunner};

#[async_trait]
impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryExecutor for QueryRunner<F, R> {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        // First, parse the request to determine if it's a query or mutation
        let parsed = match parse_request(&request.query) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: format!("parse error: {}", e),
                        path: None,
                        locations: None,
                    }],
                };
            }
        };

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        // Route to appropriate handler based on operation type
        // Pass identity through for ACP permission checks
        let result = match parsed {
            ParsedOperation::Query { explain, .. } => {
                if let Some(explain_type) = explain {
                    // Return query plan instead of executing
                    self.explain_query_with_identity(&request.query, identity, explain_type)
                        .await
                } else {
                    self.execute_query_with_identity(&request.query, identity)
                        .await
                }
            }
            ParsedOperation::Mutation(_) => {
                self.execute_mutation_with_identity(&request.query, identity)
                    .await
            }
            ParsedOperation::Subscription { .. } => {
                // Subscriptions require SSE transport - they cannot be executed via regular request/response
                Err(crate::error::QueryError::parse(
                    "Subscriptions must be executed via Server-Sent Events (SSE). \
                     Send the request with Accept: text/event-stream header.",
                ))
            }
        };

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

        // Parse the request to determine if it's a query or mutation
        let parsed = match parse_request(&request.query) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: format!("parse error: {}", e),
                        path: None,
                        locations: None,
                    }],
                };
            }
        };

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        // Route to appropriate handler based on operation type
        let result = match parsed {
            ParsedOperation::Query { explain, .. } => {
                // Get the transaction-scoped fetcher and execute with identity for ACP
                let fetcher = txn_ctx.doc_fetcher();
                if let Some(explain_type) = explain {
                    // Return query plan instead of executing
                    self.explain_query_with_identity(&request.query, identity, explain_type)
                        .await
                } else {
                    self.execute_query_internal(&request.query, fetcher.as_ref(), identity)
                        .await
                }
            }
            ParsedOperation::Mutation(_) => {
                // Check if this is a read-only transaction
                if txn_ctx.is_readonly() {
                    return QueryResponse::error(
                        "cannot execute mutation in read-only transaction".to_string(),
                    );
                }

                // Get the transaction-scoped mutator
                let mutator = match txn_ctx.doc_mutator() {
                    Some(m) => m,
                    None => {
                        return QueryResponse::error(
                            "mutations not supported in this transaction context".to_string(),
                        );
                    }
                };

                self.execute_mutation_internal(&request.query, mutator, identity)
                    .await
            }
            ParsedOperation::Subscription { .. } => {
                // Subscriptions require SSE transport
                Err(crate::error::QueryError::parse(
                    "Subscriptions must be executed via Server-Sent Events (SSE). \
                     Send the request with Accept: text/event-stream header.",
                ))
            }
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
