//! Query execution trait for HTTP/API layer integration.
//!
//! This module defines the interface between the HTTP layer and the query execution engine.
//! The HTTP crate depends on this trait, allowing parallel development of HTTP and query execution.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::{Result, TransactionError};
use crate::txn::TransactionHandle;

/// A GraphQL query request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The GraphQL query string.
    pub query: String,

    /// Optional operation name (for multi-operation documents).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,

    /// Optional variables for the query.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<JsonValue>,
}

impl QueryRequest {
    /// Create a new query request with just a query string.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            operation_name: None,
            variables: None,
        }
    }

    /// Set the operation name.
    pub fn with_operation_name(mut self, name: impl Into<String>) -> Self {
        self.operation_name = Some(name.into());
        self
    }

    /// Set variables.
    pub fn with_variables(mut self, vars: JsonValue) -> Self {
        self.variables = Some(vars);
        self
    }
}

/// A GraphQL query response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Query result data (null if errors occurred).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,

    /// Errors that occurred during execution.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub errors: Vec<QueryResponseError>,
}

impl QueryResponse {
    /// Create a successful response with data.
    pub fn success(data: JsonValue) -> Self {
        Self {
            data: Some(data),
            errors: Vec::new(),
        }
    }

    /// Create an error response.
    pub fn error(err: impl Into<QueryResponseError>) -> Self {
        Self {
            data: None,
            errors: vec![err.into()],
        }
    }

    /// Create a response with both data and errors (partial success).
    pub fn partial(data: JsonValue, errors: Vec<QueryResponseError>) -> Self {
        Self {
            data: Some(data),
            errors,
        }
    }

    /// Check if the response contains errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// A GraphQL error in the response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponseError {
    /// Error message.
    pub message: String,

    /// Optional path to the field that caused the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<String>>,

    /// Optional locations in the query where the error occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<ErrorLocation>>,
}

impl QueryResponseError {
    /// Create a new error with just a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            path: None,
            locations: None,
        }
    }

    /// Set the path.
    pub fn with_path(mut self, path: Vec<String>) -> Self {
        self.path = Some(path);
        self
    }

    /// Set locations.
    pub fn with_locations(mut self, locations: Vec<ErrorLocation>) -> Self {
        self.locations = Some(locations);
        self
    }
}

impl<S: Into<String>> From<S> for QueryResponseError {
    fn from(msg: S) -> Self {
        Self::new(msg)
    }
}

/// Location in the query document where an error occurred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorLocation {
    pub line: u32,
    pub column: u32,
}

/// Query executor trait.
///
/// This is the main interface between HTTP/API layer and query execution.
/// Implementors handle parsing, planning, and executing GraphQL queries.
///
/// # Transaction Support
///
/// The executor supports executing queries within transaction contexts:
///
/// 1. Call `begin_txn()` to start a new transaction
/// 2. Execute queries with `execute_in_txn()` using the returned transaction ID
/// 3. Call `commit_txn()` or `rollback_txn()` to end the transaction
///
/// # Example
///
/// ```ignore
/// use query::{QueryExecutor, QueryRequest, QueryResponse};
///
/// async fn handle_graphql<E: QueryExecutor>(
///     executor: &E,
///     request: QueryRequest,
/// ) -> QueryResponse {
///     executor.execute(request).await
/// }
///
/// async fn handle_transaction<E: QueryExecutor>(
///     executor: &E,
///     queries: Vec<QueryRequest>,
/// ) -> Result<Vec<QueryResponse>, TransactionError> {
///     let handle = executor.begin_txn(false).await?;
///     let mut responses = Vec::new();
///
///     for query in queries {
///         let resp = executor.execute_in_txn(query, &handle).await;
///         if resp.has_errors() {
///             executor.rollback_txn(&handle).await.ok();
///             return Err(TransactionError::execution("query failed"));
///         }
///         responses.push(resp);
///     }
///
///     executor.commit_txn(&handle).await?;
///     Ok(responses)
/// }
/// ```
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute a GraphQL query and return the response.
    ///
    /// This handles the full pipeline: parsing → planning → execution → response.
    /// Each query runs in its own implicit transaction that is automatically
    /// committed on success.
    async fn execute(&self, request: QueryRequest) -> QueryResponse;

    /// Execute a query within an existing transaction context.
    ///
    /// This allows batching multiple operations in a single transaction.
    /// The transaction must have been created with `begin_txn()`.
    ///
    /// Returns an error response if the transaction handle is invalid or
    /// the transaction has already been committed/rolled back.
    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse;

    /// Begin a new transaction.
    ///
    /// Returns a transaction handle that can be used with `execute_in_txn()`.
    /// The transaction remains active until `commit_txn()` or `rollback_txn()` is called.
    ///
    /// # Arguments
    /// * `readonly` - If true, the transaction cannot perform write operations
    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError>;

    /// Commit a transaction.
    ///
    /// All operations performed within the transaction become permanent.
    /// After commit, the transaction handle is no longer valid.
    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError>;

    /// Rollback a transaction.
    ///
    /// All operations performed within the transaction are discarded.
    /// After rollback, the transaction handle is no longer valid.
    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError>;

    /// Get the GraphQL schema for introspection.
    async fn schema(&self) -> Result<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_query_request_builder() {
        let req = QueryRequest::new("{ users { name } }")
            .with_operation_name("GetUsers")
            .with_variables(json!({"limit": 10}));

        assert_eq!(req.query, "{ users { name } }");
        assert_eq!(req.operation_name, Some("GetUsers".to_string()));
        assert_eq!(req.variables, Some(json!({"limit": 10})));
    }

    #[test]
    fn test_query_response_success() {
        let resp = QueryResponse::success(json!({"users": []}));
        assert!(!resp.has_errors());
        assert!(resp.data.is_some());
    }

    #[test]
    fn test_query_response_error() {
        let resp = QueryResponse::error("something went wrong");
        assert!(resp.has_errors());
        assert!(resp.data.is_none());
        assert_eq!(resp.errors[0].message, "something went wrong");
    }

    #[test]
    fn test_query_response_partial() {
        let resp = QueryResponse::partial(
            json!({"users": []}),
            vec![QueryResponseError::new("warning")],
        );
        assert!(resp.has_errors());
        assert!(resp.data.is_some());
    }

    #[test]
    fn test_request_serialization() {
        let req = QueryRequest::new("{ users { name } }");
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("users"));

        let parsed: QueryRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.query, req.query);
    }

    #[test]
    fn test_response_serialization() {
        let resp = QueryResponse::success(json!({"data": "test"}));
        let json = serde_json::to_string(&resp).unwrap();

        // errors should be omitted when empty
        assert!(!json.contains("errors"));
    }
}
