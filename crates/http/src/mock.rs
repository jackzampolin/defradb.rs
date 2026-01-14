//! Mock QueryExecutor for testing HTTP routes.

use async_trait::async_trait;
use serde_json::json;

use query::error::{QueryError, Result};
use query::executor::{QueryExecutor, QueryRequest, QueryResponse};

/// Mock executor for testing HTTP routes with pattern-matched query responses.
#[derive(Debug, Clone)]
pub struct MockQueryExecutor {
    schema_sdl: String,
}

/// Mock executor for testing error paths. Schema errors are optional.
#[derive(Debug, Clone)]
pub struct FailingMockExecutor {
    schema_error: Option<String>,
}

impl FailingMockExecutor {
    /// Create a mock that fails schema() calls.
    pub fn with_schema_error(msg: impl Into<String>) -> Self {
        Self {
            schema_error: Some(msg.into()),
        }
    }
}

#[async_trait]
impl QueryExecutor for FailingMockExecutor {
    async fn execute(&self, _request: QueryRequest) -> QueryResponse {
        QueryResponse::error("execution failed")
    }

    async fn execute_in_txn(&self, request: QueryRequest, _txn_id: &str) -> QueryResponse {
        self.execute(request).await
    }

    async fn schema(&self) -> Result<String> {
        match &self.schema_error {
            Some(msg) => Err(QueryError::internal(msg.clone())),
            None => Ok("type Query { ping: String }".to_string()),
        }
    }
}

impl MockQueryExecutor {
    /// Create a mock executor with default schema.
    pub fn new() -> Self {
        Self {
            schema_sdl: default_mock_schema(),
        }
    }

    /// Create with custom schema.
    pub fn with_schema(schema: impl Into<String>) -> Self {
        Self {
            schema_sdl: schema.into(),
        }
    }
}

impl Default for MockQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueryExecutor for MockQueryExecutor {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        let query = request.query.to_lowercase();

        if query.contains("users") {
            QueryResponse::success(json!({
                "users": [
                    {"_docID": "bae-123", "name": "Alice", "age": 30},
                    {"_docID": "bae-456", "name": "Bob", "age": 25}
                ]
            }))
        } else if query.contains("__schema") || query.contains("__type") {
            QueryResponse::success(json!({
                "__schema": {
                    "types": [],
                    "queryType": {"name": "Query"},
                    "mutationType": {"name": "Mutation"}
                }
            }))
        } else if query.contains("create") || query.contains("update") || query.contains("delete") {
            QueryResponse::success(json!({
                "_docID": "bae-new-789"
            }))
        } else {
            QueryResponse::success(json!({
                "data": null,
                "message": "Mock response"
            }))
        }
    }

    async fn execute_in_txn(&self, request: QueryRequest, _txn_id: &str) -> QueryResponse {
        self.execute(request).await
    }

    async fn schema(&self) -> Result<String> {
        Ok(self.schema_sdl.clone())
    }
}

fn default_mock_schema() -> String {
    r#"
type User {
    _docID: ID!
    name: String!
    age: Int
    email: String
}

type Query {
    users: [User!]!
    user(_docID: ID!): User
}

type Mutation {
    create_User(input: UserInput!): User!
    update_User(_docID: ID!, input: UserInput!): User!
    delete_User(_docID: ID!): User!
}

input UserInput {
    name: String!
    age: Int
    email: String
}
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_users_query() {
        let executor = MockQueryExecutor::new();
        let request = QueryRequest::new("{ users { name } }");

        let response = executor.execute(request).await;
        assert!(!response.has_errors());

        let data = response.data.unwrap();
        let users = data.get("users").unwrap();
        assert!(users.is_array());
    }

    #[tokio::test]
    async fn test_mock_schema() {
        let executor = MockQueryExecutor::new();
        let schema = executor.schema().await.unwrap();
        assert!(schema.contains("type User"));
        assert!(schema.contains("type Query"));
    }

    #[tokio::test]
    async fn test_mock_introspection() {
        let executor = MockQueryExecutor::new();
        let request = QueryRequest::new("{ __schema { types { name } } }");

        let response = executor.execute(request).await;
        assert!(!response.has_errors());
    }
}
