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
#[derive(Debug, Serialize)]
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
#[derive(Debug, Deserialize)]
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
            tracing::error!(error = %e, "Schema retrieval failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(crate::error::ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response()
        }
    }
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
}
