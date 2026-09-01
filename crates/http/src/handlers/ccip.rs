//! Chainlink CCIP-read transport for GraphQL queries.

use axum::{
    body::Bytes,
    extract::{Path, State},
    Json,
};
use query::executor::{QueryRequest, QueryResponse};
use query::subscription::is_subscription_operation_with_limits;
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::handlers::graphql::{check_encrypted_fields, graphql_required_permission};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::query_context::execute_with_context;
use crate::router::AppState;

#[derive(Debug, Deserialize)]
struct CcipRequest {
    #[serde(rename = "sender")]
    _sender: String,
    data: String,
}

#[derive(Debug, Serialize)]
pub struct CcipResponse {
    pub data: String,
}

#[derive(Debug, Deserialize)]
struct GraphqlRequest {
    query: String,
    #[serde(rename = "operationName", default)]
    operation_name: Option<String>,
    #[serde(default)]
    variables: Option<serde_json::Value>,
}

pub async fn get(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((_sender, data)): Path<(String, String)>,
) -> Result<Json<CcipResponse>, HttpError> {
    execute(state, identity, &data).await
}

pub async fn post(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: Bytes,
) -> Result<Json<CcipResponse>, HttpError> {
    let request: CcipRequest =
        serde_json::from_slice(&body).map_err(|error| HttpError::BadRequest(error.to_string()))?;
    execute(state, identity, &request.data).await
}

async fn execute(
    state: AppState,
    identity: ExtractIdentity,
    data: &str,
) -> Result<Json<CcipResponse>, HttpError> {
    let bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data))
        .map_err(|error| HttpError::BadRequest(error.to_string()))?;
    let request: GraphqlRequest =
        serde_json::from_slice(&bytes).map_err(|error| HttpError::BadRequest(error.to_string()))?;

    if is_subscription_operation_with_limits(
        &request.query,
        request.variables.as_ref(),
        request.operation_name.as_deref(),
        state.query_limits,
    ) {
        return Err(HttpError::BadRequest("streaming not supported".into()));
    }

    check_encrypted_fields(&state, &request.query)?;
    let permission = graphql_required_permission(&request.query, state.query_limits);
    require_permission(&state, &identity, permission).await?;

    let response = execute_with_context(
        &state,
        &identity,
        QueryRequest {
            query: request.query,
            operation_name: request.operation_name,
            variables: request.variables,
            identity: identity.did().cloned(),
        },
    )
    .await;
    encode_response(response)
}

fn encode_response(response: QueryResponse) -> Result<Json<CcipResponse>, HttpError> {
    let bytes =
        serde_json::to_vec(&response).map_err(|error| HttpError::Internal(error.to_string()))?;
    Ok(Json(CcipResponse {
        data: format!("0x{}", hex::encode(bytes)),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::mock::MockQueryExecutor;
    use crate::router::create_router;

    fn encoded_query(query: &str) -> String {
        hex::encode(json!({"query": query}).to_string())
    }

    async fn response(method: Method, uri: &str, body: Body) -> axum::response::Response {
        create_router(Arc::new(MockQueryExecutor::new()))
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(body)
                    .expect("request should build"),
            )
            .await
            .expect("router should respond")
    }

    async fn assert_graphql_response(response: axum::response::Response) {
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let ccip: Value = serde_json::from_slice(&body).expect("CCIP response");
        let encoded = ccip["data"].as_str().expect("encoded GraphQL response");
        let decoded = hex::decode(encoded.trim_start_matches("0x")).expect("hex response");
        let graphql: Value = serde_json::from_slice(&decoded).expect("GraphQL response");
        assert_eq!(graphql["data"]["users"][0]["name"], "Alice");
    }

    #[tokio::test]
    async fn get_executes_hex_encoded_graphql() {
        let response = response(
            Method::GET,
            &format!(
                "/api/v0/ccip/0x1234/{}",
                encoded_query("query { Users { name } }")
            ),
            Body::empty(),
        )
        .await;

        assert_graphql_response(response).await;
    }

    #[tokio::test]
    async fn post_executes_hex_encoded_graphql() {
        let body = json!({
            "sender": "0x1234",
            "data": format!("0x{}", encoded_query("query { Users { name } }"))
        });
        let response = response(Method::POST, "/api/v0/ccip", Body::from(body.to_string())).await;

        assert_graphql_response(response).await;
    }

    #[tokio::test]
    async fn malformed_hex_is_a_bad_request() {
        let body = json!({"sender": "0x1234", "data": "0xnot-hex"});
        let response = response(Method::POST, "/api/v0/ccip", Body::from(body.to_string())).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn malformed_json_is_a_bad_request() {
        let response = response(Method::POST, "/api/v0/ccip", Body::from("{")).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn subscriptions_are_rejected() {
        let body = json!({
            "sender": "0x1234",
            "data": encoded_query("subscription { Users { name } }")
        });
        let response = response(Method::POST, "/api/v0/ccip", Body::from(body.to_string())).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
