//! Server integration tests.

use std::net::SocketAddr;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use tower::util::ServiceExt;

use defra_http::mock::{FailingMockExecutor, MockQueryExecutor, MockRestOperations};
use defra_http::{Server, ServerConfig};

fn test_server() -> Server {
    Server::new(MockQueryExecutor::new())
}

#[tokio::test]
async fn test_health_check_route() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/health-check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_version_route() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_post_route() {
    let router = test_server().router().unwrap();
    let body = json!({"query": "{ users { name } }"}).to_string();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_post_invalid_json() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header("content-type", "application/json")
                .body(Body::from("{invalid json}"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Axum returns 400 Bad Request for JSON parse errors
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_graphql_get_route() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/graphql?query=%7B%20users%20%7B%20name%20%7D%20%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_get_with_variables() {
    let router = test_server().router().unwrap();
    let vars = urlencoding::encode(r#"{"limit":10}"#);

    let response = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v0/graphql?query=%7B%20users%20%7D&variables={}",
                    vars
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_get_invalid_variables() {
    let router = test_server().router().unwrap();
    let invalid_vars = urlencoding::encode("{invalid}");

    let response = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v0/graphql?query=%7B%20users%20%7D&variables={}",
                    invalid_vars
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should still return 200 but with error in body
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_schema_route() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_not_found_route() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_server_config_default() {
    let config = ServerConfig::default();
    assert_eq!(config.address.port(), 9181);
    assert!(config.allowed_origins.is_empty());
}

#[tokio::test]
async fn test_graphql_post_empty_query() {
    let router = test_server().router().unwrap();
    let body = json!({"query": ""}).to_string();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    // Empty query should still be accepted (executor handles validation)
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_get_missing_query_param() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/graphql")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Missing required 'query' param returns 400
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_cors_with_allowed_origin() {
    let config = ServerConfig {
        address: SocketAddr::from(([127, 0, 0, 1], 0)),
        allowed_origins: vec!["http://localhost:3000".to_string()],
    };
    let server = Server::with_config(MockQueryExecutor::new(), config);
    let router = server.router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/v0/graphql")
                .header("Origin", "http://localhost:3000")
                .header("Access-Control-Request-Method", "POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Preflight should succeed with CORS headers
    assert!(response
        .headers()
        .contains_key("access-control-allow-origin"));
    assert!(response
        .headers()
        .contains_key("access-control-allow-methods"));
}

#[tokio::test]
async fn test_cors_wildcard() {
    let config = ServerConfig {
        address: SocketAddr::from(([127, 0, 0, 1], 0)),
        allowed_origins: vec!["*".to_string()],
    };
    let server = Server::with_config(MockQueryExecutor::new(), config);
    let router = server.router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/v0/graphql")
                .header("Origin", "http://any-origin.com")
                .header("Access-Control-Request-Method", "POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Wildcard allows any origin
    assert!(response
        .headers()
        .contains_key("access-control-allow-origin"));
}

#[tokio::test]
async fn test_cors_case_insensitive() {
    let config = ServerConfig {
        address: SocketAddr::from(([127, 0, 0, 1], 0)),
        allowed_origins: vec!["http://LOCALHOST:3000".to_string()],
    };
    let server = Server::with_config(MockQueryExecutor::new(), config);
    let router = server.router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/v0/graphql")
                .header("Origin", "http://localhost:3000")
                .header("Access-Control-Request-Method", "POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Case-insensitive matching (matches Go DefraDB)
    assert!(response
        .headers()
        .contains_key("access-control-allow-origin"));
}

#[tokio::test]
async fn test_cors_invalid_origin_fails_fast() {
    let config = ServerConfig {
        address: SocketAddr::from(([127, 0, 0, 1], 0)),
        // Non-ASCII characters are invalid in HTTP header values
        allowed_origins: vec![
            "http://localhost:3000".to_string(),
            "http://invalid\x00origin".to_string(),
        ],
    };
    let server = Server::with_config(MockQueryExecutor::new(), config);

    // router() should return an error for invalid origins
    let result = server.router();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("invalid CORS origins"));
}

#[tokio::test]
async fn test_graphql_post_returns_errors_in_body() {
    let server = Server::new(FailingMockExecutor::with_schema_error("ignored"));
    let router = server.router().unwrap();
    let body = json!({"query": "{ users { name } }"}).to_string();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    // GraphQL spec: errors return 200 OK with errors in body
    assert_eq!(response.status(), StatusCode::OK);

    // Verify response body contains errors
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("errors"),
        "Response should contain errors: {}",
        body_str
    );
}

#[tokio::test]
async fn test_graphql_post_response_body_structure() {
    let router = test_server().router().unwrap();
    let body = json!({"query": "{ users { name } }"}).to_string();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify response body has correct structure
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(
        json.get("data").is_some(),
        "Response should contain 'data' field"
    );
    assert!(
        json.get("data").unwrap().get("users").is_some(),
        "Response should contain 'users' in data"
    );
}

#[tokio::test]
async fn test_version_response_body() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify version response has required fields
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(
        json.get("version").is_some(),
        "Response should contain 'version' field"
    );
    assert!(
        json.get("commit").is_some(),
        "Response should contain 'commit' field"
    );
}

#[tokio::test]
async fn test_schema_response_body() {
    let router = test_server().router().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify schema response contains SDL content
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("type User"),
        "Schema should contain User type"
    );
    assert!(
        body_str.contains("type Query"),
        "Schema should contain Query type"
    );
}

#[tokio::test]
async fn test_server_with_rest_operations() {
    let server = Server::new(MockQueryExecutor::new()).with_rest(MockRestOperations::new());
    let router = server.router().unwrap();

    // Test that REST endpoints work when REST operations are configured
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify response body
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(
        json.get("collections").is_some(),
        "Response should contain 'collections' field"
    );
}

#[tokio::test]
async fn test_server_without_rest_returns_error_for_collections() {
    let server = Server::new(MockQueryExecutor::new());
    let router = server.router().unwrap();

    // Without REST operations, collections endpoint should return 500
    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
