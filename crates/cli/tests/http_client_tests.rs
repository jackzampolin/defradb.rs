// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Tests for the HTTP client

use cli::commands::client::http_client::{GraphQLRequest, GraphQLResponse, GraphQLError, HttpClient};
use reqwest::StatusCode;

#[test]
fn test_http_client_new() {
    let client = HttpClient::new("http://localhost:9181/").unwrap();
    assert_eq!(client.base_url(), "http://localhost:9181");
}

#[test]
fn test_http_client_new_invalid_url() {
    let result = HttpClient::new("not-a-valid-url");
    assert!(result.is_err());
}

#[test]
fn test_graphql_request_serialization() {
    let request = GraphQLRequest {
        query: "{ Users { name } }".to_string(),
        variables: None,
        operation_name: None,
        txn_id: None,
    };
    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("query"));
    assert!(!json.contains("variables"));
}

#[test]
fn test_graphql_request_with_txn_id() {
    let request = GraphQLRequest {
        query: "{ Users { name } }".to_string(),
        variables: None,
        operation_name: None,
        txn_id: Some("12345".to_string()),
    };
    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("\"txn_id\":\"12345\""));
}

#[test]
fn test_graphql_response_has_errors() {
    let response = GraphQLResponse {
        data: None,
        errors: vec![GraphQLError {
            message: "error".to_string(),
        }],
    };
    assert!(response.has_errors());
    assert_eq!(response.error_message(), "error");
}

#[test]
fn test_graphql_response_no_errors() {
    let response = GraphQLResponse {
        data: Some(serde_json::json!({})),
        errors: vec![],
    };
    assert!(!response.has_errors());
}

#[test]
fn test_is_retryable_status_service_unavailable() {
    assert!(HttpClient::is_retryable_status(
        StatusCode::SERVICE_UNAVAILABLE
    ));
}

#[test]
fn test_is_retryable_status_too_many_requests() {
    assert!(HttpClient::is_retryable_status(
        StatusCode::TOO_MANY_REQUESTS
    ));
}

#[test]
fn test_is_retryable_status_internal_server_error() {
    assert!(HttpClient::is_retryable_status(
        StatusCode::INTERNAL_SERVER_ERROR
    ));
}

#[test]
fn test_is_retryable_status_bad_gateway() {
    assert!(HttpClient::is_retryable_status(StatusCode::BAD_GATEWAY));
}

#[test]
fn test_is_retryable_status_gateway_timeout() {
    assert!(HttpClient::is_retryable_status(StatusCode::GATEWAY_TIMEOUT));
}

#[test]
fn test_is_retryable_status_request_timeout() {
    assert!(HttpClient::is_retryable_status(StatusCode::REQUEST_TIMEOUT));
}

#[test]
fn test_is_not_retryable_bad_request() {
    assert!(!HttpClient::is_retryable_status(StatusCode::BAD_REQUEST));
}

#[test]
fn test_is_not_retryable_not_found() {
    assert!(!HttpClient::is_retryable_status(StatusCode::NOT_FOUND));
}

#[test]
fn test_is_not_retryable_unauthorized() {
    assert!(!HttpClient::is_retryable_status(StatusCode::UNAUTHORIZED));
}

#[test]
fn test_is_not_retryable_forbidden() {
    assert!(!HttpClient::is_retryable_status(StatusCode::FORBIDDEN));
}

#[test]
fn test_is_not_retryable_ok() {
    assert!(!HttpClient::is_retryable_status(StatusCode::OK));
}

#[test]
fn test_retry_constants() {
    // Verify retry configuration values are sensible
    assert_eq!(cli::commands::client::http_client::MAX_RETRIES, 3);
    assert_eq!(cli::commands::client::http_client::INITIAL_BACKOFF_MS, 100);
    assert!(cli::commands::client::http_client::RETRYABLE_STATUS_CODES.contains(&503));
    assert!(cli::commands::client::http_client::RETRYABLE_STATUS_CODES.contains(&429));
    assert!(cli::commands::client::http_client::RETRYABLE_STATUS_CODES.contains(&500));
}
