//! Identity extractor tests.

use std::time::Duration;

use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, header::HOST, Request},
};
use identity::{new_token, Did, Identity, RawIdentity};

use defra_http::{ExtractIdentity, ExtractTokenIdentity, IdentityExtractionError};

/// Test host used for audience validation
const TEST_HOST: &str = "localhost:9181";

fn create_test_token() -> (String, Did) {
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let did = identity.did().unwrap();

    // Create token with audience matching TEST_HOST (lowercased as per Go behavior)
    let token = new_token(
        &identity,
        Duration::from_secs(3600),
        Some(TEST_HOST.to_lowercase()),
        None,
    )
    .unwrap();
    let token_str = String::from_utf8(token).unwrap();

    (token_str, did)
}

async fn extract_from_request(
    auth_header: Option<&str>,
) -> Result<ExtractIdentity, IdentityExtractionError> {
    let mut builder = Request::builder().uri("/test").header(HOST, TEST_HOST); // Add Host header for audience validation
    if let Some(header) = auth_header {
        builder = builder.header(AUTHORIZATION, header);
    }
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();
    ExtractIdentity::from_request_parts(&mut parts, &()).await
}

#[tokio::test]
async fn test_no_auth_header_returns_anonymous() {
    let result = extract_from_request(None).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_anonymous());
}

#[tokio::test]
async fn test_empty_bearer_returns_anonymous() {
    let result = extract_from_request(Some("Bearer ")).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_anonymous());
}

#[tokio::test]
async fn test_non_bearer_auth_returns_error() {
    // Go DefraDB behavior: non-Bearer auth returns 403 Forbidden
    let result = extract_from_request(Some("Basic dXNlcjpwYXNz")).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        IdentityExtractionError::InvalidToken(_)
    ));
}

#[tokio::test]
async fn test_valid_bearer_token_extracts_did() {
    let (token, expected_did) = create_test_token();
    let auth_header = format!("Bearer {}", token);

    let result = extract_from_request(Some(&auth_header)).await;
    assert!(result.is_ok());
    let extracted = result.unwrap();
    assert!(!extracted.is_anonymous());
    assert_eq!(extracted.into_did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_lowercase_bearer_works() {
    let (token, expected_did) = create_test_token();
    let auth_header = format!("bearer {}", token);

    let result = extract_from_request(Some(&auth_header)).await;
    assert!(result.is_ok());
    let extracted = result.unwrap();
    assert!(!extracted.is_anonymous());
    assert_eq!(extracted.into_did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_invalid_token_returns_error() {
    let result = extract_from_request(Some("Bearer invalid-token")).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        IdentityExtractionError::InvalidToken(_)
    ));
}

#[tokio::test]
async fn test_extract_token_identity_full() {
    let (token, expected_did) = create_test_token();
    let auth_header = format!("Bearer {}", token);

    let builder = Request::builder()
        .uri("/test")
        .header(HOST, TEST_HOST) // Add Host header for audience validation
        .header(AUTHORIZATION, auth_header);
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    let result = ExtractTokenIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let extracted = result.unwrap();
    assert!(extracted.identity().is_some());
    assert_eq!(extracted.did().unwrap(), expected_did);
}

// === Host Header Security Tests ===
// These tests verify that authenticated requests require a valid Host header
// to prevent token bypass via missing/malformed Host header attacks.

#[tokio::test]
async fn test_missing_host_header_with_token_returns_error() {
    // Create a valid token
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let token = new_token(
        &identity,
        Duration::from_secs(3600),
        Some(TEST_HOST.to_lowercase()),
        None,
    )
    .unwrap();
    let token_str = String::from_utf8(token).unwrap();
    let auth_header = format!("Bearer {}", token_str);

    // Build request WITHOUT Host header but WITH Authorization
    let builder = Request::builder()
        .uri("/test")
        .header(AUTHORIZATION, auth_header);
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    let result = ExtractIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        IdentityExtractionError::MissingHost(msg) => {
            assert!(msg.contains("Host header required"));
        }
        other => panic!("Expected MissingHost error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_missing_host_header_anonymous_request_succeeds() {
    // Build request WITHOUT Host header and WITHOUT Authorization (anonymous)
    let builder = Request::builder().uri("/test");
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    let result = ExtractIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_anonymous());
}

#[tokio::test]
async fn test_empty_bearer_with_missing_host_succeeds() {
    // Build request WITHOUT Host header but with empty Bearer token
    // Empty bearer = anonymous, so this should succeed
    let builder = Request::builder()
        .uri("/test")
        .header(AUTHORIZATION, "Bearer ");
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    let result = ExtractIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_anonymous());
}

#[tokio::test]
async fn test_extract_token_identity_missing_host_returns_error() {
    // Create a valid token
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let token = new_token(
        &identity,
        Duration::from_secs(3600),
        Some(TEST_HOST.to_lowercase()),
        None,
    )
    .unwrap();
    let token_str = String::from_utf8(token).unwrap();
    let auth_header = format!("Bearer {}", token_str);

    // Build request WITHOUT Host header but WITH Authorization
    let builder = Request::builder()
        .uri("/test")
        .header(AUTHORIZATION, auth_header);
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    // ExtractTokenIdentity should also reject missing Host with token
    let result = ExtractTokenIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        IdentityExtractionError::MissingHost(msg) => {
            assert!(msg.contains("Host header required"));
        }
        other => panic!("Expected MissingHost error, got {:?}", other),
    }
}

// === Additional Edge Case Tests ===

#[tokio::test]
async fn test_wrong_host_header_with_valid_token_returns_error() {
    // Create a token for one host (localhost:9181)
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let token = new_token(
        &identity,
        Duration::from_secs(3600),
        Some("localhost:9181".to_lowercase()),
        None,
    )
    .unwrap();
    let token_str = String::from_utf8(token).unwrap();
    let auth_header = format!("Bearer {}", token_str);

    // Build request WITH a DIFFERENT Host header
    let builder = Request::builder()
        .uri("/test")
        .header(HOST, "different-host:8080")
        .header(AUTHORIZATION, auth_header);
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    // Should reject because audience doesn't match
    let result = ExtractIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        IdentityExtractionError::TokenVerificationFailed(msg) => {
            assert!(
                msg.contains("audience mismatch"),
                "Expected audience mismatch error, got: {}",
                msg
            );
        }
        other => panic!("Expected TokenVerificationFailed error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_bearer_token_with_leading_whitespace() {
    let (token, expected_did) = create_test_token();
    // Token with leading spaces after "Bearer "
    let auth_header = format!("Bearer   {}", token);

    let result = extract_from_request(Some(&auth_header)).await;
    // Should still work because token is trimmed
    assert!(result.is_ok());
    let extracted = result.unwrap();
    assert!(!extracted.is_anonymous());
    assert_eq!(extracted.into_did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_bearer_token_with_trailing_whitespace() {
    let (token, expected_did) = create_test_token();
    // Token with trailing spaces
    let auth_header = format!("Bearer {}   ", token);

    let result = extract_from_request(Some(&auth_header)).await;
    // Should still work because token is trimmed
    assert!(result.is_ok());
    let extracted = result.unwrap();
    assert!(!extracted.is_anonymous());
    assert_eq!(extracted.into_did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_uppercase_bearer_returns_error() {
    // "BEARER" (uppercase) should be rejected - only "Bearer" and "bearer" are valid
    let (token, _) = create_test_token();
    let auth_header = format!("BEARER {}", token);

    let result = extract_from_request(Some(&auth_header)).await;
    // Should fail - not a valid Bearer prefix
    assert!(result.is_err());
}
