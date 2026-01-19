//! Identity extractor tests.

use std::time::Duration;

use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, Request},
};
use identity::{new_token, Did, Identity, RawIdentity};

use defra_http::{ExtractIdentity, ExtractTokenIdentity, IdentityExtractionError};

fn create_test_token() -> (String, Did) {
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let did = identity.did().unwrap();

    let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();
    let token_str = String::from_utf8(token).unwrap();

    (token_str, did)
}

async fn extract_from_request(
    auth_header: Option<&str>,
) -> Result<ExtractIdentity, IdentityExtractionError> {
    let mut builder = Request::builder().uri("/test");
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
        .header(AUTHORIZATION, auth_header);
    let request = builder.body(()).unwrap();
    let (mut parts, _body) = request.into_parts();

    let result = ExtractTokenIdentity::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let extracted = result.unwrap();
    assert!(extracted.identity().is_some());
    assert_eq!(extracted.did().unwrap(), expected_did);
}
