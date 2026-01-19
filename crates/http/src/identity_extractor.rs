//! Identity extraction from HTTP Authorization headers.
//!
//! This module provides an Axum extractor for parsing JWT bearer tokens
//! from the Authorization header and extracting the identity.

use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use std::future::Future;

use identity::{from_token, Did, Identity, TokenIdentity};

use crate::error::ErrorResponse;

/// Extractor for identity from Authorization header.
///
/// Parses `Authorization: Bearer <JWT>` header and extracts the identity.
/// If no Authorization header is present, the identity is None (anonymous).
/// If the token is invalid or malformed, returns a 403 Forbidden error
/// (matching Go DefraDB behavior).
#[derive(Debug, Clone)]
pub struct ExtractIdentity(pub Option<Did>);

impl ExtractIdentity {
    /// Returns a reference to the extracted DID if present.
    pub fn did(&self) -> Option<&Did> {
        self.0.as_ref()
    }

    /// Consumes self and returns the extracted DID if present.
    pub fn into_did(self) -> Option<Did> {
        self.0
    }
}

/// Error type for identity extraction failures.
#[derive(Debug)]
pub enum IdentityExtractionError {
    /// Invalid token format or signature.
    /// Returns 403 Forbidden to match Go DefraDB behavior.
    InvalidToken(String),
}

impl IntoResponse for IdentityExtractionError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            // Go DefraDB returns 403 Forbidden for invalid tokens, not 401 Unauthorized
            IdentityExtractionError::InvalidToken(msg) => {
                (StatusCode::FORBIDDEN, format!("Invalid token: {}", msg))
            }
        };

        (status, Json(ErrorResponse { error: message })).into_response()
    }
}

/// Extract identity from Authorization header value.
///
/// Behavior matches Go DefraDB:
/// - No Authorization header → anonymous
/// - "Bearer " with empty token → anonymous
/// - "Bearer <token>" → parse token, 403 on failure
/// - Non-Bearer auth → 403 Forbidden (treated as invalid token)
fn extract_identity_from_auth_header(
    auth_value: Option<&str>,
) -> Result<Option<Did>, IdentityExtractionError> {
    let Some(auth_value) = auth_value else {
        // No Authorization header = anonymous request
        return Ok(None);
    };

    // Check for Bearer prefix (case-insensitive for "Bearer" only)
    let token = if let Some(token) = auth_value.strip_prefix("Bearer ") {
        token.trim()
    } else if let Some(token) = auth_value.strip_prefix("bearer ") {
        token.trim()
    } else {
        // Go DefraDB behavior: Non-Bearer auth is treated as an invalid token.
        // strings.TrimPrefix doesn't strip if prefix doesn't match, so the
        // whole header becomes the "token" and fails to parse → 403 Forbidden.
        return Err(IdentityExtractionError::InvalidToken(
            "unsupported authorization scheme (expected Bearer)".to_string(),
        ));
    };

    // Empty token after stripping prefix = anonymous
    if token.is_empty() {
        return Ok(None);
    }

    // Parse the token
    let token_identity = from_token(token.as_bytes())
        .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;

    // Extract DID
    let did = token_identity
        .did()
        .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;

    Ok(Some(did))
}

impl<S> FromRequestParts<S> for ExtractIdentity
where
    S: Send + Sync,
{
    type Rejection = IdentityExtractionError;

    fn from_request_parts<'life0, 'life1, 'async_trait>(
        parts: &'life0 mut Parts,
        _state: &'life1 S,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<Self, Self::Rejection>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // Get the auth header value before entering the async block
        let auth_value = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Box::pin(async move {
            let result = extract_identity_from_auth_header(auth_value.as_deref())?;
            Ok(ExtractIdentity(result))
        })
    }
}

/// Full token identity extractor.
///
/// Similar to `ExtractIdentity` but keeps the full `TokenIdentity`
/// for cases where more than just the DID is needed (e.g., audience verification).
#[derive(Debug)]
pub struct ExtractTokenIdentity(pub Option<TokenIdentity>);

impl ExtractTokenIdentity {
    /// Returns the extracted token identity if present.
    pub fn identity(&self) -> Option<&TokenIdentity> {
        self.0.as_ref()
    }

    /// Returns the DID if identity is present.
    pub fn did(&self) -> Option<Did> {
        self.0.as_ref().and_then(|id| id.did().ok())
    }
}

/// Extract full token identity from Authorization header value.
///
/// Same behavior as `extract_identity_from_auth_header` but returns
/// the full `TokenIdentity` instead of just the DID.
fn extract_token_identity_from_auth_header(
    auth_value: Option<&str>,
) -> Result<Option<TokenIdentity>, IdentityExtractionError> {
    let Some(auth_value) = auth_value else {
        return Ok(None);
    };

    // Check for Bearer prefix (case-insensitive for "Bearer" only)
    let token = if let Some(token) = auth_value.strip_prefix("Bearer ") {
        token.trim()
    } else if let Some(token) = auth_value.strip_prefix("bearer ") {
        token.trim()
    } else {
        // Go DefraDB behavior: Non-Bearer auth is treated as an invalid token → 403
        return Err(IdentityExtractionError::InvalidToken(
            "unsupported authorization scheme (expected Bearer)".to_string(),
        ));
    };

    if token.is_empty() {
        return Ok(None);
    }

    // Parse the token
    let token_identity = from_token(token.as_bytes())
        .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;

    Ok(Some(token_identity))
}

impl<S> FromRequestParts<S> for ExtractTokenIdentity
where
    S: Send + Sync,
{
    type Rejection = IdentityExtractionError;

    fn from_request_parts<'life0, 'life1, 'async_trait>(
        parts: &'life0 mut Parts,
        _state: &'life1 S,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<Self, Self::Rejection>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // Get the auth header value before entering the async block
        let auth_value = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Box::pin(async move {
            let result = extract_token_identity_from_auth_header(auth_value.as_deref())?;
            Ok(ExtractTokenIdentity(result))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use identity::{new_token, RawIdentity};
    use std::time::Duration;

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
        assert!(result.unwrap().0.is_none());
    }

    #[tokio::test]
    async fn test_empty_bearer_returns_anonymous() {
        let result = extract_from_request(Some("Bearer ")).await;
        assert!(result.is_ok());
        assert!(result.unwrap().0.is_none());
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
        assert!(extracted.0.is_some());
        assert_eq!(extracted.0.unwrap(), expected_did);
    }

    #[tokio::test]
    async fn test_lowercase_bearer_works() {
        let (token, expected_did) = create_test_token();
        let auth_header = format!("bearer {}", token);

        let result = extract_from_request(Some(&auth_header)).await;
        assert!(result.is_ok());
        let extracted = result.unwrap();
        assert!(extracted.0.is_some());
        assert_eq!(extracted.0.unwrap(), expected_did);
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
        assert!(extracted.0.is_some());
        assert_eq!(extracted.did().unwrap(), expected_did);
    }
}
