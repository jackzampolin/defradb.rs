//! Identity extraction from HTTP Authorization headers.
//!
//! This module provides an Axum extractor for parsing JWT bearer tokens
//! from the Authorization header and extracting the identity.
//!
//! Token verification follows Go DefraDB behavior:
//! - Tokens are verified using the request Host header as the expected audience
//! - Token expiration and not-before claims are validated
//! - Invalid or expired tokens return 403 Forbidden

use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, header::HOST, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};

use identity::{from_token, verify_auth_token, Did, Identity, TokenIdentity};

use crate::error::ErrorResponse;

/// Extractor for identity from Authorization header.
///
/// Parses `Authorization: Bearer <JWT>` header and extracts the identity.
/// If no Authorization header is present, the identity is None (anonymous).
/// If the token is invalid or malformed, returns a 403 Forbidden error
/// (matching Go DefraDB behavior).
///
/// The inner field is private to prevent construction without proper validation.
/// Use the extractor mechanism or `ExtractIdentity::anonymous()` for testing.
#[derive(Debug, Clone)]
pub struct ExtractIdentity(Option<Did>);

impl ExtractIdentity {
    /// Create an anonymous identity (no authentication).
    /// Use this for testing or internal code that needs to represent anonymous access.
    pub fn anonymous() -> Self {
        Self(None)
    }

    /// Create an ExtractIdentity from an already-parsed DID.
    /// Used by the auth middleware to construct an ExtractIdentity for permission checks.
    pub(crate) fn from_did(did: Option<Did>) -> Self {
        Self(did)
    }

    /// Returns a reference to the extracted DID if present.
    pub fn did(&self) -> Option<&Did> {
        self.0.as_ref()
    }

    /// Consumes self and returns the extracted DID if present.
    pub fn into_did(self) -> Option<Did> {
        self.0
    }

    /// Check if this is an anonymous (unauthenticated) request.
    pub fn is_anonymous(&self) -> bool {
        self.0.is_none()
    }
}

/// Error type for identity extraction failures.
#[derive(Debug)]
#[non_exhaustive]
pub enum IdentityExtractionError {
    /// Invalid token format or signature.
    /// Returns 403 Forbidden to match Go DefraDB behavior.
    InvalidToken(String),
    /// Token verification failed (expired, not yet valid, or audience mismatch).
    /// Returns 403 Forbidden to match Go DefraDB behavior.
    TokenVerificationFailed(String),
    /// Missing or invalid Host header for authenticated request.
    /// Returns 403 Forbidden because audience verification cannot proceed.
    MissingHost(String),
}

/// Result of extracting and validating the Host header.
#[derive(Debug)]
enum HostHeaderResult {
    /// Valid Host header value (lowercased).
    Valid(String),
    /// Host header is missing.
    Missing,
    /// Host header contains invalid (non-ASCII) characters.
    Invalid,
}

/// Extract and validate the Host header from HTTP headers.
fn extract_host_header(headers: &axum::http::HeaderMap) -> HostHeaderResult {
    match headers.get(HOST) {
        Some(value) => match value.to_str() {
            Ok(s) if !s.is_empty() => HostHeaderResult::Valid(s.to_lowercase()),
            Ok(_) => {
                tracing::warn!("Empty Host header in request");
                HostHeaderResult::Missing
            }
            Err(_) => {
                tracing::warn!("Host header contains non-ASCII characters");
                HostHeaderResult::Invalid
            }
        },
        None => {
            tracing::warn!("Missing Host header in request");
            HostHeaderResult::Missing
        }
    }
}

/// Identity pre-populated by the auth middleware.
///
/// When present in request extensions, `ExtractIdentity` reuses this
/// instead of re-parsing the Authorization header.
#[derive(Clone, Debug)]
pub(crate) struct MiddlewareIdentity(pub Option<Did>);

/// Parse identity from HTTP headers (shared between middleware and extractors).
///
/// Extracts the Authorization header, validates the Host header for
/// authenticated requests, and returns the parsed DID.
pub(crate) fn parse_identity_from_headers(
    headers: &axum::http::HeaderMap,
) -> Result<Option<Did>, IdentityExtractionError> {
    let auth_value = match headers.get(AUTHORIZATION) {
        Some(value) => match value.to_str() {
            Ok(s) => Some(s.to_string()),
            Err(_) => {
                return Err(IdentityExtractionError::InvalidToken(
                    "Authorization header contains invalid characters".to_string(),
                ))
            }
        },
        None => None,
    };

    let has_auth_token = match &auth_value {
        Some(auth) => {
            let trimmed = auth
                .strip_prefix("Bearer ")
                .or_else(|| auth.strip_prefix("bearer "));
            trimmed.map(|t| !t.trim().is_empty()).unwrap_or(false)
        }
        _ => false,
    };

    let host_result = extract_host_header(headers);

    let host = match host_result {
        HostHeaderResult::Valid(h) => h,
        HostHeaderResult::Missing if has_auth_token => {
            return Err(IdentityExtractionError::MissingHost(
                "Host header required for authenticated requests".to_string(),
            ));
        }
        HostHeaderResult::Invalid if has_auth_token => {
            return Err(IdentityExtractionError::MissingHost(
                "valid Host header required for authenticated requests".to_string(),
            ));
        }
        _ => String::new(),
    };

    extract_identity_from_auth_header(auth_value.as_deref(), &host)
}

impl IntoResponse for IdentityExtractionError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            // Go DefraDB returns 403 Forbidden for invalid tokens, not 401 Unauthorized
            IdentityExtractionError::InvalidToken(msg) => {
                (StatusCode::FORBIDDEN, format!("Invalid token: {}", msg))
            }
            // Token verification failures (expired, audience mismatch) also return 403
            IdentityExtractionError::TokenVerificationFailed(msg) => (
                StatusCode::FORBIDDEN,
                format!("Token verification failed: {}", msg),
            ),
            // Missing/invalid Host header for authenticated request returns 403
            IdentityExtractionError::MissingHost(msg) => {
                (StatusCode::FORBIDDEN, format!("Host header error: {}", msg))
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
/// - "Bearer <token>" → parse and verify token, 403 on failure
/// - Non-Bearer auth → 403 Forbidden (treated as invalid token)
///
/// Token verification includes:
/// - Expiration check (exp claim)
/// - Not-before check (nbf claim)
/// - Audience check (aud claim must contain expected_audience)
fn extract_identity_from_auth_header(
    auth_value: Option<&str>,
    expected_audience: &str,
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

    // Parse the token (validates signature)
    let token_identity = from_token(token.as_bytes())
        .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;

    // Verify token (expiration, not-before, audience)
    // Go DefraDB uses req.Host as expected audience
    verify_auth_token(&token_identity, expected_audience)
        .map_err(|e| IdentityExtractionError::TokenVerificationFailed(e.to_string()))?;

    // Extract DID
    let did = token_identity
        .did()
        .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;

    // Store the raw JWT keyed by DID for passthrough to SourceHub/hub.rs ACP operations.
    // Go DefraDB's BearerToken() pattern: the original JWT (signed by the user's key)
    // is forwarded to SourceHub for register_object and other bearer policy commands.
    defra_core::signing::set_request_bearer_token(did.as_str(), token.to_string());

    Ok(Some(did))
}

impl<S> FromRequestParts<S> for ExtractIdentity
where
    S: Send + Sync,
{
    type Rejection = IdentityExtractionError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // If the auth middleware already parsed identity, reuse it.
        if let Some(mid) = parts.extensions.get::<MiddlewareIdentity>() {
            return Ok(ExtractIdentity(mid.0.clone()));
        }

        // Fallback: parse from headers (for routes/tests without the middleware).
        let did = parse_identity_from_headers(&parts.headers)?;
        Ok(ExtractIdentity(did))
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
    ///
    /// If a token identity exists but DID derivation fails, logs a warning
    /// and returns None. For explicit error handling, use `try_did()`.
    pub fn did(&self) -> Option<Did> {
        self.0.as_ref().and_then(|id| match id.did() {
            Ok(did) => Some(did),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to derive DID from validated token identity - treating as anonymous"
                );
                None
            }
        })
    }

    /// Returns the DID if identity is present, or an error if DID derivation fails.
    ///
    /// Use this when you need to explicitly handle DID derivation errors.
    pub fn try_did(&self) -> Result<Option<Did>, IdentityExtractionError> {
        match &self.0 {
            Some(id) => {
                let did = id
                    .did()
                    .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;
                Ok(Some(did))
            }
            None => Ok(None),
        }
    }
}

/// Extract full token identity from Authorization header value.
///
/// Same behavior as `extract_identity_from_auth_header` but returns
/// the full `TokenIdentity` instead of just the DID.
///
/// Token verification includes:
/// - Expiration check (exp claim)
/// - Not-before check (nbf claim)
/// - Audience check (aud claim must contain expected_audience)
fn extract_token_identity_from_auth_header(
    auth_value: Option<&str>,
    expected_audience: &str,
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

    // Parse the token (validates signature)
    let token_identity = from_token(token.as_bytes())
        .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;

    // Verify token (expiration, not-before, audience)
    verify_auth_token(&token_identity, expected_audience)
        .map_err(|e| IdentityExtractionError::TokenVerificationFailed(e.to_string()))?;

    Ok(Some(token_identity))
}

impl<S> FromRequestParts<S> for ExtractTokenIdentity
where
    S: Send + Sync,
{
    type Rejection = IdentityExtractionError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Get the auth header value, returning error for malformed headers
        let auth_value: Option<String> = match parts.headers.get(AUTHORIZATION) {
            Some(value) => match value.to_str() {
                Ok(s) => Some(s.to_string()),
                Err(_) => {
                    return Err(IdentityExtractionError::InvalidToken(
                        "Authorization header contains invalid characters".to_string(),
                    ))
                }
            },
            None => None,
        };

        // Check if request has an Authorization header with a token
        let has_auth_token = auth_value
            .as_deref()
            .and_then(|auth| {
                auth.strip_prefix("Bearer ")
                    .or_else(|| auth.strip_prefix("bearer "))
            })
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false);

        // Get Host header for audience verification (matches Go DefraDB behavior)
        // For authenticated requests, require a valid Host header — this prevents
        // token bypass via missing/malformed Host header.
        let host = match extract_host_header(&parts.headers) {
            HostHeaderResult::Valid(h) => h,
            HostHeaderResult::Missing if has_auth_token => {
                return Err(IdentityExtractionError::MissingHost(
                    "Host header required for authenticated requests".to_string(),
                ));
            }
            HostHeaderResult::Invalid if has_auth_token => {
                return Err(IdentityExtractionError::MissingHost(
                    "valid Host header required for authenticated requests".to_string(),
                ));
            }
            // Anonymous requests don't need Host header
            _ => String::new(),
        };

        let result = extract_token_identity_from_auth_header(auth_value.as_deref(), &host)?;
        Ok(ExtractTokenIdentity(result))
    }
}
