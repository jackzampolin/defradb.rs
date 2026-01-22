//! NAC (Node Access Control) permission guard helpers.
//!
//! This module provides utility functions for HTTP handlers to enforce
//! NAC permission checks before performing operations.
//!
//! # Error Handling
//!
//! This module returns `401 Unauthorized` for NAC permission denials to match
//! Go DefraDB's behavior. Go's `CollectionMiddleware` returns 401 when it
//! detects `ErrNotAuthorizedToPerformOperation`.

use identity::Did;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::router::{AppState, NodePermission};

/// Check if an identity has a specific NAC permission.
///
/// Returns `Ok(())` if the permission is granted, or an appropriate error:
/// - `HttpError::Unauthorized` (401) if the identity lacks the required permission
/// - `HttpError::Unauthorized` (401) if authentication is required but not provided
///
/// If NAC is not configured on the server, all permissions are allowed.
///
/// # Go DefraDB Compatibility
///
/// Returns 401 Unauthorized to match Go DefraDB's CollectionMiddleware which
/// returns `http.StatusUnauthorized` for `ErrNotAuthorizedToPerformOperation`.
pub async fn require_permission(
    state: &AppState,
    identity: &ExtractIdentity,
    permission: NodePermission,
) -> Result<(), HttpError> {
    // If NAC is not configured, allow all operations
    let Some(nac) = &state.nac else {
        return Ok(());
    };

    // Get the identity DID, requiring authentication for NAC-protected operations.
    // In Go, if no identity is provided, the NAC check proceeds and fails with
    // "not authorized to perform operation", so we use Unauthorized here too.
    let did = identity
        .did()
        .ok_or_else(|| HttpError::Unauthorized("authentication required".into()))?;

    // Check the permission
    let allowed = nac
        .check_permission(did, permission)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, ?permission, "NAC permission check failed");
            HttpError::Internal("permission check failed".into())
        })?;

    if !allowed {
        // Return 401 to match Go DefraDB's CollectionMiddleware behavior
        return Err(HttpError::Unauthorized(
            "not authorized to perform operation".into(),
        ));
    }

    Ok(())
}

/// Get the DID from an identity extractor, returning an error if not authenticated.
///
/// Helper for handlers that need the DID after a permission check.
pub fn require_identity(identity: &ExtractIdentity) -> Result<&Did, HttpError> {
    identity
        .did()
        .ok_or_else(|| HttpError::Unauthorized("authentication required".into()))
}
