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
/// # Anonymous / Wildcard Fallback
///
/// When no identity is provided (anonymous), checks the wildcard DID (`*`)
/// for the requested permission before rejecting. This matches FFI behavior
/// in `nac_check.rs` where anonymous requests check `Did::wildcard()`.
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

    let did = match identity.did() {
        Some(d) => d.clone(),
        None => {
            // Anonymous request: check if wildcard DID has the permission.
            // Matches FFI nac_check.rs behavior where empty/null identity
            // checks Did::wildcard() before rejecting.
            let wildcard = Did::wildcard();
            let wildcard_allowed = nac
                .check_permission(&wildcard, permission)
                .await
                .unwrap_or(false);
            if wildcard_allowed {
                return Ok(());
            }
            return Err(HttpError::Unauthorized(
                "not authorized to perform operation".into(),
            ));
        }
    };

    // Check the permission for the explicit identity
    let allowed = nac.check_permission(&did, permission).await.map_err(|e| {
        tracing::error!(error = %e, ?permission, "NAC permission check failed");
        HttpError::Internal("permission check failed".into())
    })?;

    if !allowed {
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
