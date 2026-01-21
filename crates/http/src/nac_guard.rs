//! NAC (Node Access Control) permission guard helpers.
//!
//! This module provides utility functions for HTTP handlers to enforce
//! NAC permission checks before performing operations.

use identity::Did;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::router::{AppState, NodePermission};

/// Check if an identity has a specific NAC permission.
///
/// Returns `Ok(())` if the permission is granted, or an appropriate error:
/// - `HttpError::Forbidden` if the identity lacks the required permission
/// - `HttpError::Forbidden` if authentication is required but not provided
///
/// If NAC is not configured on the server, all permissions are allowed.
pub async fn require_permission(
    state: &AppState,
    identity: &ExtractIdentity,
    permission: NodePermission,
) -> Result<(), HttpError> {
    // If NAC is not configured, allow all operations
    let Some(nac) = &state.nac else {
        return Ok(());
    };

    // Get the identity DID, requiring authentication for NAC-protected operations
    let did = identity.did().ok_or_else(|| {
        HttpError::Forbidden(format!(
            "authentication required for {} operation",
            permission
        ))
    })?;

    // Check the permission
    let allowed = nac
        .check_permission(did, permission)
        .await
        .map_err(|e| HttpError::Internal(format!("NAC check failed: {}", e)))?;

    if !allowed {
        return Err(HttpError::Forbidden(format!(
            "identity does not have {} permission",
            permission
        )));
    }

    Ok(())
}

/// Check if an identity has a specific NAC permission, allowing anonymous access.
///
/// Similar to `require_permission` but allows anonymous access when NAC
/// is not enabled or for read-only operations.
///
/// Returns `Ok(())` if the permission is granted, or an appropriate error:
/// - `HttpError::Forbidden` if the identity lacks the required permission
///
/// If NAC is not configured on the server, all permissions are allowed.
/// If the identity is anonymous but NAC is enabled, permission is checked
/// with a synthetic anonymous DID.
pub async fn check_permission_optional_auth(
    state: &AppState,
    identity: &ExtractIdentity,
    permission: NodePermission,
) -> Result<(), HttpError> {
    // If NAC is not configured, allow all operations
    let Some(nac) = &state.nac else {
        return Ok(());
    };

    // If anonymous, check if NAC is actually enabled
    let Some(did) = identity.did() else {
        // For anonymous users, check if NAC status allows the operation
        // (NotConfigured or DisabledTemporarily allows all)
        let status = nac.get_status().await;
        if status.to_string() != "enabled" {
            return Ok(());
        }
        // NAC is enabled but user is anonymous - deny
        return Err(HttpError::Forbidden(format!(
            "authentication required for {} operation when NAC is enabled",
            permission
        )));
    };

    // Check the permission
    let allowed = nac
        .check_permission(did, permission)
        .await
        .map_err(|e| HttpError::Internal(format!("NAC check failed: {}", e)))?;

    if !allowed {
        return Err(HttpError::Forbidden(format!(
            "identity does not have {} permission",
            permission
        )));
    }

    Ok(())
}

/// Get the DID from an identity extractor, returning an error if not authenticated.
///
/// Helper for handlers that need the DID after a permission check.
pub fn require_identity(identity: &ExtractIdentity) -> Result<&Did, HttpError> {
    identity
        .did()
        .ok_or_else(|| HttpError::Forbidden("authentication required".into()))
}
