//! NAC (Node Access Control) handlers.
//!
//! These handlers provide HTTP endpoints for managing NAC, including:
//! - Getting NAC status
//! - Adding/removing admin relationships
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{extract::State, response::IntoResponse, Json};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NacStatusInfo, NodePermission};

/// Request body for adding/removing admin (Rust format).
#[derive(Debug, serde::Deserialize)]
pub struct AdminRequest {
    /// The target DID to add/remove as admin.
    pub target: String,
}

/// Request body for Go-compatible NAC relationship operations.
/// Go DefraDB uses: `{"Relation": "admin", "TargetActor": "did:key:..."}`
#[derive(Debug, serde::Deserialize)]
pub struct GoNacRelationshipRequest {
    /// Relation type (e.g., "admin").
    #[serde(rename = "Relation")]
    pub relation: String,
    /// Target actor DID.
    #[serde(rename = "TargetActor")]
    pub target_actor: String,
}

/// Response for admin operations.
#[derive(Debug, serde::Serialize)]
pub struct AdminResponse {
    /// Whether the operation was successful.
    pub success: bool,
    /// A message describing the result.
    pub message: String,
}

/// GET /api/v0/nac/status
///
/// Get the current NAC status including whether it's enabled and who the owner is.
///
/// Requires `NacStatus` permission when NAC is enabled.
pub async fn get_status(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::NacStatus).await?;

    // NAC status is available even if NAC is not configured
    match &state.nac {
        Some(nac) => {
            let status = nac.get_status().await;
            let owner = nac.owner().await;

            let info = NacStatusInfo {
                status: status.to_string(),
                owner: owner.map(|d| d.to_string()),
            };

            Ok(Json(info).into_response())
        }
        None => {
            // NAC not configured
            let info = NacStatusInfo {
                status: "not configured".to_string(),
                owner: None,
            };
            Ok(Json(info).into_response())
        }
    }
}

/// POST /api/v0/nac/admin
///
/// Add a new admin. The requestor must be an existing admin.
///
/// Requires `NacRelationAdd` permission when NAC is enabled.
pub async fn add_admin(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<AdminRequest>,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::NacRelationAdd).await?;

    // Require NAC to be available
    let nac = state.require_nac()?;

    // Require authenticated identity
    let requestor = identity
        .did()
        .cloned()
        .ok_or_else(|| HttpError::Forbidden("authentication required to add admin".into()))?;

    // Parse target DID
    let target = identity::Did::new(&body.target)
        .map_err(|e| HttpError::BadRequest(format!("invalid target DID: {}", e)))?;

    // Add admin
    let added = nac.add_admin(&requestor, &target).await.map_err(|e| {
        // Log the actual error for debugging, but return generic message to prevent information leakage
        tracing::warn!(error = %e, "NAC add_admin operation failed");
        HttpError::Forbidden("not authorized to add admin".into())
    })?;

    let message = if added {
        format!("admin added: {}", target)
    } else {
        format!("identity is already an admin: {}", target)
    };

    Ok(Json(AdminResponse {
        success: true,
        message,
    }))
}

/// DELETE /api/v0/nac/admin
///
/// Remove an admin. The requestor must be an existing admin.
/// The owner cannot be removed.
///
/// Requires `NacRelationDelete` permission when NAC is enabled.
pub async fn remove_admin(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<AdminRequest>,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::NacRelationDelete).await?;

    // Require NAC to be available
    let nac = state.require_nac()?;

    // Require authenticated identity
    let requestor = identity
        .did()
        .cloned()
        .ok_or_else(|| HttpError::Forbidden("authentication required to remove admin".into()))?;

    // Parse target DID
    let target = identity::Did::new(&body.target)
        .map_err(|e| HttpError::BadRequest(format!("invalid target DID: {}", e)))?;

    // Remove admin
    let removed = nac.remove_admin(&requestor, &target).await.map_err(|e| {
        // Log the actual error for debugging, but return generic message to prevent information leakage
        tracing::warn!(error = %e, "NAC remove_admin operation failed");
        HttpError::Forbidden("not authorized to remove admin".into())
    })?;

    let message = if removed {
        format!("admin removed: {}", target)
    } else {
        format!("identity is not an admin: {}", target)
    };

    Ok(Json(AdminResponse {
        success: true,
        message,
    }))
}

// ============================================================================
// Go-compatible NAC endpoints (aliased as /acp/node/*)
// ============================================================================

/// POST /api/v0/acp/node/relationship (Go-compatible)
///
/// Add a NAC relationship. Go DefraDB format:
/// `{"Relation": "admin", "TargetActor": "did:key:..."}`
///
/// Requires `NacRelationAdd` permission when NAC is enabled.
pub async fn go_add_relationship(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<GoNacRelationshipRequest>,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::NacRelationAdd).await?;

    // Require NAC to be available
    let nac = state.require_nac()?;

    // Only "admin" relation is supported
    if body.relation.to_lowercase() != "admin" {
        return Err(HttpError::BadRequest(format!(
            "unsupported relation type '{}': only 'admin' is supported",
            body.relation
        )));
    }

    // Require authenticated identity
    let requestor = identity.did().cloned().ok_or_else(|| {
        HttpError::Forbidden("authentication required to add relationship".into())
    })?;

    // Parse target DID
    let target = identity::Did::new(&body.target_actor)
        .map_err(|e| HttpError::BadRequest(format!("invalid TargetActor DID: {}", e)))?;

    // Add admin
    let added = nac.add_admin(&requestor, &target).await.map_err(|e| {
        tracing::warn!(error = %e, "NAC add_admin operation failed");
        HttpError::Forbidden("not authorized to add relationship".into())
    })?;

    // Go returns 200 OK with empty body on success (regardless of whether already added)
    let _ = added; // Indicate we intentionally don't use this value
    Ok(axum::http::StatusCode::OK.into_response())
}

/// DELETE /api/v0/acp/node/relationship (Go-compatible)
///
/// Remove a NAC relationship. Go DefraDB format:
/// `{"Relation": "admin", "TargetActor": "did:key:..."}`
///
/// Requires `NacRelationDelete` permission when NAC is enabled.
pub async fn go_remove_relationship(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<GoNacRelationshipRequest>,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::NacRelationDelete).await?;

    // Require NAC to be available
    let nac = state.require_nac()?;

    // Only "admin" relation is supported
    if body.relation.to_lowercase() != "admin" {
        return Err(HttpError::BadRequest(format!(
            "unsupported relation type '{}': only 'admin' is supported",
            body.relation
        )));
    }

    // Require authenticated identity
    let requestor = identity.did().cloned().ok_or_else(|| {
        HttpError::Forbidden("authentication required to remove relationship".into())
    })?;

    // Parse target DID
    let target = identity::Did::new(&body.target_actor)
        .map_err(|e| HttpError::BadRequest(format!("invalid TargetActor DID: {}", e)))?;

    // Remove admin
    let _removed = nac.remove_admin(&requestor, &target).await.map_err(|e| {
        tracing::warn!(error = %e, "NAC remove_admin operation failed");
        HttpError::Forbidden("not authorized to remove relationship".into())
    })?;

    // Go returns 200 OK with empty body on success
    Ok(axum::http::StatusCode::OK.into_response())
}
