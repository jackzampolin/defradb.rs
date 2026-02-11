//! NAC (Node Access Control) handlers.
//!
//! These handlers provide HTTP endpoints for managing NAC, including:
//! - Getting NAC status
//! - Adding/removing admin relationships
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{extract::State, response::IntoResponse, Json};

use crate::auth_error::normalize_auth_error;
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

/// Request body for enabling NAC (Go-compatible format).
#[derive(Debug, serde::Deserialize)]
pub struct EnableNacRequest {
    /// The owner DID to initialize NAC with.
    #[serde(rename = "OwnerDID")]
    pub owner_did: String,
}

/// Response for admin operations.
#[derive(Debug, serde::Serialize)]
pub struct AdminResponse {
    /// Whether the operation was successful.
    pub success: bool,
    /// A message describing the result.
    pub message: String,
}

/// POST /api/v0/acp/node/enable
///
/// Enable NAC with the given owner identity.
/// Go DefraDB format: `{"OwnerDID": "did:key:..."}`
pub async fn enable(
    State(state): State<AppState>,
    Json(body): Json<EnableNacRequest>,
) -> Result<impl IntoResponse, HttpError> {
    let nac = state.require_nac()?;

    let owner = identity::Did::new(&body.owner_did)
        .map_err(|e| HttpError::BadRequest(format!("invalid OwnerDID: {}", e)))?;

    nac.enable(&owner).await.map_err(|e| {
        tracing::warn!(error = %e, "NAC enable operation failed");
        HttpError::BadRequest(e)
    })?;

    Ok(axum::http::StatusCode::OK.into_response())
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

    match &state.nac {
        Some(nac) => {
            let info = nac.info().await;
            Ok(Json(info).into_response())
        }
        None => {
            let info = NacStatusInfo {
                status: "not configured".to_string(),
                configured_enabled: false,
                dev_mode: false,
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

    let nac = state.require_nac()?;

    // "owner" relation is immutable
    if body.relation == "owner" {
        return Err(HttpError::BadRequest(
            "relation not in resource".to_string(),
        ));
    }

    // Require authenticated identity
    let requestor = identity.did().cloned().ok_or_else(|| {
        HttpError::Forbidden("authentication required to add relationship".into())
    })?;

    if body.target_actor.is_empty() {
        return Err(HttpError::BadRequest("actor must be a valid did".into()));
    }

    // Parse target DID
    let target = identity::Did::new(&body.target_actor)
        .map_err(|e| HttpError::BadRequest(format!("invalid TargetActor DID: {}", e)))?;

    let added = nac
        .add_relationship(&requestor, &target, &body.relation)
        .await
        .map_err(|e| {
            let normalized = normalize_auth_error(e, "nac-relation-add");
            tracing::warn!(error = %normalized, "NAC add_relationship operation failed");
            HttpError::Forbidden(normalized)
        })?;

    Ok(Json(serde_json::json!({"added": added})).into_response())
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

    let nac = state.require_nac()?;

    // "owner" relation is immutable
    if body.relation == "owner" {
        return Err(HttpError::BadRequest(
            "relation not in resource".to_string(),
        ));
    }

    // Require authenticated identity
    let requestor = identity.did().cloned().ok_or_else(|| {
        HttpError::Forbidden("authentication required to remove relationship".into())
    })?;

    if body.target_actor.is_empty() {
        return Ok(Json(serde_json::json!({"deleted": false})).into_response());
    }

    // Parse target DID
    let target = identity::Did::new(&body.target_actor)
        .map_err(|e| HttpError::BadRequest(format!("invalid TargetActor DID: {}", e)))?;

    let removed = nac
        .remove_relationship(&requestor, &target, &body.relation)
        .await
        .map_err(|e| {
            let normalized = normalize_auth_error(e, "nac-relation-delete");
            tracing::warn!(error = %normalized, "NAC remove_relationship operation failed");
            HttpError::Forbidden(normalized)
        })?;

    Ok(Json(serde_json::json!({"deleted": removed})).into_response())
}

/// POST /api/v0/acp/node/disable
///
/// Temporarily disable NAC on this node.
/// The requestor must be an admin.
pub async fn disable(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<()>, HttpError> {
    let nac = state.require_nac()?;

    let requestor = identity
        .did()
        .cloned()
        .ok_or_else(|| HttpError::Forbidden("authentication required to disable NAC".into()))?;

    nac.disable(&requestor).await.map_err(|e| {
        let normalized = normalize_auth_error(e, "nac-disable");
        tracing::warn!(error = %normalized, "NAC disable operation failed");
        HttpError::Forbidden(normalized)
    })?;

    Ok(Json(()))
}

/// POST /api/v0/acp/node/re-enable
///
/// Re-enable NAC on this node after it was disabled.
/// The requestor must be an admin.
pub async fn re_enable(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<()>, HttpError> {
    let nac = state.require_nac()?;

    let requestor = identity
        .did()
        .cloned()
        .ok_or_else(|| HttpError::Forbidden("authentication required to re-enable NAC".into()))?;

    nac.re_enable(&requestor).await.map_err(|e| {
        let normalized = normalize_auth_error(e, "nac-re-enable");
        tracing::warn!(error = %normalized, "NAC re-enable operation failed");
        HttpError::Forbidden(normalized)
    })?;

    Ok(Json(()))
}
