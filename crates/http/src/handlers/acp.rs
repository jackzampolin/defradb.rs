//! ACP (Access Control Policy) endpoint handlers.
//!
//! These handlers provide HTTP access to ACP policy management:
//! - Add policy
//! - List policies
//! - Get policy by ID
//!
//! All endpoints enforce NAC permissions when NAC is enabled.
//!
//! Note: The add_policy endpoint accepts text/plain body to match Go DefraDB behavior.
//! Go DefraDB reads the raw policy text from the request body, not JSON.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Serialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission, PolicyInfo};

/// Response for adding a policy.
#[derive(Debug, Clone, Serialize)]
pub struct AddPolicyResponse {
    #[serde(rename = "PolicyID")]
    pub policy_id: String,
}

/// Add a new ACP policy.
///
/// POST /api/v0/acp/policy
///
/// Accepts raw policy text in the request body (text/plain), matching Go DefraDB.
/// The policy should be valid YAML or JSON following the ACP policy specification.
///
/// Requires `DacPolicyAdd` permission when NAC is enabled.
pub async fn add_policy(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,
) -> Result<Json<AddPolicyResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacPolicyAdd).await?;

    let acp = state.require_acp()?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest("policy cannot be empty".into()));
    }

    let policy_id = acp
        .add_policy(&body)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(AddPolicyResponse { policy_id }))
}

/// List all ACP policies.
///
/// GET /api/v0/acp/policy
///
/// Requires `DacStatus` permission when NAC is enabled.
pub async fn list_policies(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<PolicyInfo>>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacStatus).await?;

    let acp = state.require_acp()?;

    let policies = acp.list_policies().await.map_err(HttpError::Internal)?;

    Ok(Json(policies))
}

/// Get a specific ACP policy by ID.
///
/// GET /api/v0/acp/policy/:id
///
/// Requires `DacStatus` permission when NAC is enabled.
pub async fn get_policy(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(id): Path<String>,
) -> Result<Json<PolicyInfo>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacStatus).await?;

    let acp = state.require_acp()?;

    let policy = acp
        .get_policy(&id)
        .await
        .map_err(HttpError::Internal)?
        .ok_or_else(|| HttpError::NotFound(format!("Policy '{}' not found", id)))?;

    Ok(Json(policy))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_policy_response_serialize() {
        let response = AddPolicyResponse {
            policy_id: "policy-123".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("PolicyID"));
        assert!(json.contains("policy-123"));
    }

    #[test]
    fn test_policy_info_serialize() {
        let info = PolicyInfo {
            id: "policy-123".to_string(),
            name: Some("Test Policy".to_string()),
            description: None,
            resources: None,
            actor: None,
            creation_time: None,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("policy-123"));
        assert!(json.contains("Test Policy"));
        // Should not contain null fields
        assert!(!json.contains("description"));
    }
}
