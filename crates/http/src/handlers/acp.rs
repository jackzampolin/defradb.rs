//! ACP (Access Control Policy) endpoint handlers.
//!
//! These handlers provide HTTP access to ACP policy management:
//! - Add policy
//! - List policies
//! - Get policy by ID

use axum::{
    extract::{Path, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::router::{AppState, PolicyInfo};

/// Request to add a new ACP policy.
#[derive(Debug, Clone, Deserialize)]
pub struct AddPolicyRequest {
    pub policy: String,
}

/// Response for adding a policy.
#[derive(Debug, Clone, Serialize)]
pub struct AddPolicyResponse {
    #[serde(rename = "PolicyID")]
    pub policy_id: String,
}

/// Add a new ACP policy.
///
/// POST /api/v0/acp/policy
pub async fn add_policy(
    State(state): State<AppState>,
    Json(request): Json<AddPolicyRequest>,
) -> Result<Json<AddPolicyResponse>, HttpError> {
    let acp = state
        .acp
        .as_ref()
        .ok_or_else(|| HttpError::Internal("ACP not configured".into()))?;

    if request.policy.trim().is_empty() {
        return Err(HttpError::BadRequest("policy cannot be empty".into()));
    }

    let policy_id = acp
        .add_policy(&request.policy)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(AddPolicyResponse { policy_id }))
}

/// List all ACP policies.
///
/// GET /api/v0/acp/policy
pub async fn list_policies(
    State(state): State<AppState>,
) -> Result<Json<Vec<PolicyInfo>>, HttpError> {
    let acp = state
        .acp
        .as_ref()
        .ok_or_else(|| HttpError::Internal("ACP not configured".into()))?;

    let policies = acp
        .list_policies()
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(policies))
}

/// Get a specific ACP policy by ID.
///
/// GET /api/v0/acp/policy/:id
pub async fn get_policy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PolicyInfo>, HttpError> {
    let acp = state
        .acp
        .as_ref()
        .ok_or_else(|| HttpError::Internal("ACP not configured".into()))?;

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
    fn test_add_policy_request_deserialize() {
        let json = r#"{"policy": "name: test\nresources:\n  users:\n    permissions:\n      read:\n        expr: owner"}"#;
        let request: AddPolicyRequest = serde_json::from_str(json).unwrap();
        assert!(request.policy.contains("test"));
    }

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
