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
use serde::{Deserialize, Serialize};

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AcpLightClientStatus, AppState, NodePermission, PolicyInfo};

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

    let _creator = identity
        .did()
        .ok_or_else(|| HttpError::BadRequest("policy creator can not be empty".into()))?;

    let acp = state.require_acp()?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest("policy data can not be empty".into()));
    }

    let policy_id = acp
        .add_policy(&body)
        .await
        .map_err(http_error_from_backend_message)?;

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

/// Get ACP light client status.
///
/// GET /api/v0/acp/status
///
/// Requires `DacStatus` permission when NAC is enabled.
pub async fn get_status(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<AcpLightClientStatus>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacStatus).await?;

    let acp = state.require_acp()?;
    let status = acp
        .get_light_client_status()
        .await
        .map_err(HttpError::ServiceUnavailable)?;

    Ok(Json(status))
}

/// Request body for document ACP relationship operations (Go-compatible).
#[derive(Debug, Clone, Deserialize)]
pub struct DocRelationshipRequest {
    #[serde(alias = "collection")]
    pub collection: String,
    #[serde(alias = "docID")]
    pub doc_id: String,
    #[serde(alias = "relation")]
    pub relation: String,
    #[serde(alias = "actor")]
    pub target_actor: String,
}

/// Response for document ACP relationship operations (Go-compatible).
#[derive(Debug, Clone, Serialize)]
pub struct DocRelationshipResponse {
    #[serde(rename = "ExistedAlready")]
    pub existed_already: bool,
}

/// Request body for document ACP decision preview operations.
#[derive(Debug, Clone, Deserialize)]
pub struct DocDecisionRequest {
    pub actor: String,
    pub permission: String,
    #[serde(rename = "policyID", alias = "policy_id")]
    pub policy_id: String,
    #[serde(rename = "resourceName", alias = "resource_name")]
    pub resource_name: String,
    #[serde(rename = "docID", alias = "doc_id")]
    pub doc_id: String,
}

/// Response for document ACP decision preview operations.
#[derive(Debug, Clone, Serialize)]
pub struct DocDecisionResponse {
    pub allowed: bool,
}

fn parse_doc_permission(permission: &str) -> Result<acp::DocumentPermission, HttpError> {
    match permission {
        "read" => Ok(acp::DocumentPermission::Read),
        "update" => Ok(acp::DocumentPermission::Update),
        "delete" => Ok(acp::DocumentPermission::Delete),
        _ => Err(HttpError::BadRequest(format!(
            "invalid document permission '{}'",
            permission
        ))),
    }
}

/// Check document ACP access without mutating relationships.
///
/// POST /api/v0/acp/document/decide
///
/// Requires `DacStatus` permission when NAC is enabled.
pub async fn decide_doc_access(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<DocDecisionRequest>,
) -> Result<Json<DocDecisionResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacStatus).await?;

    if body.actor.is_empty()
        || body.permission.is_empty()
        || body.policy_id.is_empty()
        || body.resource_name.is_empty()
        || body.doc_id.is_empty()
    {
        return Err(HttpError::BadRequest(
            "missing a required argument needed to decide doc access.".into(),
        ));
    }

    let actor = body
        .actor
        .parse::<identity::Did>()
        .map_err(|e| HttpError::BadRequest(format!("invalid actor DID: {}", e)))?;
    let permission = parse_doc_permission(&body.permission)?;

    let doc_acp = state.require_doc_acp()?;
    let allowed = doc_acp
        .check_doc_access(
            &actor,
            permission,
            &body.policy_id,
            &body.resource_name,
            &body.doc_id,
        )
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(DocDecisionResponse { allowed }))
}

/// Add a document ACP relationship.
///
/// POST /api/v0/acp/document/relationship
///
/// Requires `DacRelationAdd` permission when NAC is enabled.
pub async fn add_doc_relationship(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<DocRelationshipRequest>,
) -> Result<Json<DocRelationshipResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacRelationAdd).await?;

    let requestor = identity
        .did()
        .ok_or_else(|| HttpError::BadRequest("identity required for document ACP".into()))?;

    if body.collection.is_empty() {
        return Err(HttpError::BadRequest(
            "collection name can't be empty".into(),
        ));
    }
    if body.target_actor.is_empty() || body.doc_id.is_empty() || body.relation.is_empty() {
        return Err(HttpError::BadRequest(
            "missing a required argument needed to add doc actor relationship.".into(),
        ));
    }

    if body.relation == "owner" {
        return Err(HttpError::BadRequest(
            "OPERATION_FORBIDDEN: cannot add owner relation".into(),
        ));
    }

    let doc_acp = state.require_doc_acp()?;

    let is_new = doc_acp
        .add_doc_relationship(
            requestor,
            &body.target_actor,
            &body.collection,
            &body.doc_id,
            &body.relation,
        )
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(DocRelationshipResponse {
        existed_already: !is_new,
    }))
}

/// Remove a document ACP relationship.
///
/// DELETE /api/v0/acp/document/relationship
///
/// Requires `DacRelationDelete` permission when NAC is enabled.
pub async fn remove_doc_relationship(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<DocRelationshipRequest>,
) -> Result<Json<DocRelationshipResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::DacRelationDelete).await?;

    let requestor = identity
        .did()
        .ok_or_else(|| HttpError::BadRequest("identity required for document ACP".into()))?;

    if body.collection.is_empty() {
        return Err(HttpError::BadRequest(
            "collection name can't be empty".into(),
        ));
    }
    if body.target_actor.is_empty() || body.doc_id.is_empty() || body.relation.is_empty() {
        return Err(HttpError::BadRequest(
            "missing a required argument needed to delete doc actor relationship.".into(),
        ));
    }

    if body.relation == "owner" {
        return Err(HttpError::BadRequest(
            "OPERATION_FORBIDDEN: cannot delete owner relation".into(),
        ));
    }

    let doc_acp = state.require_doc_acp()?;

    let was_removed = doc_acp
        .delete_doc_relationship(
            requestor,
            &body.target_actor,
            &body.collection,
            &body.doc_id,
            &body.relation,
        )
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(DocRelationshipResponse {
        existed_already: !was_removed,
    }))
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

    #[test]
    fn test_doc_decision_request_accepts_camel_and_snake_case() {
        let camel: DocDecisionRequest = serde_json::from_str(
            r#"{
                "actor": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
                "permission": "read",
                "policyID": "policy",
                "resourceName": "resource",
                "docID": "doc"
            }"#,
        )
        .unwrap();
        assert_eq!(camel.policy_id, "policy");
        assert_eq!(camel.resource_name, "resource");
        assert_eq!(camel.doc_id, "doc");

        let snake: DocDecisionRequest = serde_json::from_str(
            r#"{
                "actor": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
                "permission": "delete",
                "policy_id": "policy",
                "resource_name": "resource",
                "doc_id": "doc"
            }"#,
        )
        .unwrap();
        assert_eq!(snake.policy_id, "policy");
        assert_eq!(snake.resource_name, "resource");
        assert_eq!(snake.doc_id, "doc");
    }

    #[test]
    fn test_parse_doc_permission() {
        assert_eq!(
            parse_doc_permission("read").unwrap(),
            acp::DocumentPermission::Read
        );
        assert_eq!(
            parse_doc_permission("update").unwrap(),
            acp::DocumentPermission::Update
        );
        assert_eq!(
            parse_doc_permission("delete").unwrap(),
            acp::DocumentPermission::Delete
        );
        assert!(matches!(
            parse_doc_permission("admin"),
            Err(HttpError::BadRequest(_))
        ));
    }
}
