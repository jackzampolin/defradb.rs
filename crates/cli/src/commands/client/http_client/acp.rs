//! ACP and NAC HTTP client methods

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

/// ACP policy add request
#[derive(Debug, Serialize)]
pub struct AcpAddPolicyRequest {
    pub policy: String,
}

/// ACP policy add response
#[derive(Debug, Deserialize, Serialize)]
pub struct AcpAddPolicyResponse {
    #[serde(rename = "PolicyID")]
    pub policy_id: String,
}

/// ACP policy info from list/describe
#[derive(Debug, Deserialize, Serialize)]
pub struct AcpPolicy {
    /// Policy ID
    #[serde(rename = "id", alias = "ID")]
    pub id: String,

    /// Policy name (if available)
    #[serde(rename = "name", alias = "Name", default)]
    pub name: Option<String>,

    /// Policy description (if available)
    #[serde(rename = "description", alias = "Description", default)]
    pub description: Option<String>,

    /// Resources defined in the policy
    #[serde(rename = "resources", alias = "Resources", default)]
    pub resources: Option<JsonValue>,

    /// Actor definitions
    #[serde(rename = "actor", alias = "Actor", default)]
    pub actor: Option<JsonValue>,

    /// Creation time (if available)
    #[serde(rename = "creationTime", alias = "CreationTime", default)]
    pub creation_time: Option<String>,
}

/// NAC relationship request
#[derive(Debug, Serialize)]
pub struct NacRelationshipRequest {
    pub relation: String,
    pub actor: String,
}

impl HttpClient {
    pub async fn acp_add_policy(&self, policy: &str) -> Result<AcpAddPolicyResponse> {
        let url = format!("{}/api/v0/acp/policy", self.base_url);
        let response = self.post_text(&url, policy).await?;
        if !response.status().is_success() {
            let status = response.status();
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|e| format!("[failed to read body: {}]", e));
            if let Ok(err) = serde_json::from_str::<super::types::ErrorResponse>(&body_text) {
                return Err(crate::error::Error::Server(err.error));
            }
            return Err(crate::error::Error::Server(format!(
                "HTTP {}: {}",
                status,
                body_text.trim()
            )));
        }
        let result: AcpAddPolicyResponse = response.json().await?;
        Ok(result)
    }

    pub async fn acp_get_policy(&self, policy_id: &str) -> Result<AcpPolicy> {
        let url = format!("{}/api/v0/acp/policy/{}", self.base_url, encode(policy_id));
        self.request_json("GET", &url, None).await
    }

    pub async fn acp_doc_relationship_add(
        &self,
        collection: &str,
        doc_id: &str,
        relation: &str,
        actor: &str,
    ) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/document/relationship", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({
            "collection": collection,
            "docID": doc_id,
            "relation": relation,
            "actor": actor,
        }))?;
        self.request_json("POST", &url, Some(&body)).await
    }

    pub async fn acp_doc_relationship_delete(
        &self,
        collection: &str,
        doc_id: &str,
        relation: &str,
        actor: &str,
    ) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/document/relationship", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({
            "collection": collection,
            "docID": doc_id,
            "relation": relation,
            "actor": actor,
        }))?;
        self.request_json("DELETE", &url, Some(&body)).await
    }

    pub async fn nac_add_relationship(&self, relation: &str, actor: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/relationship", self.base_url);
        let body = serde_json::to_string(&NacRelationshipRequest {
            relation: relation.to_string(),
            actor: actor.to_string(),
        })?;
        self.request_json("POST", &url, Some(&body)).await
    }

    pub async fn nac_remove_relationship(&self, relation: &str, actor: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/relationship", self.base_url);
        let body = serde_json::to_string(&NacRelationshipRequest {
            relation: relation.to_string(),
            actor: actor.to_string(),
        })?;
        self.request_json("DELETE", &url, Some(&body)).await
    }
}
