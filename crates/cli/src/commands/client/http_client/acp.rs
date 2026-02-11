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
    /// Add a new ACP policy
    pub async fn acp_add_policy(&self, policy: &str) -> Result<AcpAddPolicyResponse> {
        let url = format!("{}/api/v0/acp/policy", self.base_url);
        let request = AcpAddPolicyRequest {
            policy: policy.to_string(),
        };
        self.post_json(&url, &request).await
    }

    /// List all ACP policies
    pub async fn acp_list_policies(&self) -> Result<Vec<AcpPolicy>> {
        let url = format!("{}/api/v0/acp/policy", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let policies: Vec<AcpPolicy> = response.json().await?;
        Ok(policies)
    }

    /// Get a specific ACP policy by ID
    pub async fn acp_get_policy(&self, policy_id: &str) -> Result<AcpPolicy> {
        let url = format!("{}/api/v0/acp/policy/{}", self.base_url, encode(policy_id));
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let policy: AcpPolicy = response.json().await?;
        Ok(policy)
    }

    /// Add a document ACP relationship
    pub async fn acp_doc_relationship_add(
        &self,
        collection: &str,
        doc_id: &str,
        relation: &str,
        actor: &str,
    ) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/document/relationship", self.base_url);
        let body = serde_json::json!({
            "collection": collection,
            "docID": doc_id,
            "relation": relation,
            "actor": actor,
        });
        let body_str = serde_json::to_string(&body)?;
        let response = self.send_with_retry("POST", &url, Some(&body_str)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Remove a document ACP relationship
    pub async fn acp_doc_relationship_delete(
        &self,
        collection: &str,
        doc_id: &str,
        relation: &str,
        actor: &str,
    ) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/document/relationship", self.base_url);
        let body = serde_json::json!({
            "collection": collection,
            "docID": doc_id,
            "relation": relation,
            "actor": actor,
        });
        let body_str = serde_json::to_string(&body)?;
        let response = self
            .send_with_retry("DELETE", &url, Some(&body_str))
            .await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Add a NAC relationship
    pub async fn nac_add_relationship(&self, relation: &str, actor: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/relationship", self.base_url);
        let request = NacRelationshipRequest {
            relation: relation.to_string(),
            actor: actor.to_string(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Remove a NAC relationship
    pub async fn nac_remove_relationship(&self, relation: &str, actor: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/relationship", self.base_url);
        let request = NacRelationshipRequest {
            relation: relation.to_string(),
            actor: actor.to_string(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("DELETE", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Get NAC status
    pub async fn nac_status(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/status", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Disable node ACP
    pub async fn nac_disable(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/disable", self.base_url);
        let response = self.send_with_retry("POST", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Re-enable node ACP
    pub async fn nac_re_enable(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/acp/node/re-enable", self.base_url);
        let response = self.send_with_retry("POST", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }
}
