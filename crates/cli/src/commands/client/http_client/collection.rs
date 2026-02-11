//! Collection HTTP client methods

use serde_json::Value as JsonValue;
use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    /// Get document IDs from a collection
    pub async fn collection_doc_ids(&self, name: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections/{}", self.base_url, encode(name));
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Update a document by ID
    pub async fn collection_update_doc(
        &self,
        name: &str,
        doc_id: &str,
        patch: &str,
    ) -> Result<JsonValue> {
        let url = format!(
            "{}/api/v0/collections/{}/{}",
            self.base_url,
            encode(name),
            encode(doc_id)
        );
        let response = self.send_with_retry("PATCH", &url, Some(patch)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Delete a document by ID
    pub async fn collection_delete_doc(&self, name: &str, doc_id: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/{}",
            self.base_url,
            encode(name),
            encode(doc_id)
        );
        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Patch a collection schema
    pub async fn collection_patch(&self, patch: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections", self.base_url);
        let response = self.send_with_retry("PATCH", &url, Some(patch)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Set the active collection version
    pub async fn collection_set_active(&self, version_id: Option<&str>) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections/set-active", self.base_url);
        let body = serde_json::json!({ "versionID": version_id });
        let body_str = serde_json::to_string(&body)?;
        let response = self.send_with_retry("POST", &url, Some(&body_str)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Truncate all documents in a collection
    pub async fn collection_truncate(&self, name: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/truncate",
            self.base_url,
            encode(name)
        );
        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }
}
