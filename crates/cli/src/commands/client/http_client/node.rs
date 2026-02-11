//! Node HTTP client methods

use serde_json::Value as JsonValue;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    /// Get node identity
    pub async fn node_identity(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/node/identity", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Purge all database data
    pub async fn purge(&self) -> Result<()> {
        let url = format!("{}/api/v0/purge", self.base_url);
        let response = self.send_with_retry("POST", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }
}
