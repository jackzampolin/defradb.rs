//! Lens HTTP client methods

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::HttpClient;
use crate::error::Result;

/// Response for setting a lens migration
#[derive(Debug, Deserialize, Serialize)]
pub struct LensSetMigrationResponse {
    /// The transform ID assigned to this migration
    #[serde(rename = "transformId", alias = "TransformID", alias = "transform_id")]
    pub transform_id: String,
}

impl HttpClient {
    /// Set a lens migration
    pub async fn lens_set_migration(&self, config: &str) -> Result<LensSetMigrationResponse> {
        let url = format!("{}/api/v0/lens/set", self.base_url);
        let response = self.send_with_retry("POST", &url, Some(config)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: LensSetMigrationResponse = response.json().await?;
        Ok(result)
    }

    /// Reload all lens modules
    pub async fn lens_reload(&self) -> Result<()> {
        let url = format!("{}/api/v0/lens/reload", self.base_url);
        let response = self.send_with_retry("POST", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Add a lens migration
    pub async fn lens_add(&self, config: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/lens", self.base_url);
        let response = self.send_with_retry("POST", &url, Some(config)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// List lens migrations
    pub async fn lens_list(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/lens", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }
}
