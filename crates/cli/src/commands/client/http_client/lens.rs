//! Lens HTTP client methods

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::HttpClient;
use crate::error::Result;

/// Response for setting a lens migration
#[derive(Debug, Deserialize, Serialize)]
pub struct LensSetMigrationResponse {
    #[serde(rename = "lensId", alias = "transformId")]
    pub lens_id: String,
}

impl HttpClient {
    pub async fn lens_set_migration(&self, config: &str) -> Result<LensSetMigrationResponse> {
        let url = format!("{}/api/v0/collections/migrations", self.base_url);
        self.request_json("POST", &url, Some(config)).await
    }

    pub async fn lens_add(&self, config: &str) -> Result<LensSetMigrationResponse> {
        let url = format!("{}/api/v0/lens", self.base_url);
        let parsed: JsonValue = serde_json::from_str(config)
            .map_err(|e| crate::error::Error::Server(format!("invalid JSON config: {}", e)))?;
        let wrapped = serde_json::json!({"lens": parsed});
        let body = serde_json::to_string(&wrapped)?;
        self.request_json("POST", &url, Some(&body)).await
    }
}
