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
    pub async fn lens_set_migration(&self, config: &str) -> Result<LensSetMigrationResponse> {
        let url = format!("{}/api/v0/lens/set", self.base_url);
        self.request_json("POST", &url, Some(config)).await
    }

    pub async fn lens_add(&self, config: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/lens", self.base_url);
        self.request_json("POST", &url, Some(config)).await
    }
}
