use serde_json::Value as JsonValue;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    pub async fn dump(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/debug/dump", self.base_url());
        self.request_json("GET", &url, None).await
    }
}
