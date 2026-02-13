//! Collection HTTP client methods

use serde_json::Value as JsonValue;
use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    pub async fn collection_doc_ids(&self, name: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections/{}", self.base_url, encode(name));
        self.request_json("GET", &url, None).await
    }

    pub async fn collection_update_doc(&self, name: &str, doc_id: &str, patch: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/{}",
            self.base_url,
            encode(name),
            encode(doc_id)
        );
        self.request_void("PATCH", &url, Some(patch)).await
    }

    pub async fn collection_delete_doc(&self, name: &str, doc_id: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/{}",
            self.base_url,
            encode(name),
            encode(doc_id)
        );
        self.request_void("DELETE", &url, None).await
    }

    pub async fn collection_patch(&self, patch: &str) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections", self.base_url);
        self.request_json("PATCH", &url, Some(patch)).await
    }

    pub async fn collection_set_active(&self, version_id: Option<&str>) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections/set-active", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({ "versionID": version_id }))?;
        self.request_json("POST", &url, Some(&body)).await
    }

    pub async fn collection_truncate(&self, name: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/truncate",
            self.base_url,
            encode(name)
        );
        self.request_void("DELETE", &url, None).await
    }
}
