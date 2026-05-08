//! Collection HTTP client methods

use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    pub async fn collection_update_doc(&self, name: &str, doc_id: &str, patch: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/document/{}",
            self.base_url,
            encode(name),
            encode(doc_id)
        );
        self.request_void("PATCH", &url, Some(patch)).await
    }

    pub async fn collection_delete_doc(&self, name: &str, doc_id: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/document/{}",
            self.base_url,
            encode(name),
            encode(doc_id)
        );
        self.request_void("DELETE", &url, None).await
    }

    pub async fn collection_patch(&self, patch: &str) -> Result<()> {
        let url = format!("{}/api/v0/collections", self.base_url);
        let body = serde_json::json!({ "Patch": patch });
        let body_str = serde_json::to_string(&body)?;
        self.request_void("PATCH", &url, Some(&body_str)).await
    }

    pub async fn collection_set_active(&self, version_id: Option<&str>) -> Result<()> {
        let url = format!("{}/api/v0/collections/default", self.base_url);
        let text = version_id.unwrap_or("");
        let response = self.post_text(&url, text, None).await?;
        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }
        Ok(())
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
