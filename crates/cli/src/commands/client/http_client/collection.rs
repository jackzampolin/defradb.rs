//! Collection HTTP client methods

use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

fn collection_patch_body(patch: &str, migration: Option<&str>) -> Result<String> {
    let mut body = serde_json::json!({ "Patch": patch });
    if let Some(migration) = migration {
        body["Migration"] = serde_json::from_str(migration)?;
    }
    Ok(serde_json::to_string(&body)?)
}

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

    pub async fn collection_patch(&self, patch: &str, migration: Option<&str>) -> Result<()> {
        let url = format!("{}/api/v0/collections", self.base_url);
        let body_str = collection_patch_body(patch, migration)?;
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

    /// Delete one or more collections by name via Go-compatible
    /// `DELETE /collections?name=Users,Books&active-only=true`.
    pub async fn collection_delete(&self, names: &[String], active_only: bool) -> Result<()> {
        let joined = names
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let url = format!(
            "{}/api/v0/collections?name={}&active-only={}",
            self.base_url,
            encode(&joined),
            active_only
        );
        self.request_void("DELETE", &url, None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_patch_body_embeds_migration_json() {
        let body = collection_patch_body("[]", Some(r#"{"Lenses":[]}"#)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert!(value["Migration"].is_object());
        assert_eq!(value["Migration"]["Lenses"], serde_json::json!([]));

        let patch_only: serde_json::Value =
            serde_json::from_str(&collection_patch_body("[]", None).unwrap()).unwrap();
        assert!(patch_only.get("Migration").is_none());
    }
}
