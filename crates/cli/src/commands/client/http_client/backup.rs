//! Backup HTTP client methods

use serde::Serialize;

use super::HttpClient;
use crate::error::Result;

/// Export request body (Go-compatible format).
#[derive(Serialize)]
struct ExportRequest<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    collections: Vec<&'a str>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pretty: bool,
    format: &'a str,
}

impl HttpClient {
    pub async fn backup_export(
        &self,
        collections: Option<&[String]>,
        pretty: bool,
    ) -> Result<String> {
        let url = format!("{}/api/v0/backup/export", self.base_url);
        let cols: Vec<&str> = collections
            .map(|c| c.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        let body = serde_json::to_string(&ExportRequest {
            collections: cols,
            pretty,
            format: "json",
        })?;
        self.request_text("POST", &url, Some(&body)).await
    }

    pub async fn backup_import(&self, data: &str) -> Result<()> {
        let url = format!("{}/api/v0/backup/import", self.base_url);
        self.request_void("POST", &url, Some(data)).await
    }
}
