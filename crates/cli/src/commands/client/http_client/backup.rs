//! Backup HTTP client methods

use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    pub async fn backup_export(
        &self,
        collections: Option<&[String]>,
        pretty: bool,
    ) -> Result<String> {
        let mut url = format!("{}/api/v0/backup/export", self.base_url);
        let mut params = Vec::new();
        if let Some(cols) = collections {
            for col in cols {
                params.push(format!("collections={}", encode(col)));
            }
        }
        if pretty {
            params.push("pretty=true".to_string());
        }
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }
        self.request_text("GET", &url, None).await
    }

    pub async fn backup_import(&self, data: &str) -> Result<()> {
        let url = format!("{}/api/v0/backup/import", self.base_url);
        self.request_void("POST", &url, Some(data)).await
    }
}
