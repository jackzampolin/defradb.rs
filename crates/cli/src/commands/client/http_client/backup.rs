//! Backup HTTP client methods

use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    /// Export database backup
    pub async fn backup_export(
        &self,
        collections: Option<&[String]>,
        pretty: bool,
    ) -> Result<String> {
        let mut url = format!("{}/api/v0/backup/export", self.base_url);

        // Build query parameters with URL encoding
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

        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let data = response.text().await?;
        Ok(data)
    }

    /// Import database backup
    pub async fn backup_import(&self, data: &str) -> Result<()> {
        let url = format!("{}/api/v0/backup/import", self.base_url);
        let response = self.send_with_retry("POST", &url, Some(data)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }
}
