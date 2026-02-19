//! Encrypted index HTTP client methods

use serde::{Deserialize, Serialize};
use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

/// Encrypted index info from list/add
#[derive(Debug, Deserialize, Serialize)]
pub struct EncryptedIndexInfo {
    #[serde(rename = "FieldName")]
    pub field_name: String,
    #[serde(rename = "Type")]
    pub index_type: String,
}

impl HttpClient {
    /// Add an encrypted index on a collection field.
    pub async fn encrypted_index_add(
        &self,
        collection: &str,
        field: &str,
    ) -> Result<EncryptedIndexInfo> {
        let url = format!(
            "{}/api/v0/collections/{}/encrypted-indexes",
            self.base_url,
            encode(collection)
        );
        let body = serde_json::to_string(&serde_json::json!({ "FieldName": field }))
            .map_err(|e| crate::error::Error::Server(e.to_string()))?;
        self.request_json("POST", &url, Some(&body)).await
    }

    /// List encrypted indexes for a collection.
    pub async fn encrypted_index_list(&self, collection: &str) -> Result<Vec<EncryptedIndexInfo>> {
        let url = format!(
            "{}/api/v0/collections/{}/encrypted-indexes",
            self.base_url,
            encode(collection)
        );
        self.request_json("GET", &url, None).await
    }

    /// Delete an encrypted index from a collection field.
    pub async fn encrypted_index_delete(&self, collection: &str, field: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/encrypted-indexes/{}",
            self.base_url,
            encode(collection),
            encode(field)
        );
        self.request_void("DELETE", &url, None).await
    }
}
