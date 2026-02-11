//! Encrypted index HTTP client methods

use serde::{Deserialize, Serialize};
use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

/// Encrypted index info from list/create (Go-compatible format).
#[derive(Debug, Deserialize, Serialize)]
pub struct EncryptedIndexInfo {
    #[serde(rename = "FieldName")]
    pub field_name: String,
    #[serde(rename = "Type")]
    pub index_type: String,
}

/// Request to create an encrypted index (Go-compatible format).
#[derive(Debug, Serialize)]
struct CreateEncryptedIndexRequest {
    #[serde(rename = "FieldName")]
    field_name: String,
    #[serde(rename = "Type")]
    index_type: String,
}

impl HttpClient {
    pub async fn encrypted_index_create(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<EncryptedIndexInfo> {
        let url = format!(
            "{}/api/v0/collections/{}/encrypted-indexes",
            self.base_url(),
            encode(collection)
        );
        let request = CreateEncryptedIndexRequest {
            field_name: field_name.to_string(),
            index_type: "equality".to_string(),
        };
        self.post_json(&url, &request).await
    }

    pub async fn encrypted_index_list(&self, collection: &str) -> Result<Vec<EncryptedIndexInfo>> {
        let url = format!(
            "{}/api/v0/collections/{}/encrypted-indexes",
            self.base_url(),
            encode(collection)
        );
        self.request_json("GET", &url, None).await
    }

    pub async fn encrypted_index_delete(&self, collection: &str, field_name: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/collections/{}/encrypted-indexes/{}",
            self.base_url(),
            encode(collection),
            encode(field_name)
        );
        self.request_void("DELETE", &url, None).await
    }
}
