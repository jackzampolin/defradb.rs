//! Index HTTP client methods

use serde::{Deserialize, Serialize};
use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

/// Index create request
#[derive(Debug, Serialize)]
pub struct IndexCreateRequest {
    pub collection: String,
    pub fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub unique: bool,
}

/// Index info from list/create
#[derive(Debug, Deserialize, Serialize)]
pub struct IndexInfo {
    /// Index name
    #[serde(rename = "name", alias = "Name")]
    pub name: String,

    /// Collection name
    #[serde(rename = "collection", alias = "Collection")]
    pub collection: String,

    /// Fields in the index
    #[serde(rename = "fields", alias = "Fields")]
    pub fields: Vec<IndexFieldInfo>,

    /// Whether the index is unique
    #[serde(rename = "unique", alias = "Unique", default)]
    pub unique: bool,
}

/// Index field info
#[derive(Debug, Deserialize, Serialize)]
pub struct IndexFieldInfo {
    /// Field name
    #[serde(rename = "name", alias = "Name")]
    pub name: String,

    /// Sort direction (ASC or DESC)
    #[serde(rename = "direction", alias = "Direction", default)]
    pub direction: Option<String>,
}

impl HttpClient {
    pub async fn index_create(
        &self,
        collection: &str,
        fields: &[String],
        name: Option<&str>,
        unique: bool,
    ) -> Result<IndexInfo> {
        let url = format!("{}/api/v0/index", self.base_url);
        let request = IndexCreateRequest {
            collection: collection.to_string(),
            fields: fields.to_vec(),
            name: name.map(|s| s.to_string()),
            unique,
        };
        self.post_json(&url, &request).await
    }

    pub async fn index_list(&self, collection: Option<&str>) -> Result<Vec<IndexInfo>> {
        let url = match collection {
            Some(col) => format!("{}/api/v0/index?collection={}", self.base_url, encode(col)),
            None => format!("{}/api/v0/index", self.base_url),
        };
        self.request_json("GET", &url, None).await
    }

    pub async fn index_delete(&self, collection: &str, name: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/index?collection={}&name={}",
            self.base_url,
            encode(collection),
            encode(name)
        );
        self.request_void("DELETE", &url, None).await
    }
}
