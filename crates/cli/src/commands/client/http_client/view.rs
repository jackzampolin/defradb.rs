use serde::Serialize;
use serde_json::Value as JsonValue;

use super::HttpClient;
use crate::error::Result;

#[derive(Serialize)]
struct AddViewRequest {
    #[serde(rename = "Query")]
    query: String,
    #[serde(rename = "SDL")]
    sdl: String,
    #[serde(rename = "Transform", skip_serializing_if = "Option::is_none")]
    transform: Option<String>,
}

#[derive(Serialize)]
struct ViewNamesRequest {
    #[serde(rename = "Names", skip_serializing_if = "Option::is_none")]
    names: Option<Vec<String>>,
}

impl HttpClient {
    pub async fn view_add(
        &self,
        query: &str,
        sdl: &str,
        transform: Option<&str>,
    ) -> Result<JsonValue> {
        let url = format!("{}/api/v0/views", self.base_url());
        let body = AddViewRequest {
            query: query.to_string(),
            sdl: sdl.to_string(),
            transform: transform.map(|s| s.to_string()),
        };
        self.post_json(&url, &body).await
    }

    pub async fn view_refresh(&self, names: Option<Vec<String>>) -> Result<JsonValue> {
        let url = format!("{}/api/v0/views/refresh", self.base_url());
        let body = ViewNamesRequest { names };
        self.post_json(&url, &body).await
    }

    pub async fn view_gc(&self, names: Option<Vec<String>>) -> Result<JsonValue> {
        let url = format!("{}/api/v0/views/gc", self.base_url());
        let body = ViewNamesRequest { names };
        self.post_json(&url, &body).await
    }
}
