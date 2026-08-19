use serde::Serialize;
use serde_json::Value as JsonValue;

use super::HttpClient;
use crate::error::Result;

/// Go's `addViewRequest` (`http/client.go:259`), field names included.
#[derive(Serialize)]
pub struct AddViewRequest {
    #[serde(rename = "Query")]
    pub query: String,
    #[serde(rename = "SDL")]
    pub sdl: String,
    #[serde(rename = "TransformCID", skip_serializing_if = "Option::is_none")]
    pub transform: Option<String>,
}

#[derive(Serialize)]
struct ViewNamesRequest {
    #[serde(rename = "Names", skip_serializing_if = "Option::is_none")]
    names: Option<Vec<String>>,
}

/// Which views a refresh applies to, mirroring Go's `view refresh` flags.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ViewRefreshSelectors {
    pub name: Option<String>,
    pub collection_id: Option<String>,
    pub version_id: Option<String>,
    pub get_inactive: bool,
}

impl ViewRefreshSelectors {
    /// The query string Go's client sends, or empty when nothing is selected.
    ///
    /// Go omits an unset selector rather than sending it empty, and omits
    /// `get_inactive` entirely when false, so this does the same.
    pub fn query_string(&self) -> String {
        let mut pairs: Vec<(&str, String)> = Vec::new();
        if let Some(name) = &self.name {
            pairs.push(("name", name.clone()));
        }
        if let Some(version_id) = &self.version_id {
            pairs.push(("version_id", version_id.clone()));
        }
        if let Some(collection_id) = &self.collection_id {
            pairs.push(("collection_id", collection_id.clone()));
        }
        if self.get_inactive {
            pairs.push(("get_inactive", "true".to_string()));
        }
        if pairs.is_empty() {
            return String::new();
        }
        let encoded: Vec<String> = pairs
            .into_iter()
            .map(|(key, value)| format!("{key}={}", urlencoding::encode(&value)))
            .collect();
        format!("?{}", encoded.join("&"))
    }
}

impl HttpClient {
    pub async fn view_add(
        &self,
        query: &str,
        sdl: &str,
        transform: Option<&str>,
    ) -> Result<JsonValue> {
        let url = self.view_add_url();
        let body = AddViewRequest {
            query: query.to_string(),
            sdl: sdl.to_string(),
            transform: transform.map(|s| s.to_string()),
        };
        self.post_json(&url, &body).await
    }

    /// Refresh views.
    ///
    /// Go answers with a bare `200` and no body (`http/handler_store.go:450`),
    /// so this must not try to deserialize one: against a Go node the refresh
    /// would succeed and the CLI would then report a parse error. Rust's `{}`
    /// is equally accepted because neither is read.
    pub async fn view_refresh(&self, selectors: &ViewRefreshSelectors) -> Result<()> {
        let url = format!("{}{}", self.view_refresh_url(), selectors.query_string());
        self.request_void("POST", &url, None).await
    }

    pub async fn view_gc(&self, names: Option<Vec<String>>) -> Result<JsonValue> {
        let url = format!("{}/api/v0/views/gc", self.base_url());
        let body = ViewNamesRequest { names };
        self.post_json(&url, &body).await
    }
}
