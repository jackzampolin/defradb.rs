//! GraphQL and schema HTTP client methods

use serde_json::Value as JsonValue;

use super::types::GraphQLRequest;
use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    /// Execute a GraphQL query
    pub async fn graphql(
        &self,
        query: &str,
        variables: Option<JsonValue>,
        txn_id: Option<String>,
    ) -> Result<super::GraphQLResponse> {
        let request = GraphQLRequest {
            query: query.to_string(),
            variables,
            operation_name: None,
            txn_id,
        };

        let url = format!("{}/api/v0/graphql", self.base_url);
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: super::GraphQLResponse = response.json().await?;
        Ok(result)
    }

    pub async fn schema(&self) -> Result<String> {
        let url = format!("{}/api/v0/schema", self.base_url);
        self.request_text("GET", &url, None).await
    }

    /// Add a schema definition (SDL text)
    pub async fn schema_add(&self, sdl: &str, txn_id: Option<&str>) -> Result<JsonValue> {
        let url = format!("{}/api/v0/collections", self.base_url);
        let response = self.post_text(&url, sdl, txn_id).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }
}
