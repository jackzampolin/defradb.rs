use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Minimal GraphQL client for DefraDB's HTTP API.
pub struct GraphQLClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
struct GraphQLRequest {
    query: String,
}

#[derive(Debug, Deserialize)]
pub struct GraphQLResponse {
    pub data: Option<Value>,
    pub errors: Option<Vec<Value>>,
}

impl GraphQLClient {
    pub fn new(client: Client, base_url: &str) -> Self {
        Self {
            client,
            base_url: base_url.to_string(),
        }
    }

    /// Deploy a schema via `POST /api/v0/schema` with text/plain body.
    pub async fn deploy_schema(&self, sdl: &str) -> Result<Value> {
        let url = format!("{}/api/v0/schema", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "text/plain")
            .body(sdl.to_string())
            .send()
            .await
            .context("schema deploy request failed")?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("failed to read schema response")?;

        anyhow::ensure!(
            status.is_success(),
            "schema deploy failed ({}): {}",
            status,
            body
        );

        serde_json::from_str(&body).context("failed to parse schema response")
    }

    /// Execute a GraphQL query/mutation via `POST /api/v0/graphql`.
    pub async fn query(&self, query: &str) -> Result<GraphQLResponse> {
        let url = format!("{}/api/v0/graphql", self.base_url);
        let req = GraphQLRequest {
            query: query.to_string(),
        };

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .context("graphql request failed")?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("failed to read graphql response")?;

        anyhow::ensure!(
            status.is_success(),
            "graphql request failed ({}): {}",
            status,
            body
        );

        serde_json::from_str(&body).context("failed to parse graphql response")
    }

    /// Execute a query, assert no errors, return the `data` field.
    pub async fn query_ok(&self, query: &str) -> Result<Value> {
        let resp = self.query(query).await?;

        if let Some(errors) = &resp.errors {
            if !errors.is_empty() {
                return Err(anyhow::anyhow!("graphql errors: {:?}", errors));
            }
        }

        resp.data.context("graphql response missing data field")
    }
}
