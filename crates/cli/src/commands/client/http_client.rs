// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! HTTP client for communicating with DefraDB server

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::{Error, Result};

/// Default timeout for HTTP requests (30 seconds)
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default connection timeout (10 seconds)
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Global shared HTTP client for connection reuse across commands
static SHARED_CLIENT: OnceLock<Client> = OnceLock::new();

fn get_shared_client() -> &'static Client {
    SHARED_CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
            .pool_max_idle_per_host(5)
            .build()
            .expect("Failed to build HTTP client")
    })
}

/// HTTP client for DefraDB server communication
pub struct HttpClient {
    client: &'static Client,
    base_url: String,
}

/// GraphQL request body
#[derive(Debug, Serialize)]
pub struct GraphQLRequest {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<JsonValue>,
    #[serde(rename = "operationName", skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_id: Option<String>,
}

/// GraphQL response from server
#[derive(Debug, Deserialize)]
pub struct GraphQLResponse {
    pub data: Option<JsonValue>,
    #[serde(default)]
    pub errors: Vec<GraphQLError>,
}

/// GraphQL error
#[derive(Debug, Deserialize)]
pub struct GraphQLError {
    pub message: String,
}

/// Transaction begin request
#[derive(Debug, Serialize)]
pub struct TxBeginRequest {
    pub readonly: bool,
}

/// Transaction begin response
#[derive(Debug, Deserialize)]
pub struct TxBeginResponse {
    pub txn_id: String,
}

/// Transaction commit/discard request
#[derive(Debug, Serialize)]
pub struct TxRequest {
    pub txn_id: String,
}

/// Transaction success response
#[derive(Debug, Deserialize)]
pub struct TxSuccessResponse {
    pub status: String,
}

/// Server error response
#[derive(Debug, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl HttpClient {
    /// Create a new HTTP client with the given base URL.
    ///
    /// Uses a shared connection pool with configured timeouts for efficiency.
    pub fn new(base_url: &str) -> Self {
        Self {
            client: get_shared_client(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Execute a GraphQL query
    pub async fn graphql(
        &self,
        query: &str,
        variables: Option<JsonValue>,
        txn_id: Option<String>,
    ) -> Result<GraphQLResponse> {
        let request = GraphQLRequest {
            query: query.to_string(),
            variables,
            operation_name: None,
            txn_id,
        };

        let url = format!("{}/api/v0/graphql", self.base_url);
        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Server(format!("HTTP {}: {}", status, body.trim())));
        }

        let result: GraphQLResponse = response.json().await?;
        Ok(result)
    }

    /// Get the GraphQL schema
    pub async fn schema(&self) -> Result<String> {
        let url = format!("{}/api/v0/schema", self.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Server(format!("HTTP {}: {}", status, body.trim())));
        }

        let schema = response.text().await?;
        Ok(schema)
    }

    /// Begin a new transaction
    pub async fn tx_begin(&self, readonly: bool) -> Result<TxBeginResponse> {
        let url = format!("{}/api/v0/tx/begin", self.base_url);
        let request = TxBeginRequest { readonly };
        self.post_json(&url, &request).await
    }

    /// Commit a transaction
    pub async fn tx_commit(&self, txn_id: &str) -> Result<TxSuccessResponse> {
        let url = format!("{}/api/v0/tx/commit", self.base_url);
        let request = TxRequest {
            txn_id: txn_id.to_string(),
        };
        self.post_json(&url, &request).await
    }

    /// Rollback (discard) a transaction
    pub async fn tx_rollback(&self, txn_id: &str) -> Result<TxSuccessResponse> {
        let url = format!("{}/api/v0/tx/rollback", self.base_url);
        let request = TxRequest {
            txn_id: txn_id.to_string(),
        };
        self.post_json(&url, &request).await
    }

    /// Helper for POST requests with JSON body
    async fn post_json<T: DeserializeOwned>(&self, url: &str, body: &impl Serialize) -> Result<T> {
        let response = self.client.post(url).json(body).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = response.text().await.unwrap_or_default();
            if let Ok(err) = serde_json::from_str::<ErrorResponse>(&body_text) {
                return Err(Error::Server(err.error));
            }
            return Err(Error::Server(format!(
                "HTTP {}: {}",
                status,
                body_text.trim()
            )));
        }

        let result: T = response.json().await?;
        Ok(result)
    }
}

impl GraphQLResponse {
    /// Check if the response has errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Format errors as a string
    pub fn error_message(&self) -> String {
        self.errors
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_new() {
        let client = HttpClient::new("http://localhost:9181/");
        assert_eq!(client.base_url, "http://localhost:9181");
    }

    #[test]
    fn test_graphql_request_serialization() {
        let request = GraphQLRequest {
            query: "{ Users { name } }".to_string(),
            variables: None,
            operation_name: None,
            txn_id: None,
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("query"));
        assert!(!json.contains("variables"));
    }

    #[test]
    fn test_graphql_response_has_errors() {
        let response = GraphQLResponse {
            data: None,
            errors: vec![GraphQLError {
                message: "error".to_string(),
            }],
        };
        assert!(response.has_errors());
        assert_eq!(response.error_message(), "error");
    }

    #[test]
    fn test_graphql_response_no_errors() {
        let response = GraphQLResponse {
            data: Some(serde_json::json!({})),
            errors: vec![],
        };
        assert!(!response.has_errors());
    }
}
