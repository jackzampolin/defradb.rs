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

use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value as JsonValue;
use url::Url;

use crate::error::{Error, Result};

/// Default timeout for HTTP requests (30 seconds)
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default connection timeout (10 seconds)
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Maximum number of retry attempts for transient failures
const MAX_RETRIES: u32 = 3;

/// Initial backoff delay in milliseconds
const INITIAL_BACKOFF_MS: u64 = 100;

/// HTTP status codes that are considered retryable
const RETRYABLE_STATUS_CODES: &[u16] = &[408, 429, 500, 502, 503, 504];

/// Global shared HTTP client for connection reuse across commands
static SHARED_CLIENT: OnceLock<std::result::Result<Client, String>> = OnceLock::new();

fn get_shared_client() -> Result<&'static Client> {
    let result = SHARED_CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS))
            .pool_max_idle_per_host(5)
            .build()
            .map_err(|e| format!("{}", e))
    });

    match result {
        Ok(client) => Ok(client),
        Err(msg) => Err(Error::HttpClientInit(msg.clone())),
    }
}

/// HTTP client for DefraDB server communication
pub struct HttpClient {
    client: &'static Client,
    base_url: String,
    auth_token: Option<String>,
    verbose: bool,
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
    /// Returns an error if the URL is invalid or the HTTP client fails to initialize.
    pub fn new(base_url: &str) -> Result<Self> {
        let normalized_url = base_url.trim_end_matches('/');

        // Validate the URL format
        Url::parse(normalized_url)
            .map_err(|e| Error::InvalidUrl(normalized_url.to_string(), e.to_string()))?;

        Ok(Self {
            client: get_shared_client()?,
            base_url: normalized_url.to_string(),
            auth_token: None,
            verbose: false,
        })
    }

    /// Set the authentication token (JWT Bearer token) for ACP-protected operations
    pub fn with_auth_token(mut self, token: Option<String>) -> Self {
        self.auth_token = token;
        self
    }

    /// Enable verbose mode to print request/response details
    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Add authorization header if auth_token is set
    fn add_auth_header(&self, request: RequestBuilder) -> RequestBuilder {
        if let Some(ref token) = self.auth_token {
            request.header("Authorization", format!("Bearer {}", token))
        } else {
            request
        }
    }

    /// Check if a status code is retryable
    fn is_retryable_status(status: StatusCode) -> bool {
        RETRYABLE_STATUS_CODES.contains(&status.as_u16())
    }

    /// Check if an error is retryable (connection errors, timeouts)
    fn is_retryable_error(error: &reqwest::Error) -> bool {
        error.is_connect() || error.is_timeout()
    }

    /// Execute a request with retry logic for transient failures
    async fn send_with_retry(
        &self,
        method: &str,
        url: &str,
        body: Option<&str>,
    ) -> Result<Response> {
        let mut last_error = None;
        let mut backoff_ms = INITIAL_BACKOFF_MS;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                if self.verbose {
                    eprintln!("Retry attempt {} after {}ms delay...", attempt, backoff_ms);
                }
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2; // Exponential backoff
            }

            if self.verbose {
                eprintln!(">>> {} {}", method, url);
                if let Some(b) = body {
                    eprintln!(">>> Body: {}", b);
                }
            }

            let request = match method {
                "GET" => self.add_auth_header(self.client.get(url)),
                "POST" => {
                    let req = self.add_auth_header(self.client.post(url));
                    if let Some(b) = body {
                        req.header("Content-Type", "application/json")
                            .body(b.to_string())
                    } else {
                        req
                    }
                }
                _ => {
                    return Err(Error::Server(format!(
                        "Unsupported HTTP method: {}",
                        method
                    )))
                }
            };

            match request.send().await {
                Ok(response) => {
                    if self.verbose {
                        eprintln!("<<< HTTP {}", response.status());
                    }

                    // Don't retry successful responses or client errors (4xx except specific ones)
                    if response.status().is_success()
                        || (response.status().is_client_error()
                            && !Self::is_retryable_status(response.status()))
                    {
                        return Ok(response);
                    }

                    // Check if this status is retryable
                    if Self::is_retryable_status(response.status()) && attempt < MAX_RETRIES {
                        if self.verbose {
                            eprintln!("Retryable status code: {}", response.status());
                        }
                        last_error = Some(Error::Server(format!("HTTP {}", response.status())));
                        continue;
                    }

                    return Ok(response);
                }
                Err(e) => {
                    if self.verbose {
                        eprintln!("<<< Error: {}", e);
                    }

                    // Only retry on transient errors
                    if Self::is_retryable_error(&e) && attempt < MAX_RETRIES {
                        if self.verbose {
                            eprintln!("Retryable error, will retry...");
                        }
                        last_error = Some(Error::HttpRequest(e));
                        continue;
                    }

                    return Err(Error::HttpRequest(e));
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| Error::Server("Request failed after retries".to_string())))
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
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = match response.text().await {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("Warning: Failed to read error response body: {}", e);
                    String::new()
                }
            };
            return Err(Error::Server(format!("HTTP {}: {}", status, body.trim())));
        }

        let result: GraphQLResponse = response.json().await?;
        Ok(result)
    }

    /// Get the GraphQL schema
    pub async fn schema(&self) -> Result<String> {
        let url = format!("{}/api/v0/schema", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = match response.text().await {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("Warning: Failed to read error response body: {}", e);
                    String::new()
                }
            };
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
        let body_str = serde_json::to_string(body)?;
        let response = self.send_with_retry("POST", url, Some(&body_str)).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = match response.text().await {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("Warning: Failed to read error response body: {}", e);
                    String::new()
                }
            };
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
        let client = HttpClient::new("http://localhost:9181/").unwrap();
        assert_eq!(client.base_url, "http://localhost:9181");
    }

    #[test]
    fn test_http_client_new_invalid_url() {
        let result = HttpClient::new("not-a-valid-url");
        assert!(result.is_err());
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
