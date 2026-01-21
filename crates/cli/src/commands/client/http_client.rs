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
use urlencoding::encode;

use crate::error::{Error, Result};

/// Default timeout for HTTP requests (30 seconds)
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Default connection timeout (10 seconds)
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Maximum number of retry attempts for transient failures
pub const MAX_RETRIES: u32 = 3;

/// Initial backoff delay in milliseconds
pub const INITIAL_BACKOFF_MS: u64 = 100;

/// HTTP status codes that are considered retryable
pub const RETRYABLE_STATUS_CODES: &[u16] = &[408, 429, 500, 502, 503, 504];

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
    pub fn is_retryable_status(status: StatusCode) -> bool {
        RETRYABLE_STATUS_CODES.contains(&status.as_u16())
    }

    /// Get the base URL for the client
    pub fn base_url(&self) -> &str {
        &self.base_url
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
                // Always log retries so users know about transient failures
                eprintln!(
                    "Warning: Request failed, retry attempt {}/{} after {}ms delay...",
                    attempt, MAX_RETRIES, backoff_ms
                );
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
                "DELETE" => self.add_auth_header(self.client.delete(url)),
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
        Err(last_error.unwrap_or_else(|| {
            Error::Server(format!(
                "{} {} failed after {} retries",
                method, url, MAX_RETRIES
            ))
        }))
    }

    /// Extract error details from a failed response and return an error.
    ///
    /// This helper reduces code duplication for error handling across HTTP client methods.
    async fn extract_error(response: Response) -> Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|e| format!("[failed to read body: {}]", e));
        Error::Server(format!("HTTP {}: {}", status, body.trim()))
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
            return Err(Self::extract_error(response).await);
        }

        let result: GraphQLResponse = response.json().await?;
        Ok(result)
    }

    /// Get the GraphQL schema
    pub async fn schema(&self) -> Result<String> {
        let url = format!("{}/api/v0/schema", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
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
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|e| format!("[failed to read body: {}]", e));
            if let Ok(err) = serde_json::from_str::<ErrorResponse>(&body_text) {
                return Err(Error::Server(err.error));
            }
            return Err(Error::Server(format!("HTTP {}: {}", status, body_text.trim())));
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

/// ACP policy add request
#[derive(Debug, Serialize)]
pub struct AcpAddPolicyRequest {
    pub policy: String,
}

/// ACP policy add response
#[derive(Debug, Deserialize, Serialize)]
pub struct AcpAddPolicyResponse {
    #[serde(rename = "PolicyID")]
    pub policy_id: String,
}

/// ACP policy info from list/describe
#[derive(Debug, Deserialize, Serialize)]
pub struct AcpPolicy {
    /// Policy ID
    #[serde(rename = "id", alias = "ID")]
    pub id: String,

    /// Policy name (if available)
    #[serde(rename = "name", alias = "Name", default)]
    pub name: Option<String>,

    /// Policy description (if available)
    #[serde(rename = "description", alias = "Description", default)]
    pub description: Option<String>,

    /// Resources defined in the policy
    #[serde(rename = "resources", alias = "Resources", default)]
    pub resources: Option<JsonValue>,

    /// Actor definitions
    #[serde(rename = "actor", alias = "Actor", default)]
    pub actor: Option<JsonValue>,

    /// Creation time (if available)
    #[serde(rename = "creationTime", alias = "CreationTime", default)]
    pub creation_time: Option<String>,
}

impl HttpClient {
    /// Add a new ACP policy
    pub async fn acp_add_policy(&self, policy: &str) -> Result<AcpAddPolicyResponse> {
        let url = format!("{}/api/v0/acp/policy", self.base_url);
        let request = AcpAddPolicyRequest {
            policy: policy.to_string(),
        };
        self.post_json(&url, &request).await
    }

    /// List all ACP policies
    pub async fn acp_list_policies(&self) -> Result<Vec<AcpPolicy>> {
        let url = format!("{}/api/v0/acp/policy", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let policies: Vec<AcpPolicy> = response.json().await?;
        Ok(policies)
    }

    /// Get a specific ACP policy by ID
    pub async fn acp_get_policy(&self, policy_id: &str) -> Result<AcpPolicy> {
        // URL-encode the policy ID to handle special characters safely
        let url = format!("{}/api/v0/acp/policy/{}", self.base_url, encode(policy_id));
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let policy: AcpPolicy = response.json().await?;
        Ok(policy)
    }

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

    /// Create an index on a collection
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

    /// List indexes (optionally filtered by collection)
    pub async fn index_list(&self, collection: Option<&str>) -> Result<Vec<IndexInfo>> {
        let url = match collection {
            Some(col) => format!("{}/api/v0/index?collection={}", self.base_url, encode(col)),
            None => format!("{}/api/v0/index", self.base_url),
        };
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let indexes: Vec<IndexInfo> = response.json().await?;
        Ok(indexes)
    }

    /// Drop an index by name
    pub async fn index_drop(&self, collection: &str, name: &str) -> Result<()> {
        let url = format!(
            "{}/api/v0/index?collection={}&name={}",
            self.base_url,
            encode(collection),
            encode(name)
        );
        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }
}

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

/// P2P node info
#[derive(Debug, Deserialize, Serialize)]
pub struct P2pInfo {
    /// Peer ID
    #[serde(rename = "id", alias = "ID", alias = "peerId", alias = "PeerID")]
    pub id: String,

    /// Listen addresses
    #[serde(rename = "addresses", alias = "Addresses", default)]
    pub addresses: Vec<String>,
}

/// P2P peer info
#[derive(Debug, Deserialize, Serialize)]
pub struct P2pPeerInfo {
    /// Peer ID
    #[serde(rename = "id", alias = "ID")]
    pub id: String,

    /// Peer address
    #[serde(rename = "address", alias = "Address", default)]
    pub address: Option<String>,
}

/// P2P replicator info
#[derive(Debug, Deserialize, Serialize)]
pub struct P2pReplicatorInfo {
    /// Peer ID
    #[serde(rename = "id", alias = "ID", default)]
    pub id: Option<String>,

    /// Collections being replicated
    #[serde(rename = "collections", alias = "Collections", default)]
    pub collections: Vec<String>,

    /// Peer address
    #[serde(rename = "address", alias = "Address", default)]
    pub address: Option<String>,
}

/// P2P replicator request
#[derive(Debug, Serialize)]
pub struct P2pReplicatorRequest {
    pub collections: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

/// P2P collection request
#[derive(Debug, Serialize)]
pub struct P2pCollectionRequest {
    pub collections: Vec<String>,
}

/// P2P peer add request
#[derive(Debug, Serialize)]
pub struct P2pPeerAddRequest {
    pub address: String,
}

impl HttpClient {
    /// Get P2P node info
    pub async fn p2p_info(&self) -> Result<P2pInfo> {
        let url = format!("{}/api/v0/p2p/info", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let info: P2pInfo = response.json().await?;
        Ok(info)
    }

    /// List connected peers
    pub async fn p2p_peers_list(&self) -> Result<Vec<P2pPeerInfo>> {
        let url = format!("{}/api/v0/p2p/peers", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let peers: Vec<P2pPeerInfo> = response.json().await?;
        Ok(peers)
    }

    /// Connect to a peer
    pub async fn p2p_peers_add(&self, address: &str) -> Result<()> {
        let url = format!("{}/api/v0/p2p/peers", self.base_url);
        let request = P2pPeerAddRequest {
            address: address.to_string(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// List replicators
    pub async fn p2p_replicator_list(&self) -> Result<Vec<P2pReplicatorInfo>> {
        let url = format!("{}/api/v0/p2p/replicator", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let replicators: Vec<P2pReplicatorInfo> = response.json().await?;
        Ok(replicators)
    }

    /// Add a replicator
    pub async fn p2p_replicator_add(
        &self,
        collections: &[String],
        address: Option<&str>,
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/replicator", self.base_url);
        let request = P2pReplicatorRequest {
            collections: collections.to_vec(),
            address: address.map(|s| s.to_string()),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Delete a replicator
    pub async fn p2p_replicator_delete(
        &self,
        collections: &[String],
        address: Option<&str>,
    ) -> Result<()> {
        let mut url = format!("{}/api/v0/p2p/replicator", self.base_url);

        // Build query parameters with URL encoding
        let mut params = Vec::new();
        for col in collections {
            params.push(format!("collections={}", encode(col)));
        }
        if let Some(addr) = address {
            params.push(format!("address={}", encode(addr)));
        }
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }

        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// List P2P collections
    pub async fn p2p_collection_list(&self) -> Result<Vec<String>> {
        let url = format!("{}/api/v0/p2p/collections", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let collections: Vec<String> = response.json().await?;
        Ok(collections)
    }

    /// Add collections to P2P
    pub async fn p2p_collection_add(&self, collections: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections", self.base_url);
        let request = P2pCollectionRequest {
            collections: collections.to_vec(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Remove collections from P2P
    pub async fn p2p_collection_remove(&self, collections: &[String]) -> Result<()> {
        let mut url = format!("{}/api/v0/p2p/collections", self.base_url);

        // Build query parameters with URL encoding
        let params: Vec<String> = collections
            .iter()
            .map(|c| format!("collections={}", encode(c)))
            .collect();
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }

        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }
}
