//! HTTP client for communicating with DefraDB server

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value as JsonValue;
use url::Url;

use crate::error::{Error, Result};

mod acp;
mod backup;
mod block;
mod collection;
mod encrypted_index;
mod graphql;
mod index;
mod lens;
mod p2p;
mod transaction;
mod types;

pub use acp::{AcpAddPolicyRequest, AcpAddPolicyResponse, AcpPolicy, NacRelationshipRequest};
pub use encrypted_index::EncryptedIndexInfo;
pub use index::{IndexCreateRequest, IndexFieldInfo, IndexInfo};
pub use lens::LensSetMigrationResponse;
pub use p2p::{
    P2pCollectionRequest, P2pConnectRequest, P2pDocumentRequest, P2pInfo, P2pPeerAddRequest,
    P2pPeerInfo, P2pReplicatorInfo, P2pReplicatorRequest,
};
pub use types::{ErrorResponse, GraphQLError, GraphQLRequest, GraphQLResponse, TxBeginResponse};

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

/// Declares a simple endpoint method with no parameters.
macro_rules! endpoint {
    ($name:ident, GET $path:expr => $ret:ty) => {
        pub async fn $name(&self) -> Result<$ret> {
            let url = format!("{}{}", self.base_url, $path);
            self.request_json("GET", &url, None).await
        }
    };
    ($name:ident, POST $path:expr => $ret:ty) => {
        pub async fn $name(&self) -> Result<$ret> {
            let url = format!("{}{}", self.base_url, $path);
            self.request_json("POST", &url, None).await
        }
    };
    ($name:ident, POST $path:expr) => {
        pub async fn $name(&self) -> Result<()> {
            let url = format!("{}{}", self.base_url, $path);
            self.request_void("POST", &url, None).await
        }
    };
}

/// HTTP client for DefraDB server communication
pub struct HttpClient {
    client: &'static Client,
    base_url: String,
    auth_token: Option<String>,
    verbose: bool,
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
                "PATCH" => {
                    let req = self.add_auth_header(self.client.patch(url));
                    if let Some(b) = body {
                        req.header("Content-Type", "application/json")
                            .body(b.to_string())
                    } else {
                        req
                    }
                }
                "DELETE" => {
                    let req = self.add_auth_header(self.client.delete(url));
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
        Err(last_error.unwrap_or_else(|| {
            Error::Server(format!(
                "{} {} failed after {} retries",
                method, url, MAX_RETRIES
            ))
        }))
    }

    /// Extract error details from a failed response and return an error.
    async fn extract_error(response: Response) -> Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|e| format!("[failed to read body: {}]", e));
        Error::Server(format!("HTTP {}: {}", status, body.trim()))
    }

    /// Helper for POST requests with plain text body
    async fn post_text(&self, url: &str, text: &str) -> Result<Response> {
        let request = self
            .add_auth_header(self.client.post(url))
            .header("Content-Type", "text/plain")
            .body(text.to_string());

        if self.verbose {
            eprintln!(">>> POST {}", url);
            eprintln!(">>> Body: {}", text);
        }

        let response = request.send().await.map_err(Error::HttpRequest)?;

        if self.verbose {
            eprintln!("<<< HTTP {}", response.status());
        }

        Ok(response)
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
            if let Ok(err) = serde_json::from_str::<types::ErrorResponse>(&body_text) {
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

    async fn request_json<T: DeserializeOwned>(
        &self,
        method: &str,
        url: &str,
        body: Option<&str>,
    ) -> Result<T> {
        let response = self.send_with_retry(method, url, body).await?;
        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }
        Ok(response.json().await?)
    }

    async fn request_void(&self, method: &str, url: &str, body: Option<&str>) -> Result<()> {
        let response = self.send_with_retry(method, url, body).await?;
        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }
        Ok(())
    }

    async fn request_text(&self, method: &str, url: &str, body: Option<&str>) -> Result<String> {
        let response = self.send_with_retry(method, url, body).await?;
        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }
        Ok(response.text().await?)
    }

    // P2P
    endpoint!(p2p_info,            GET  "/api/v0/p2p/info"           => Vec<String>);
    endpoint!(p2p_peers_list,      GET  "/api/v0/p2p/peers"          => Vec<P2pPeerInfo>);
    endpoint!(p2p_replicator_list, GET  "/api/v0/p2p/replicator"     => Vec<P2pReplicatorInfo>);
    endpoint!(p2p_collection_list, GET  "/api/v0/p2p/collections"    => Vec<String>);
    endpoint!(p2p_active_peers,    GET  "/api/v0/p2p/active-peers"   => JsonValue);
    endpoint!(p2p_document_list,   GET  "/api/v0/p2p/documents"      => JsonValue);
    // ACP / NAC
    endpoint!(acp_list_policies,   GET  "/api/v0/acp/policy"         => Vec<AcpPolicy>);
    endpoint!(nac_status,          GET  "/api/v0/acp/node/status"    => JsonValue);
    endpoint!(nac_disable,         POST "/api/v0/acp/node/disable"   => JsonValue);
    endpoint!(nac_re_enable,       POST "/api/v0/acp/node/re-enable" => JsonValue);

    // Lens
    endpoint!(lens_list,           GET  "/api/v0/lens"               => JsonValue);
    endpoint!(lens_reload,         POST "/api/v0/lens/reload");

    // Node
    endpoint!(node_identity,       GET  "/api/v0/node/identity"      => JsonValue);
    endpoint!(purge,               POST "/api/v0/purge");
}
