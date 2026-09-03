/// Client for SourceHub ACP gRPC/REST queries and CometBFT tx broadcast.
///
/// Uses the Cosmos LCD REST API for queries (avoids proto compilation)
/// and CometBFT JSON-RPC for transaction broadcast.
pub(crate) struct SourceHubClient {
    /// LCD/REST base URL derived from gRPC address (same host, port 1317 or LCD port)
    grpc_address: String,
    /// CometBFT RPC address for broadcast_tx_sync
    comet_rpc_address: String,
    /// HTTP client for REST queries (configured with per-request timeout)
    http: reqwest::Client,
}

impl SourceHubClient {
    pub(crate) fn new(
        grpc_address: String,
        comet_rpc_address: String,
        request_timeout: std::time::Duration,
    ) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(ClientError::Http)?;
        Ok(Self {
            grpc_address,
            comet_rpc_address,
            http,
        })
    }

    /// Query a policy by ID.
    pub(crate) async fn query_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<PolicyInfo>, ClientError> {
        let url = format!(
            "{}/sourcenetwork/vera/acp/policy/{}",
            self.rest_base_url(),
            policy_id
        );
        let resp = self.http.get(&url).send().await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            if text.contains("NOT_FOUND") || text.contains("not found") {
                return Ok(None);
            }
            return Err(ClientError::QueryFailed(text));
        }
        let body: serde_json::Value = resp.json().await?;
        let record = body.get("record").unwrap_or(&body);
        let policy = record.get("policy").unwrap_or(&body["policy"]);
        if policy.is_null() {
            return Ok(None);
        }
        Ok(Some(PolicyInfo {
            id: policy_id.to_string(),
            name: policy["name"].as_str().unwrap_or("").to_string(),
            raw_policy: record["raw_policy"].as_str().map(ToOwned::to_owned),
        }))
    }

    /// Query the owner of an object registered under a policy.
    /// Returns (is_registered, owner_did).
    pub(crate) async fn query_object_owner(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(bool, String), ClientError> {
        let url = format!(
            "{}/sourcenetwork/vera/acp/object_owner/{}/{}/{}",
            self.rest_base_url(),
            policy_id,
            resource,
            object_id
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            if text.contains("NOT_FOUND") || text.contains("not found") {
                return Ok((false, String::new()));
            }
            return Err(ClientError::QueryFailed(text));
        }
        let body: serde_json::Value = resp.json().await?;
        let is_registered = body["is_registered"].as_bool().unwrap_or(false);
        let owner_did = body["record"]["relationship"]["subject"]["actor"]["id"]
            .as_str()
            .unwrap_or("")
            .to_string();
        Ok((is_registered, owner_did))
    }

    /// Verify if an actor has access to an object.
    ///
    /// Uses CometBFT ABCI query with protobuf-encoded request since
    /// the REST/LCD endpoint doesn't support repeated nested fields in GET params.
    pub(crate) async fn verify_access(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ClientError> {
        // Protobuf-encode the QueryVerifyAccessRequestRequest
        let request_bytes =
            encode_verify_access_request(policy_id, resource, object_id, permission, actor_did);
        let request_hex = hex::encode(&request_bytes);

        let url = format!(
            "{}/abci_query?path=\"/vera.acp.Query/VerifyAccessRequest\"&data=0x{}",
            self.comet_rpc_base_url(),
            request_hex
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(ClientError::QueryFailed(
                resp.text().await.unwrap_or_default(),
            ));
        }
        let body: serde_json::Value = resp.json().await?;

        // Check ABCI response code.
        // Code 0 means success; any other code means the query itself failed
        // (invalid request, chain error, etc.) — NOT an access denial.
        // Returning Ok(false) here would fail-open on SourceHub errors.
        let abci_code = body["result"]["response"]["code"].as_u64().unwrap_or(0);
        if abci_code != 0 {
            let log = body["result"]["response"]["log"]
                .as_str()
                .unwrap_or("unknown");
            tracing::warn!(abci_code, log, "verify_access ABCI query failed");
            return Err(ClientError::QueryFailed(format!(
                "ABCI code {}: {}",
                abci_code, log
            )));
        }

        // Decode base64-encoded protobuf response
        let result_b64 = body["result"]["response"]["value"].as_str().unwrap_or("");
        if result_b64.is_empty() {
            return Ok(false);
        }
        let result_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, result_b64)
                .map_err(|e| ClientError::QueryFailed(format!("base64 decode: {}", e)))?;

        // QueryVerifyAccessRequestResponse: field 1 (bool) valid
        // Protobuf: tag 0x08 (field 1, varint), value 0x01 (true)
        let valid = result_bytes.len() >= 2 && result_bytes[0] == 0x08 && result_bytes[1] == 0x01;
        tracing::debug!(permission, actor_did, valid, "verify_access result");
        Ok(valid)
    }

    /// Query account number and sequence for transaction signing.
    pub(crate) async fn query_account(&self, address: &str) -> Result<(u64, u64), ClientError> {
        let url = format!(
            "{}/cosmos/auth/v1beta1/accounts/{}",
            self.rest_base_url(),
            address
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(ClientError::QueryFailed(
                resp.text().await.unwrap_or_default(),
            ));
        }
        let body: serde_json::Value = resp.json().await?;
        let account = &body["account"];
        let account_number = account["account_number"]
            .as_str()
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(0);
        let sequence = account["sequence"]
            .as_str()
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(0);
        Ok((account_number, sequence))
    }

    /// Broadcast a signed transaction via CometBFT JSON-RPC.
    /// Returns the tx hash on success.
    pub(crate) async fn broadcast_tx_sync(&self, tx_bytes: &[u8]) -> Result<String, ClientError> {
        let tx_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, tx_bytes);
        let url = self.comet_rpc_base_url();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "broadcast_tx_sync",
            "params": {
                "tx": tx_b64,
            }
        });
        let resp = self.http.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            return Err(ClientError::BroadcastFailed(
                resp.text().await.unwrap_or_default(),
            ));
        }
        let result: serde_json::Value = resp.json().await?;
        let code = result["result"]["code"].as_u64().unwrap_or(1);
        if code != 0 {
            let log = result["result"]["log"].as_str().unwrap_or("unknown error");
            return Err(ClientError::TxFailed(format!("code={}: {}", code, log)));
        }
        let hash = result["result"]["hash"].as_str().unwrap_or("").to_string();
        Ok(hash)
    }

    /// Wait for a transaction to be included in a block.
    /// Returns the full CometBFT tx query response on success.
    pub(crate) async fn await_tx(
        &self,
        tx_hash: &str,
        timeout_ms: u64,
    ) -> Result<serde_json::Value, ClientError> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);
        loop {
            if start.elapsed() > timeout {
                return Err(ClientError::TxTimeout(tx_hash.to_string()));
            }
            let url = format!("{}/tx?hash=0x{}", self.comet_rpc_base_url(), tx_hash);
            let resp = self.http.get(&url).send().await;
            if let Ok(r) = resp {
                if r.status().is_success() {
                    let body: serde_json::Value = r.json().await.unwrap_or(serde_json::Value::Null);
                    let code = body["result"]["tx_result"]["code"].as_u64();
                    if let Some(0) = code {
                        return Ok(body);
                    }
                    if let Some(c) = code {
                        let log = body["result"]["tx_result"]["log"]
                            .as_str()
                            .unwrap_or("unknown");
                        return Err(ClientError::TxFailed(format!(
                            "tx execution failed: code={} log={}",
                            c, log
                        )));
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    pub(crate) fn sequence_cache_key(&self, address: &str) -> String {
        format!("{}::{}", self.comet_rpc_address, address)
    }

    /// Derive REST base URL from gRPC address.
    /// SourceHub exposes REST on port 1317 by default, but in tests it uses
    /// the gRPC port. We use the gRPC address directly since the LCD gateway
    /// runs on the same address in the test environment.
    fn rest_base_url(&self) -> String {
        let addr = &self.grpc_address;
        if addr.starts_with("http") {
            addr.clone()
        } else if let Some(rest) = addr.strip_prefix("tcp://") {
            format!("http://{}", rest)
        } else {
            format!("http://{}", addr)
        }
    }

    fn comet_rpc_base_url(&self) -> String {
        let addr = &self.comet_rpc_address;
        if addr.starts_with("http") {
            addr.clone()
        } else if let Some(rest) = addr.strip_prefix("tcp://") {
            format!("http://{}", rest)
        } else {
            format!("http://{}", addr)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PolicyInfo {
    pub id: String,
    pub name: String,
    pub raw_policy: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientError {
    #[error("HTTP request failed: {0}")]
    Http(reqwest::Error),

    #[error("query failed: {0}")]
    QueryFailed(String),

    #[error("broadcast failed: {0}")]
    BroadcastFailed(String),

    #[error("transaction failed: {0}")]
    TxFailed(String),

    #[error("transaction timeout waiting for hash: {0}")]
    TxTimeout(String),

    #[error("SourceHub unreachable (timeout): {0}")]
    Timeout(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<reqwest::Error> for ClientError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_timeout() || e.is_connect() {
            ClientError::Timeout(e.to_string())
        } else {
            ClientError::Http(e)
        }
    }
}

// === Protobuf encoding helpers for ABCI queries ===

/// Encode QueryVerifyAccessRequestRequest as protobuf bytes.
///
/// Proto definition:
///   message QueryVerifyAccessRequestRequest {
///     string policy_id = 1;
///     AccessRequest access_request = 2;
///   }
///   message AccessRequest { repeated Operation operations = 1; Actor actor = 2; }
///   message Operation { Object object = 1; string permission = 2; }
///   message Object { string resource = 1; string id = 2; }
///   message Actor { string id = 1; }
fn encode_verify_access_request(
    policy_id: &str,
    resource: &str,
    object_id: &str,
    permission: &str,
    actor_did: &str,
) -> Vec<u8> {
    // Build Object { resource, id }
    let mut object_buf = Vec::new();
    pb_string(&mut object_buf, 1, resource);
    pb_string(&mut object_buf, 2, object_id);

    // Build Operation { object, permission }
    let mut operation_buf = Vec::new();
    pb_bytes(&mut operation_buf, 1, &object_buf);
    pb_string(&mut operation_buf, 2, permission);

    // Build Actor { id }
    let mut actor_buf = Vec::new();
    pb_string(&mut actor_buf, 1, actor_did);

    // Build AccessRequest { operations: [operation], actor }
    let mut access_request_buf = Vec::new();
    pb_bytes(&mut access_request_buf, 1, &operation_buf);
    pb_bytes(&mut access_request_buf, 2, &actor_buf);

    // Build QueryVerifyAccessRequestRequest { policy_id, access_request }
    let mut buf = Vec::new();
    pb_string(&mut buf, 1, policy_id);
    pb_bytes(&mut buf, 2, &access_request_buf);

    buf
}

// Minimal protobuf encoding helpers (duplicated from tx.rs to avoid coupling)

fn pb_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn pb_string(buf: &mut Vec<u8>, field_num: u32, value: &str) {
    if value.is_empty() {
        return;
    }
    let tag = (field_num << 3) | 2;
    pb_varint(buf, tag as u64);
    pb_varint(buf, value.len() as u64);
    buf.extend_from_slice(value.as_bytes());
}

fn pb_bytes(buf: &mut Vec<u8>, field_num: u32, value: &[u8]) {
    let tag = (field_num << 3) | 2;
    pb_varint(buf, tag as u64);
    pb_varint(buf, value.len() as u64);
    buf.extend_from_slice(value);
}
