/// Client for SourceHub ACP gRPC/REST queries and CometBFT tx broadcast.
///
/// Uses the Cosmos LCD REST API for queries (avoids proto compilation)
/// and CometBFT JSON-RPC for transaction broadcast.
pub struct SourceHubClient {
    /// LCD/REST base URL derived from gRPC address (same host, port 1317 or LCD port)
    grpc_address: String,
    /// CometBFT RPC address for broadcast_tx_sync
    comet_rpc_address: String,
    /// HTTP client for REST queries
    http: reqwest::Client,
}

impl SourceHubClient {
    pub fn new(grpc_address: String, comet_rpc_address: String) -> Self {
        Self {
            grpc_address,
            comet_rpc_address,
            http: reqwest::Client::new(),
        }
    }

    /// Query a policy by ID.
    pub async fn query_policy(&self, policy_id: &str) -> Result<Option<PolicyInfo>, ClientError> {
        let url = format!(
            "{}/sourcenetwork/sourcehub/acp/policy/{}",
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
        // Extract policy ID from response
        let record = &body["record"];
        let policy = &record["policy"];
        if policy.is_null() {
            return Ok(None);
        }
        Ok(Some(PolicyInfo {
            id: policy_id.to_string(),
            name: policy["name"].as_str().unwrap_or("").to_string(),
        }))
    }

    /// Query the owner of an object registered under a policy.
    /// Returns (is_registered, owner_did).
    pub async fn query_object_owner(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(bool, String), ClientError> {
        let url = format!(
            "{}/sourcenetwork/sourcehub/acp/object_owner/{}/{}/{}",
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
    pub async fn verify_access(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ClientError> {
        // Use POST for complex query parameters
        let url = format!(
            "{}/sourcenetwork/sourcehub/acp/verify_access_request/{}",
            self.rest_base_url(),
            policy_id
        );
        let body = serde_json::json!({
            "policy_id": policy_id,
            "access_request": {
                "operations": [{
                    "object": {
                        "resource": resource,
                        "id": object_id,
                    },
                    "permission": permission,
                }],
                "actor": {
                    "id": actor_did,
                },
            }
        });
        // The REST API uses GET with query params, but for complex nested objects
        // we encode as query parameters. Let's use the gRPC-gateway JSON approach.
        let resp = self.http.get(&url).json(&body).send().await;
        // Fallback: try ABCI query if REST doesn't work
        match resp {
            Ok(r) if r.status().is_success() => {
                let body: serde_json::Value = r.json().await?;
                Ok(body["valid"].as_bool().unwrap_or(false))
            }
            _ => {
                // Use ABCI query path for complex parameters
                self.verify_access_abci(policy_id, resource, object_id, permission, actor_did)
                    .await
            }
        }
    }

    /// Verify access using ABCI query (for complex parameters).
    async fn verify_access_abci(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ClientError> {
        // Construct the protobuf-JSON encoded query and send via ABCI query
        let query_data = serde_json::json!({
            "policy_id": policy_id,
            "access_request": {
                "operations": [{
                    "object": {
                        "resource": resource,
                        "id": object_id,
                    },
                    "permission": permission,
                }],
                "actor": {
                    "id": actor_did,
                },
            }
        });
        let query_bytes = serde_json::to_vec(&query_data)?;
        let query_hex = hex::encode(&query_bytes);

        let url = format!(
            "{}/abci_query?path=\"/sourcehub.acp.Query/VerifyAccessRequest\"&data=0x{}",
            self.comet_rpc_base_url(),
            query_hex
        );
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(ClientError::QueryFailed(
                resp.text().await.unwrap_or_default(),
            ));
        }
        let body: serde_json::Value = resp.json().await?;
        // ABCI response is base64-encoded protobuf
        let result_b64 = body["result"]["response"]["value"]
            .as_str()
            .unwrap_or("");
        if result_b64.is_empty() {
            return Ok(false);
        }
        // Decode base64 and parse the response
        let result_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            result_b64,
        )
        .map_err(|e| ClientError::QueryFailed(format!("base64 decode: {}", e)))?;
        // The protobuf response has `valid` as field 1 (varint)
        // Simple protobuf parsing: field 1, wire type 0 (varint)
        // byte 0x08 = field 1, wire type 0; byte 0x01 = true
        Ok(result_bytes.len() >= 2 && result_bytes[0] == 0x08 && result_bytes[1] == 0x01)
    }

    /// Query account number and sequence for transaction signing.
    pub async fn query_account(
        &self,
        address: &str,
    ) -> Result<(u64, u64), ClientError> {
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
    pub async fn broadcast_tx_sync(&self, tx_bytes: &[u8]) -> Result<String, ClientError> {
        let tx_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            tx_bytes,
        );
        let url = format!("{}/broadcast_tx_sync", self.comet_rpc_base_url());
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
            let log = result["result"]["log"]
                .as_str()
                .unwrap_or("unknown error");
            return Err(ClientError::TxFailed(format!("code={}: {}", code, log)));
        }
        let hash = result["result"]["hash"]
            .as_str()
            .unwrap_or("")
            .to_string();
        Ok(hash)
    }

    /// Wait for a transaction to be included in a block.
    pub async fn await_tx(&self, tx_hash: &str, timeout_ms: u64) -> Result<(), ClientError> {
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
                    let body: serde_json::Value =
                        r.json().await.unwrap_or(serde_json::Value::Null);
                    // Check tx_result.code == 0
                    let code = body["result"]["tx_result"]["code"].as_u64();
                    if let Some(0) = code {
                        return Ok(());
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
pub struct PolicyInfo {
    pub id: String,
    pub name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("query failed: {0}")]
    QueryFailed(String),

    #[error("broadcast failed: {0}")]
    BroadcastFailed(String),

    #[error("transaction failed: {0}")]
    TxFailed(String),

    #[error("transaction timeout waiting for hash: {0}")]
    TxTimeout(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}
