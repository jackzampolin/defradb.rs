use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::{Address, Bytes, B256};

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const RECEIPT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);
const RECEIPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub struct HubRsClient {
    url: String,
    http: reqwest::Client,
    next_id: AtomicU64,
}

impl HubRsClient {
    pub fn new(url: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            url,
            http,
            next_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn chain_id(&self) -> Result<u64, ClientError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": "eth_chainId",
            "params": []
        });
        let resp: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        check_rpc_error(&resp)?;
        let hex_str = resp["result"]
            .as_str()
            .ok_or_else(|| ClientError::Rpc("missing result in eth_chainId".into()))?;
        parse_hex_u64(hex_str)
    }

    pub async fn get_nonce(&self, address: Address) -> Result<u64, ClientError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": "eth_getTransactionCount",
            "params": [format!("{:?}", address), "latest"]
        });
        let resp: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        check_rpc_error(&resp)?;
        let hex_str = resp["result"]
            .as_str()
            .ok_or_else(|| ClientError::Rpc("missing result in eth_getTransactionCount".into()))?;
        parse_hex_u64(hex_str)
    }

    pub async fn eth_call(&self, to: Address, data: Bytes) -> Result<Bytes, ClientError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": "eth_call",
            "params": [{
                "to": format!("{:?}", to),
                "data": format!("0x{}", hex::encode(&data)),
            }, "latest"]
        });
        let resp: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        check_rpc_error(&resp)?;
        let hex_str = resp["result"]
            .as_str()
            .ok_or_else(|| ClientError::Rpc("missing result in eth_call".into()))?;
        let bytes = hex::decode(hex_str.strip_prefix("0x").unwrap_or(hex_str))
            .map_err(|e| ClientError::Rpc(format!("hex decode: {}", e)))?;
        Ok(Bytes::from(bytes))
    }

    pub async fn send_raw_transaction(&self, raw: Bytes) -> Result<B256, ClientError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(&raw))]
        });
        let resp: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        check_rpc_error(&resp)?;
        let hash_str = resp["result"]
            .as_str()
            .ok_or_else(|| ClientError::Rpc("missing result in eth_sendRawTransaction".into()))?;
        let bytes = hex::decode(hash_str.strip_prefix("0x").unwrap_or(hash_str))
            .map_err(|e| ClientError::Rpc(format!("tx hash hex decode: {}", e)))?;
        if bytes.len() != 32 {
            return Err(ClientError::Rpc(format!(
                "unexpected tx hash length: {}",
                bytes.len()
            )));
        }
        Ok(B256::from_slice(&bytes))
    }

    pub async fn wait_for_receipt(&self, tx_hash: B256) -> Result<serde_json::Value, ClientError> {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > RECEIPT_TIMEOUT {
                return Err(ClientError::Timeout(format!(
                    "receipt timeout for {:?}",
                    tx_hash
                )));
            }
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": self.next_id(),
                "method": "eth_getTransactionReceipt",
                "params": [format!("0x{}", hex::encode(tx_hash))]
            });
            let resp: serde_json::Value = self
                .http
                .post(&self.url)
                .json(&body)
                .send()
                .await?
                .json()
                .await?;
            check_rpc_error(&resp)?;
            if !resp["result"].is_null() {
                let status = resp["result"]["status"].as_str().unwrap_or("0x0");
                if status != "0x1" {
                    return Err(ClientError::TxReverted(format!(
                        "tx {:?} reverted (status {})",
                        tx_hash, status
                    )));
                }
                return Ok(resp["result"].clone());
            }
            tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
        }
    }
}

fn check_rpc_error(resp: &serde_json::Value) -> Result<(), ClientError> {
    if let Some(err) = resp.get("error") {
        let msg = err["message"].as_str().unwrap_or("unknown RPC error");
        let code = err["code"].as_i64().unwrap_or(0);
        return Err(ClientError::Rpc(format!("code {}: {}", code, msg)));
    }
    Ok(())
}

fn parse_hex_u64(s: &str) -> Result<u64, ClientError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(s, 16).map_err(|e| ClientError::Rpc(format!("parse hex u64: {}", e)))
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("transaction reverted: {0}")]
    TxReverted(String),

    #[error("timeout: {0}")]
    Timeout(String),
}
