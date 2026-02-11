//! Transaction HTTP client methods

use super::types::{TxBeginRequest, TxBeginResponse, TxRequest, TxSuccessResponse};
use super::HttpClient;
use crate::error::Result;

impl HttpClient {
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
}
