//! Transaction HTTP client methods

use super::types::TxBeginResponse;
use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    /// Begin a new transaction.
    ///
    /// POST /api/v0/tx?read_only=true
    pub async fn tx_begin(&self, read_only: bool) -> Result<TxBeginResponse> {
        let mut url = format!("{}/api/v0/tx", self.base_url);
        if read_only {
            url.push_str("?read_only=true");
        }
        self.request_json("POST", &url, None).await
    }

    /// Commit a transaction.
    ///
    /// POST /api/v0/tx/{id}
    pub async fn tx_commit(&self, txn_id: &str) -> Result<()> {
        let url = format!("{}/api/v0/tx/{}", self.base_url, txn_id);
        self.request_void("POST", &url, None).await
    }

    /// Discard/rollback a transaction.
    ///
    /// DELETE /api/v0/tx/{id}
    pub async fn tx_rollback(&self, txn_id: &str) -> Result<()> {
        let url = format!("{}/api/v0/tx/{}", self.base_url, txn_id);
        self.request_void("DELETE", &url, None).await
    }
}
