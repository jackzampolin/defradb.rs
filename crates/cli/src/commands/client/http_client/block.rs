//! Block HTTP client methods

use urlencoding::encode;

use super::HttpClient;
use crate::error::Result;

impl HttpClient {
    /// Verify a block's signature via the HTTP API.
    pub async fn block_verify_signature(
        &self,
        cid: &str,
        public_key: &str,
        key_type: Option<&str>,
    ) -> Result<()> {
        let mut url = format!(
            "{}/api/v0/block/verify-signature?cid={}&public-key={}",
            self.base_url,
            encode(cid),
            encode(public_key)
        );
        if let Some(kt) = key_type {
            url.push_str(&format!("&type={}", encode(kt)));
        }
        self.request_void("GET", &url, None).await
    }
}
