use async_trait::async_trait;

use crate::router::BlockOperations;

/// Mock block operations for testing.
#[derive(Debug, Clone, Default)]
pub struct MockBlockOperations;

impl MockBlockOperations {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl BlockOperations for MockBlockOperations {
    async fn signed_block_bytes(
        &self,
        _cid: &str,
        _caller_did: Option<&str>,
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        Err("mock signed block bytes unavailable".to_string())
    }

    async fn verify_signature(
        &self,
        _cid: &str,
        _public_key: &str,
        _key_type: Option<&str>,
        _caller_did: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }
}
