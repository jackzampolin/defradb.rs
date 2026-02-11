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
    async fn verify_signature(
        &self,
        _cid: &str,
        _public_key: &str,
        _key_type: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }
}
