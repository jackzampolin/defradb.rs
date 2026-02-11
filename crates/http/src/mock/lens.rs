//! Mock lens operations for testing lens handlers.

use async_trait::async_trait;
use serde_json::json;

use crate::router::LensOperations;

/// Mock lens operations for testing lens handlers.
#[derive(Debug, Clone, Default)]
pub struct MockLensOperations;

impl MockLensOperations {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl LensOperations for MockLensOperations {
    async fn set_migration(&self, _config: &str) -> Result<String, String> {
        Ok("mock-transform-001".to_string())
    }

    async fn reload(&self) -> Result<(), String> {
        Ok(())
    }

    async fn add(&self, _config: &str) -> Result<String, String> {
        Ok("mock-transform-002".to_string())
    }

    async fn list(&self) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }
}
