//! Adapter to bridge lens transform store to HTTP's LensOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::LensOperations;
use lens::{LensConfig, TransformStore, WasmTransformStore};

/// Adapter that implements LensOperations using WasmTransformStore.
pub struct LensAdapter {
    store: Arc<WasmTransformStore>,
}

impl LensAdapter {
    /// Create a new adapter with its own WASM transform store.
    pub fn new() -> Result<Self, String> {
        let store =
            WasmTransformStore::new().map_err(|e| format!("failed to create WASM store: {}", e))?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc() -> Result<Arc<dyn LensOperations>, String> {
        Ok(Arc::new(Self::new()?))
    }
}

impl Default for LensAdapter {
    fn default() -> Self {
        Self::new().expect("failed to create lens adapter")
    }
}

#[async_trait]
impl LensOperations for LensAdapter {
    async fn set_migration(&self, config: &str) -> Result<String, String> {
        // Parse the JSON configuration
        let lens_config: LensConfig = serde_json::from_str(config)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;

        // Add the transform to the store
        let transform_id = self
            .store
            .add(lens_config)
            .await
            .map_err(|e| format!("failed to set migration: {}", e))?;

        Ok(transform_id.to_string())
    }

    async fn reload(&self) -> Result<(), String> {
        // For now, reload is a no-op as we don't persist transforms.
        // When persistence is implemented, this will reload from disk.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lens_adapter_invalid_config() {
        let adapter = LensAdapter::new().unwrap();
        let result = adapter.set_migration("not valid json").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse lens config"));
    }

    #[tokio::test]
    async fn test_lens_adapter_missing_path() {
        let adapter = LensAdapter::new().unwrap();
        let config = r#"{
            "SourceSchemaVersionID": "v1",
            "DestinationSchemaVersionID": "v2",
            "Lens": {}
        }"#;
        let result = adapter.set_migration(config).await;
        assert!(result.is_err());
        // Should fail because no path or module bytes provided
        assert!(result.unwrap_err().contains("failed to set migration"));
    }

    #[tokio::test]
    async fn test_lens_adapter_reload() {
        let adapter = LensAdapter::new().unwrap();
        let result = adapter.reload().await;
        assert!(result.is_ok());
    }
}
