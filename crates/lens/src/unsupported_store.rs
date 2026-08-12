//! Transform store used when the `wasmtime-runtime` feature is off.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    Error, LensConfig, LensDocResultStream, LensDocStream, LensModule, Result, TransformId,
    TransformStore,
};

/// Transform store used when the `wasmtime-runtime` feature is off.
///
/// Registration and execution fail immediately so `set_migration` cannot
/// register a silent identity transform.
pub struct UnsupportedTransformStore;

#[async_trait]
impl TransformStore for UnsupportedTransformStore {
    async fn add(&self, _config: LensConfig) -> Result<TransformId> {
        Err(Error::RuntimeUnavailable)
    }

    async fn add_with_id(&self, _id: TransformId, _config: LensConfig) -> Result<()> {
        Err(Error::RuntimeUnavailable)
    }

    async fn list(&self) -> Result<HashMap<String, LensModule>> {
        Ok(HashMap::new())
    }

    fn transform(&self, _id: &TransformId, _docs: LensDocStream) -> Result<LensDocResultStream> {
        Err(Error::RuntimeUnavailable)
    }

    fn inverse(&self, _id: &TransformId, _docs: LensDocStream) -> Result<LensDocResultStream> {
        Err(Error::RuntimeUnavailable)
    }

    fn has_transform(&self, _id: &TransformId) -> bool {
        false
    }

    async fn remove(&self, _id: &TransformId) -> Result<()> {
        Err(Error::RuntimeUnavailable)
    }
}

#[cfg(all(test, not(feature = "wasmtime-runtime")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn add_returns_runtime_unavailable() {
        let store = UnsupportedTransformStore;
        let config = LensConfig::new("v1", "v2", LensModule::from_path("/path/to/transform.wasm"));
        let result = store.add(config).await;
        assert!(matches!(result, Err(Error::RuntimeUnavailable)));
    }
}
