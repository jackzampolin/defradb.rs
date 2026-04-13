use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(not(feature = "native"))]
use lens::MemoryTransformStore;
#[cfg(feature = "native")]
use lens::WasmTransformStore;
use lens::{LensConfig, LensDocResultStream, LensDocStream, TransformId, TransformStore};

use crate::{Error, Result};

/// Transaction-local transform store overlay.
///
/// Reads fall back to the committed global store, while writes remain isolated to
/// the transaction-local store until commit promotion runs.
pub(crate) struct TxnLensStore {
    base: Arc<dyn TransformStore>,
    local: Arc<dyn TransformStore>,
}

impl TxnLensStore {
    pub(crate) fn new(base: Arc<dyn TransformStore>) -> Result<Self> {
        Ok(Self {
            base,
            local: Self::create_local_store()?,
        })
    }

    #[cfg(feature = "native")]
    fn create_local_store() -> Result<Arc<dyn TransformStore>> {
        let store = WasmTransformStore::with_sandbox(Some(lens::WasmSandboxConfig::restrictive()))
            .map_err(|e| Error::Lens(format!("failed to create transaction lens store: {}", e)))?;
        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "native"))]
    fn create_local_store() -> Result<Arc<dyn TransformStore>> {
        Ok(Arc::new(MemoryTransformStore::new()))
    }
}

#[async_trait]
impl TransformStore for TxnLensStore {
    async fn add(&self, config: LensConfig) -> lens::Result<TransformId> {
        self.local.add(config).await
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> lens::Result<()> {
        self.local.add_with_id(id, config).await
    }

    async fn list(&self) -> lens::Result<HashMap<String, lens::LensModule>> {
        let mut result = self.base.list().await?;
        result.extend(self.local.list().await?);
        Ok(result)
    }

    fn transform(
        &self,
        id: &TransformId,
        docs: LensDocStream,
    ) -> lens::Result<LensDocResultStream> {
        if self.local.has_transform(id) {
            self.local.transform(id, docs)
        } else {
            self.base.transform(id, docs)
        }
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> lens::Result<LensDocResultStream> {
        if self.local.has_transform(id) {
            self.local.inverse(id, docs)
        } else {
            self.base.inverse(id, docs)
        }
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.local.has_transform(id) || self.base.has_transform(id)
    }

    async fn remove(&self, id: &TransformId) -> lens::Result<()> {
        if self.local.has_transform(id) {
            self.local.remove(id).await
        } else {
            self.base.remove(id).await
        }
    }
}
