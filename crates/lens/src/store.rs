//! Transform store trait and types.
//!
//! Matches Go's lens/host-go/store.Store interface.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::{Error, LensConfig, LensDoc, Result};

/// Unique identifier for a registered transform.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransformId(pub String);

impl TransformId {
    /// Create a new transform ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for TransformId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TransformId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TransformId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Stream of lens documents.
pub type LensDocStream = Pin<Box<dyn Stream<Item = LensDoc> + Send>>;

/// Stream of lens document results.
pub type LensDocResultStream = Pin<Box<dyn Stream<Item = Result<LensDoc>> + Send>>;

/// Store for managing lens transforms.
///
/// Matches Go's lens/host-go/store.Store interface.
#[async_trait]
pub trait TransformStore: Send + Sync {
    /// Register a new lens transform.
    ///
    /// Returns the unique ID for the registered transform.
    async fn add(&self, config: LensConfig) -> Result<TransformId>;

    /// Register a lens transform under a specific ID.
    ///
    /// Used for P2P sync where the transform ID is the Go-generated IPLD CID.
    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> Result<()>;

    /// List all registered transforms.
    ///
    /// Returns a map of transform IDs to their lens modules.
    async fn list(&self) -> Result<std::collections::HashMap<String, crate::LensModule>>;

    /// Transform documents using a registered lens.
    ///
    /// Applies the forward transformation (source -> destination schema version).
    fn transform(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream>;

    /// Inverse transform documents using a registered lens.
    ///
    /// Applies the reverse transformation (destination -> source schema version).
    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream>;

    /// Check if a transform exists.
    fn has_transform(&self, id: &TransformId) -> bool;

    /// Remove a registered transform.
    async fn remove(&self, id: &TransformId) -> Result<()>;
}

/// In-memory transform store for testing.
#[derive(Default)]
pub struct MemoryTransformStore {
    transforms: std::sync::RwLock<std::collections::HashMap<TransformId, LensConfig>>,
}

impl MemoryTransformStore {
    /// Create a new in-memory transform store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TransformStore for MemoryTransformStore {
    async fn add(&self, config: LensConfig) -> Result<TransformId> {
        use sha2::{Digest, Sha256};

        // Compute content-based ID for deduplication (matches Go's IPLD CID approach)
        // Hash only the lens modules, not the version IDs, so identical lens content
        // produces the same ID regardless of which versions it's associated with.
        let lenses_json = serde_json::to_vec(&config.lenses)
            .map_err(|e| Error::Pipeline(format!("failed to serialize lenses: {}", e)))?;
        let mut hasher = Sha256::new();
        hasher.update(&lenses_json);
        let hash = hasher.finalize();
        // Use "baf" prefix to mimic CID format, then 16 bytes of hash for uniqueness
        let id = TransformId::new(format!("baf{}", hex::encode(&hash[..16])));

        let mut transforms = self
            .transforms
            .write()
            .map_err(|e| Error::Pipeline(format!("failed to acquire write lock: {}", e)))?;

        // Deduplication: if this config already exists, return the existing ID
        if transforms.contains_key(&id) {
            return Ok(id);
        }

        transforms.insert(id.clone(), config);

        Ok(id)
    }

    async fn add_with_id(&self, id: TransformId, config: LensConfig) -> Result<()> {
        let mut transforms = self
            .transforms
            .write()
            .map_err(|e| Error::Pipeline(format!("failed to acquire write lock: {}", e)))?;
        transforms.insert(id, config);
        Ok(())
    }

    async fn list(&self) -> Result<std::collections::HashMap<String, crate::LensModule>> {
        let transforms = self
            .transforms
            .read()
            .map_err(|e| Error::Pipeline(format!("failed to acquire read lock: {}", e)))?;

        let result = transforms
            .iter()
            .filter_map(|(id, config)| config.lens().cloned().map(|l| (id.to_string(), l)))
            .collect();

        Ok(result)
    }

    fn transform(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        if !self.has_transform(id) {
            return Err(Error::TransformNotFound(id.to_string()));
        }

        // In-memory store just passes through documents unchanged (for testing)
        Ok(Box::pin(futures::stream::unfold(
            docs,
            |mut stream| async {
                use futures::StreamExt;
                stream.next().await.map(|doc| (Ok(doc), stream))
            },
        )))
    }

    fn inverse(&self, id: &TransformId, docs: LensDocStream) -> Result<LensDocResultStream> {
        if !self.has_transform(id) {
            return Err(Error::TransformNotFound(id.to_string()));
        }

        // In-memory store just passes through documents unchanged (for testing)
        Ok(Box::pin(futures::stream::unfold(
            docs,
            |mut stream| async {
                use futures::StreamExt;
                stream.next().await.map(|doc| (Ok(doc), stream))
            },
        )))
    }

    fn has_transform(&self, id: &TransformId) -> bool {
        self.transforms
            .read()
            .map(|t| t.contains_key(id))
            .unwrap_or(false)
    }

    async fn remove(&self, id: &TransformId) -> Result<()> {
        let mut transforms = self
            .transforms
            .write()
            .map_err(|e| Error::Pipeline(format!("failed to acquire write lock: {}", e)))?;

        if transforms.remove(id).is_none() {
            return Err(Error::TransformNotFound(id.to_string()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LensModule;

    #[tokio::test]
    async fn test_memory_store_add_transform() {
        let store = MemoryTransformStore::new();
        let config = LensConfig::new("v1", "v2", LensModule::from_path("/path/to/transform.wasm"));

        let id = store.add(config).await.unwrap();
        assert!(store.has_transform(&id));
    }

    #[tokio::test]
    async fn test_memory_store_remove_transform() {
        let store = MemoryTransformStore::new();
        let config = LensConfig::new("v1", "v2", LensModule::from_path("/path/to/transform.wasm"));

        let id = store.add(config).await.unwrap();
        assert!(store.has_transform(&id));

        store.remove(&id).await.unwrap();
        assert!(!store.has_transform(&id));
    }

    #[tokio::test]
    async fn test_memory_store_transform_not_found() {
        let store = MemoryTransformStore::new();
        let id = TransformId::new("nonexistent");

        let result = store.transform(&id, Box::pin(futures::stream::empty()));
        assert!(matches!(result, Err(Error::TransformNotFound(_))));
    }
}
