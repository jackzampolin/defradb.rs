//! Collection provider trait for schema resolution.

use async_trait::async_trait;
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use crate::error::Result;

/// Trait for resolving collection schemas by name.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait CollectionProvider: MaybeSendSync {
    /// Get a collection schema by name.
    async fn get_collection(&self, name: &str) -> Result<Option<Arc<CollectionVersion>>>;

    /// List all collection names.
    async fn list_collections(&self) -> Result<Vec<String>>;
}

/// Static collection provider for tests and backward compatibility.
pub struct StaticCollectionProvider {
    collections: HashMap<String, Arc<CollectionVersion>>,
}

impl StaticCollectionProvider {
    /// Create from a list of collection schemas.
    pub fn new(collections: Vec<CollectionVersion>) -> Self {
        let map = collections
            .into_iter()
            .map(|c| (c.name.clone(), Arc::new(c)))
            .collect();
        Self { collections: map }
    }

    /// Create from an existing HashMap.
    pub fn from_map(collections: HashMap<String, Arc<CollectionVersion>>) -> Self {
        Self { collections }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CollectionProvider for StaticCollectionProvider {
    async fn get_collection(&self, name: &str) -> Result<Option<Arc<CollectionVersion>>> {
        Ok(self.collections.get(name).cloned())
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        Ok(self.collections.keys().cloned().collect())
    }
}
