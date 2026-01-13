//! Last-Write-Wins (LWW) Register CRDT implementation
//!
//! The LWW Register resolves conflicts using priority-based ordering.
//! When two concurrent writes occur, the one with higher priority wins.
//! On tie, lexicographic comparison of values provides deterministic resolution.

use crate::priority::{decode_priority, encode_priority};
use crate::traits::{Context, Delta, PriorityReader, ReplicatedData, ValueReader};
use async_trait::async_trait;
use defra_core::{store::Store, Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

/// LWW Delta - represents a change to an LWW register
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwDelta {
    /// Document ID this delta applies to
    pub doc_id: Vec<u8>,
    /// Field name within the document
    pub field_name: String,
    /// Priority for conflict resolution
    pub priority: u64,
    /// Schema version identifier
    pub schema_version_id: String,
    /// The new value (empty vec = deletion/tombstone)
    pub data: Vec<u8>,
}

impl Delta for LwwDelta {
    fn get_priority(&self) -> u64 {
        self.priority
    }

    fn set_priority(&mut self, priority: u64) {
        self.priority = priority;
    }

    fn doc_id(&self) -> &[u8] {
        &self.doc_id
    }

    fn field_name(&self) -> &str {
        &self.field_name
    }

    fn schema_version_id(&self) -> &str {
        &self.schema_version_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// LWW Register - Last-Write-Wins conflict resolution for a field
pub struct Lww {
    /// Storage backend
    store: Arc<dyn Store>,
    /// Storage key for the value
    value_key: Vec<u8>,
    /// Storage key for the priority
    priority_key: Vec<u8>,
    /// Schema version
    schema_version_id: String,
    /// Field name
    field_name: String,
}

impl Lww {
    /// Create a new LWW register
    ///
    /// # Arguments
    /// * `store` - Storage backend
    /// * `schema_version_id` - Schema version identifier
    /// * `doc_id` - Document identifier
    /// * `field_name` - Field name
    pub fn new(
        store: Arc<dyn Store>,
        schema_version_id: String,
        doc_id: &[u8],
        field_name: String,
    ) -> Self {
        // Construct storage keys
        // Format: /data/<schema_version>/<doc_id>/<field_name>
        let mut value_key = Vec::new();
        value_key.extend_from_slice(b"/data/");
        value_key.extend_from_slice(schema_version_id.as_bytes());
        value_key.push(b'/');
        value_key.extend_from_slice(doc_id);
        value_key.push(b'/');
        value_key.extend_from_slice(field_name.as_bytes());

        // Priority key: value_key + "/priority"
        let mut priority_key = value_key.clone();
        priority_key.extend_from_slice(b"/priority");

        Self {
            store,
            value_key,
            priority_key,
            schema_version_id,
            field_name,
        }
    }

    /// Set a value with priority, implementing LWW merge logic
    async fn set_value(&mut self, data: &[u8], incoming_priority: u64) -> Result<()> {
        // Get current priority
        let current_priority = self.get_priority_internal().await?;

        // Compare priorities
        match incoming_priority.cmp(&current_priority) {
            std::cmp::Ordering::Less => {
                // Incoming priority is lower - ignore
                return Ok(());
            }
            std::cmp::Ordering::Equal => {
                // Same priority - use lexicographic tie-breaking
                let current_value = self.get_value_internal().await?;
                if data <= &current_value[..] {
                    // Current value wins or equal - ignore
                    return Ok(());
                }
                // Otherwise fall through to update
            }
            std::cmp::Ordering::Greater => {
                // Incoming priority is higher - fall through to update
            }
        }

        // Update value and priority
        if data.is_empty() {
            // Empty data = deletion/tombstone
            self.store.delete(&self.value_key).await?;
        } else {
            self.store.set(&self.value_key, data).await?;
        }

        let priority_bytes = encode_priority(incoming_priority);
        self.store.set(&self.priority_key, &priority_bytes).await?;

        Ok(())
    }

    /// Internal method to get current value
    async fn get_value_internal(&self) -> Result<Vec<u8>> {
        self.store
            .get(&self.value_key)
            .await?
            .ok_or_else(|| Error::MergeError("value not found".into()))
    }

    /// Internal method to get current priority
    async fn get_priority_internal(&self) -> Result<u64> {
        match self.store.get(&self.priority_key).await? {
            Some(bytes) => decode_priority(&bytes),
            None => Ok(0), // Default priority if not set
        }
    }
}

#[async_trait]
impl ReplicatedData for Lww {
    async fn merge(&mut self, _ctx: &Context, delta: &dyn Delta) -> Result<()> {
        // Downcast to LwwDelta
        let lww_delta = delta
            .as_any()
            .downcast_ref::<LwwDelta>()
            .ok_or_else(|| Error::MergeError("invalid delta type".into()))?;

        // Validate field name matches
        if lww_delta.field_name != self.field_name {
            return Err(Error::MergeError(format!(
                "field name mismatch: expected {}, got {}",
                self.field_name, lww_delta.field_name
            )));
        }

        // Validate schema version matches
        if lww_delta.schema_version_id != self.schema_version_id {
            return Err(Error::MergeError(format!(
                "schema version mismatch: expected {}, got {}",
                self.schema_version_id, lww_delta.schema_version_id
            )));
        }

        // Apply merge logic
        self.set_value(&lww_delta.data, lww_delta.priority).await
    }

    fn headstore_prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.extend_from_slice(b"/head/");
        prefix.extend_from_slice(self.schema_version_id.as_bytes());
        prefix.push(b'/');
        prefix.extend_from_slice(self.field_name.as_bytes());
        prefix
    }
}

#[async_trait]
impl ValueReader for Lww {
    async fn value(&self) -> Result<Vec<u8>> {
        self.get_value_internal().await
    }
}

#[async_trait]
impl PriorityReader for Lww {
    async fn priority(&self) -> Result<u64> {
        self.get_priority_internal().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// Simple in-memory store for testing
    struct MemoryStore {
        data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl Store for MemoryStore {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.data.lock().await.get(key).cloned())
        }

        async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
            self.data.lock().await.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> Result<()> {
            self.data.lock().await.remove(key);
            Ok(())
        }

        async fn has(&self, key: &[u8]) -> Result<bool> {
            Ok(self.data.lock().await.contains_key(key))
        }
    }

    #[tokio::test]
    async fn test_lww_higher_priority_wins() {
        let store = Arc::new(MemoryStore::new());
        let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string());

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First write with priority 10
        let delta1 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 10,
            schema_version_id: "v1".to_string(),
            data: b"Alice".to_vec(),
        };
        lww.merge(&ctx, &delta1).await.unwrap();
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Second write with higher priority 20
        let delta2 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 20,
            schema_version_id: "v1".to_string(),
            data: b"Bob".to_vec(),
        };
        lww.merge(&ctx, &delta2).await.unwrap();
        assert_eq!(lww.value().await.unwrap(), b"Bob");
    }

    #[tokio::test]
    async fn test_lww_lower_priority_ignored() {
        let store = Arc::new(MemoryStore::new());
        let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string());

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First write with priority 20
        let delta1 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 20,
            schema_version_id: "v1".to_string(),
            data: b"Alice".to_vec(),
        };
        lww.merge(&ctx, &delta1).await.unwrap();

        // Second write with lower priority 10 - should be ignored
        let delta2 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 10,
            schema_version_id: "v1".to_string(),
            data: b"Bob".to_vec(),
        };
        lww.merge(&ctx, &delta2).await.unwrap();
        assert_eq!(lww.value().await.unwrap(), b"Alice");
    }

    #[tokio::test]
    async fn test_lww_same_priority_lexicographic() {
        let store = Arc::new(MemoryStore::new());
        let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string());

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First write: "Alice" with priority 10
        let delta1 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 10,
            schema_version_id: "v1".to_string(),
            data: b"Alice".to_vec(),
        };
        lww.merge(&ctx, &delta1).await.unwrap();

        // Second write: "Bob" with same priority 10
        // "Bob" > "Alice" lexicographically, so Bob should win
        let delta2 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 10,
            schema_version_id: "v1".to_string(),
            data: b"Bob".to_vec(),
        };
        lww.merge(&ctx, &delta2).await.unwrap();
        assert_eq!(lww.value().await.unwrap(), b"Bob");
    }

    #[tokio::test]
    async fn test_lww_deletion() {
        let store = Arc::new(MemoryStore::new());
        let mut lww = Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string());

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Set value
        let delta1 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 10,
            schema_version_id: "v1".to_string(),
            data: b"Alice".to_vec(),
        };
        lww.merge(&ctx, &delta1).await.unwrap();

        // Delete (empty data)
        let delta2 = LwwDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 20,
            schema_version_id: "v1".to_string(),
            data: Vec::new(),
        };
        lww.merge(&ctx, &delta2).await.unwrap();

        // Value should be deleted
        assert!(lww.value().await.is_err());
    }
}
