//! Composite CRDT - Document-level CRDT composition
//!
//! The Composite CRDT manages document-level state by coordinating
//! multiple field-level CRDTs (LWW, Counter, etc).

use crate::traits::{Context, Delta, ReplicatedData};
use defra_core::{Error, Result, store::Store, types::DocId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Composite Delta - represents changes to multiple fields in a document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeDelta {
    /// Document ID this delta applies to
    pub doc_id: Vec<u8>,
    /// Schema version identifier
    pub schema_version_id: String,
    /// Priority for this delta
    pub priority: u64,
    /// Field-level deltas
    pub field_deltas: HashMap<String, FieldDelta>,
}

/// Field-level delta within a composite
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldDelta {
    /// LWW field update
    Lww { priority: u64, data: Vec<u8> },
    /// Counter increment/decrement
    Counter { priority: u64, nonce: i64, data: Vec<u8> },
    /// Deletion marker
    Delete { priority: u64 },
}

impl Delta for CompositeDelta {
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
        "" // Composite applies to entire document
    }

    fn schema_version_id(&self) -> &str {
        &self.schema_version_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Composite DAG - manages document-level CRDT state
pub struct CompositeDAG {
    /// Storage backend
    store: Arc<dyn Store>,
    /// Document ID
    doc_id: DocId,
    /// Schema version
    schema_version_id: String,
    /// Field-level CRDT managers
    /// Maps field_name -> CRDT instance
    /// (In real implementation, this would be a proper registry)
    field_managers: HashMap<String, FieldCrdtType>,
}

/// Types of field-level CRDTs
#[derive(Debug, Clone)]
enum FieldCrdtType {
    Lww,
    Counter,
}

impl CompositeDAG {
    /// Create a new CompositeDAG
    pub fn new(
        store: Arc<dyn Store>,
        doc_id: DocId,
        schema_version_id: String,
    ) -> Self {
        Self {
            store,
            doc_id,
            schema_version_id,
            field_managers: HashMap::new(),
        }
    }

    /// Register a field as LWW
    pub fn register_lww_field(&mut self, field_name: String) {
        self.field_managers.insert(field_name, FieldCrdtType::Lww);
    }

    /// Register a field as Counter
    pub fn register_counter_field(&mut self, field_name: String) {
        self.field_managers.insert(field_name, FieldCrdtType::Counter);
    }

    /// Apply field-level delta
    async fn apply_field_delta(
        &self,
        field_name: &str,
        field_delta: &FieldDelta,
    ) -> Result<()> {
        let crdt_type = self.field_managers.get(field_name)
            .ok_or_else(|| Error::MergeError(format!("unknown field: {}", field_name)))?;

        match (crdt_type, field_delta) {
            (FieldCrdtType::Lww, FieldDelta::Lww { priority: _, data }) => {
                // Apply LWW merge logic
                let mut value_key = Vec::new();
                value_key.extend_from_slice(b"/data/");
                value_key.extend_from_slice(self.schema_version_id.as_bytes());
                value_key.push(b'/');
                value_key.extend_from_slice(self.doc_id.as_str().as_bytes());
                value_key.push(b'/');
                value_key.extend_from_slice(field_name.as_bytes());

                // Simple implementation - should use Lww::merge
                if data.is_empty() {
                    self.store.delete(&value_key).await?;
                } else {
                    self.store.set(&value_key, data).await?;
                }

                Ok(())
            }
            (FieldCrdtType::Counter, FieldDelta::Counter { priority: _, nonce, data }) => {
                // Apply counter merge logic
                let mut value_key = Vec::new();
                value_key.extend_from_slice(b"/data/");
                value_key.extend_from_slice(self.schema_version_id.as_bytes());
                value_key.push(b'/');
                value_key.extend_from_slice(self.doc_id.as_str().as_bytes());
                value_key.push(b'/');
                value_key.extend_from_slice(field_name.as_bytes());

                // Check nonce idempotency
                let mut nonce_key = value_key.clone();
                nonce_key.extend_from_slice(b"/nonces/");
                nonce_key.extend_from_slice(&nonce.to_be_bytes());

                if self.store.has(&nonce_key).await? {
                    return Ok(()); // Already applied
                }

                // Apply increment
                let current = match self.store.get(&value_key).await? {
                    Some(bytes) => {
                        if bytes.len() == 8 {
                            i64::from_be_bytes(bytes[..8].try_into().unwrap())
                        } else {
                            0
                        }
                    }
                    None => 0,
                };

                if data.len() == 8 {
                    let increment = i64::from_be_bytes(data[..8].try_into().unwrap());
                    let new_value = current.saturating_add(increment);
                    self.store.set(&value_key, &new_value.to_be_bytes()).await?;
                    self.store.set(&nonce_key, &[1]).await?;
                }

                Ok(())
            }
            (_, FieldDelta::Delete { priority: _ }) => {
                // Apply deletion
                let mut value_key = Vec::new();
                value_key.extend_from_slice(b"/data/");
                value_key.extend_from_slice(self.schema_version_id.as_bytes());
                value_key.push(b'/');
                value_key.extend_from_slice(self.doc_id.as_str().as_bytes());
                value_key.push(b'/');
                value_key.extend_from_slice(field_name.as_bytes());

                self.store.delete(&value_key).await?;
                Ok(())
            }
            _ => {
                Err(Error::MergeError(format!(
                    "field type mismatch for field: {}",
                    field_name
                )))
            }
        }
    }
}

#[async_trait]
impl ReplicatedData for CompositeDAG {
    async fn merge(&mut self, _ctx: &Context, delta: &dyn Delta) -> Result<()> {
        // Downcast to CompositeDelta
        let composite_delta = delta
            .as_any()
            .downcast_ref::<CompositeDelta>()
            .ok_or_else(|| Error::MergeError("invalid delta type".into()))?;

        // Validate doc ID
        if composite_delta.doc_id != self.doc_id.as_str().as_bytes() {
            return Err(Error::MergeError("document ID mismatch".into()));
        }

        // Validate schema version
        if composite_delta.schema_version_id != self.schema_version_id {
            return Err(Error::MergeError("schema version mismatch".into()));
        }

        // Apply each field delta
        for (field_name, field_delta) in &composite_delta.field_deltas {
            self.apply_field_delta(field_name, field_delta).await?;
        }

        Ok(())
    }

    fn headstore_prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.extend_from_slice(b"/head/");
        prefix.extend_from_slice(self.doc_id.as_str().as_bytes());
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

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
    async fn test_composite_multiple_fields() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(
            store.clone(),
            DocId::new("doc1"),
            "v1".to_string(),
        );

        // Register fields
        composite.register_lww_field("name".to_string());
        composite.register_counter_field("count".to_string());

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Create composite delta with multiple fields
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"Alice".to_vec(),
            },
        );
        field_deltas.insert(
            "count".to_string(),
            FieldDelta::Counter {
                priority: 10,
                nonce: 12345,
                data: 5i64.to_be_bytes().to_vec(),
            },
        );

        let delta = CompositeDelta {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 10,
            field_deltas,
        };

        composite.merge(&ctx, &delta).await.unwrap();

        // Verify name field
        let name_key = b"/data/v1/doc1/name".to_vec();
        let name = store.get(&name_key).await.unwrap().unwrap();
        assert_eq!(name, b"Alice");

        // Verify count field
        let count_key = b"/data/v1/doc1/count".to_vec();
        let count_bytes = store.get(&count_key).await.unwrap().unwrap();
        let count = i64::from_be_bytes(count_bytes.try_into().unwrap());
        assert_eq!(count, 5);
    }
}
