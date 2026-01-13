//! Composite CRDT - Document-level CRDT composition
//!
//! The Composite CRDT manages document-level state by coordinating
//! multiple field-level CRDTs (LWW, Counter, etc).

use crate::priority::{decode_priority, encode_priority};
use crate::traits::{Context, Delta, MergeResult, ReplicatedData};
use async_trait::async_trait;
use defra_core::{store::Store, types::DocId, Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Composite Delta - represents changes to multiple fields in a document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeDelta {
    /// Document ID this delta applies to
    doc_id: Vec<u8>,
    /// Schema version identifier
    schema_version_id: String,
    /// Priority for this delta
    priority: u64,
    /// Field-level deltas
    field_deltas: HashMap<String, FieldDelta>,
}

impl CompositeDelta {
    /// Create a new composite delta
    pub fn new(doc_id: Vec<u8>, schema_version_id: String, priority: u64) -> Result<Self> {
        if doc_id.is_empty() {
            return Err(Error::MergeError("doc_id cannot be empty".into()));
        }
        if schema_version_id.is_empty() {
            return Err(Error::MergeError(
                "schema_version_id cannot be empty".into(),
            ));
        }
        Ok(Self {
            doc_id,
            schema_version_id,
            priority,
            field_deltas: HashMap::new(),
        })
    }

    /// Add a field delta
    pub fn add_field_delta(&mut self, field_name: String, delta: FieldDelta) -> Result<()> {
        if field_name.is_empty() {
            return Err(Error::MergeError("field_name cannot be empty".into()));
        }
        self.field_deltas.insert(field_name, delta);
        Ok(())
    }

    /// Get the document ID
    pub fn doc_id(&self) -> &[u8] {
        &self.doc_id
    }

    /// Get the schema version ID
    pub fn schema_version_id(&self) -> &str {
        &self.schema_version_id
    }

    /// Get the priority
    pub fn priority(&self) -> u64 {
        self.priority
    }

    /// Get the field deltas
    pub fn field_deltas(&self) -> &HashMap<String, FieldDelta> {
        &self.field_deltas
    }
}

/// Field-level delta within a composite
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldDelta {
    /// LWW field update
    Lww { priority: u64, data: Vec<u8> },
    /// Counter increment/decrement
    Counter {
        priority: u64,
        nonce: i64,
        data: Vec<u8>,
    },
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
    pub fn new(store: Arc<dyn Store>, doc_id: DocId, schema_version_id: String) -> Self {
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
        self.field_managers
            .insert(field_name, FieldCrdtType::Counter);
    }

    /// Build the value key for a field
    fn build_value_key(&self, field_name: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(b"/data/");
        key.extend_from_slice(self.schema_version_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(self.doc_id.as_str().as_bytes());
        key.push(b'/');
        key.extend_from_slice(field_name.as_bytes());
        key
    }

    /// Build the priority key for a field
    fn build_priority_key(&self, field_name: &str) -> Vec<u8> {
        let mut key = self.build_value_key(field_name);
        key.extend_from_slice(b"/priority");
        key
    }

    /// Get the current priority for a field (0 if not set)
    async fn get_field_priority(&self, field_name: &str) -> Result<u64> {
        let priority_key = self.build_priority_key(field_name);
        match self.store.get(&priority_key).await? {
            Some(bytes) => decode_priority(&bytes),
            None => Ok(0),
        }
    }

    /// Set the priority for a field
    async fn set_field_priority(&self, field_name: &str, priority: u64) -> Result<()> {
        let priority_key = self.build_priority_key(field_name);
        let priority_bytes = encode_priority(priority);
        self.store.set(&priority_key, &priority_bytes).await
    }

    /// Apply field-level delta with proper CRDT conflict resolution
    ///
    /// # Warning
    ///
    /// Counter operations involve multiple storage writes (value + nonce) without
    /// transaction support. If the process crashes between these operations, state may become
    /// inconsistent and violate CRDT idempotency guarantees.
    async fn apply_field_delta(
        &self,
        field_name: &str,
        field_delta: &FieldDelta,
    ) -> Result<MergeResult> {
        let crdt_type = self
            .field_managers
            .get(field_name)
            .ok_or_else(|| Error::MergeError(format!("unknown field: {}", field_name)))?;

        match (crdt_type, field_delta) {
            (FieldCrdtType::Lww, FieldDelta::Lww { priority, data }) => {
                let value_key = self.build_value_key(field_name);
                let current_priority = self.get_field_priority(field_name).await?;

                // LWW conflict resolution: higher priority wins
                match priority.cmp(&current_priority) {
                    std::cmp::Ordering::Less => {
                        return Ok(MergeResult::RejectedLowerPriority {
                            current: current_priority,
                            incoming: *priority,
                        });
                    }
                    std::cmp::Ordering::Equal => {
                        // Same priority - use lexicographic tie-breaking
                        let current_value = self.store.get(&value_key).await?.unwrap_or_default();
                        if data.as_slice() <= current_value.as_slice() {
                            return Ok(MergeResult::RejectedTieBreak);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        // Higher priority - proceed to update
                    }
                }

                // Apply the update
                if data.is_empty() {
                    self.store.delete(&value_key).await?;
                } else {
                    self.store.set(&value_key, data).await?;
                }
                self.set_field_priority(field_name, *priority).await?;

                Ok(MergeResult::Applied)
            }
            (
                FieldCrdtType::Counter,
                FieldDelta::Counter {
                    priority: _,
                    nonce,
                    data,
                },
            ) => {
                let value_key = self.build_value_key(field_name);

                // Check nonce idempotency (counters are commutative, no priority comparison needed)
                let mut nonce_key = value_key.clone();
                nonce_key.extend_from_slice(b"/nonces/");
                nonce_key.extend_from_slice(&nonce.to_be_bytes());

                if self.store.has(&nonce_key).await? {
                    return Ok(MergeResult::SkippedAlreadyApplied { nonce: *nonce });
                }

                // Apply increment
                let current = match self.store.get(&value_key).await? {
                    Some(bytes) => {
                        if bytes.len() != 8 {
                            return Err(Error::MergeError(format!(
                                "invalid counter data length for field '{}': expected 8 bytes, got {}",
                                field_name, bytes.len()
                            )));
                        }
                        i64::from_be_bytes(bytes[..8].try_into().unwrap())
                    }
                    None => 0,
                };

                if data.len() != 8 {
                    return Err(Error::MergeError(format!(
                        "invalid counter increment data for field '{}': expected 8 bytes, got {}",
                        field_name,
                        data.len()
                    )));
                }

                let increment = i64::from_be_bytes(data[..8].try_into().unwrap());
                let new_value = current.wrapping_add(increment);
                self.store.set(&value_key, &new_value.to_be_bytes()).await?;
                self.store.set(&nonce_key, &[1]).await?;

                Ok(MergeResult::Applied)
            }
            (_, FieldDelta::Delete { priority }) => {
                let value_key = self.build_value_key(field_name);
                let current_priority = self.get_field_priority(field_name).await?;

                // Delete conflict resolution: higher priority wins
                // On tie, non-empty value wins over deletion (empty < any value lexicographically)
                match priority.cmp(&current_priority) {
                    std::cmp::Ordering::Less => {
                        return Ok(MergeResult::RejectedLowerPriority {
                            current: current_priority,
                            incoming: *priority,
                        });
                    }
                    std::cmp::Ordering::Equal => {
                        // Same priority - deletion (empty) loses to any existing value
                        let current_value = self.store.get(&value_key).await?;
                        if current_value.is_some() && !current_value.as_ref().unwrap().is_empty() {
                            return Ok(MergeResult::RejectedTieBreak);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        // Higher priority - proceed to delete
                    }
                }

                // Apply deletion
                self.store.delete(&value_key).await?;
                self.set_field_priority(field_name, *priority).await?;

                Ok(MergeResult::Applied)
            }
            _ => Err(Error::MergeError(format!(
                "field type mismatch for field: {}",
                field_name
            ))),
        }
    }
}

#[async_trait]
impl ReplicatedData for CompositeDAG {
    async fn merge(&mut self, _ctx: &Context, delta: &dyn Delta) -> Result<MergeResult> {
        // Downcast to CompositeDelta
        let composite_delta = delta
            .as_any()
            .downcast_ref::<CompositeDelta>()
            .ok_or_else(|| {
                Error::MergeError(
                    "invalid delta type for Composite merge: expected CompositeDelta".into(),
                )
            })?;

        // Validate doc ID
        if composite_delta.doc_id != self.doc_id.as_str().as_bytes() {
            return Err(Error::MergeError("document ID mismatch".into()));
        }

        // Validate schema version
        if composite_delta.schema_version_id != self.schema_version_id {
            return Err(Error::MergeError("schema version mismatch".into()));
        }

        // Pre-validation phase: Check all fields exist and types match before applying any changes.
        // This minimizes the risk of partial application on validation errors.
        for (field_name, field_delta) in &composite_delta.field_deltas {
            let crdt_type = self
                .field_managers
                .get(field_name)
                .ok_or_else(|| Error::MergeError(format!("unknown field: {}", field_name)))?;

            // Validate field type matches delta type
            match (crdt_type, field_delta) {
                (FieldCrdtType::Lww, FieldDelta::Lww { .. })
                | (FieldCrdtType::Lww, FieldDelta::Delete { .. })
                | (FieldCrdtType::Counter, FieldDelta::Counter { .. })
                | (FieldCrdtType::Counter, FieldDelta::Delete { .. }) => {
                    // Types match
                }
                _ => {
                    return Err(Error::MergeError(format!(
                        "field type mismatch for field: {}",
                        field_name
                    )));
                }
            }

            // Validate Counter data length if applicable
            if let FieldDelta::Counter { data, .. } = field_delta {
                if data.len() != 8 {
                    return Err(Error::MergeError(format!(
                        "invalid counter increment data for field '{}': expected 8 bytes, got {}",
                        field_name,
                        data.len()
                    )));
                }
            }
        }

        // Apply each field delta (now that all fields are pre-validated)
        // Note: Storage operations still not atomic, but validation errors won't cause partial state.
        let mut any_applied = false;
        for (field_name, field_delta) in &composite_delta.field_deltas {
            let result = self.apply_field_delta(field_name, field_delta).await?;
            if result.was_applied() {
                any_applied = true;
            }
        }

        // Return Applied if at least one field was applied, or if delta is empty (no-op)
        // Otherwise all fields were rejected or skipped
        if any_applied || composite_delta.field_deltas.is_empty() {
            Ok(MergeResult::Applied)
        } else {
            // All fields were rejected or skipped - return a generic rejection
            Ok(MergeResult::RejectedTieBreak)
        }
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
    use crate::test_utils::MemoryStore;

    #[tokio::test]
    async fn test_composite_multiple_fields() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

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

    #[tokio::test]
    async fn test_composite_field_type_mismatch_lww_to_counter() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

        // Register field as LWW
        composite.register_lww_field("value".to_string());

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to apply Counter delta to LWW field
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "value".to_string(),
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

        let result = composite.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("field type mismatch"));
    }

    #[tokio::test]
    async fn test_composite_field_type_mismatch_counter_to_lww() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

        // Register field as Counter
        composite.register_counter_field("count".to_string());

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to apply LWW delta to Counter field
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "count".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"not_a_number".to_vec(),
            },
        );

        let delta = CompositeDelta {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 10,
            field_deltas,
        };

        let result = composite.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("field type mismatch"));
    }

    #[tokio::test]
    async fn test_composite_unknown_field() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

        // Don't register any fields

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to apply delta to unknown field
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "unknown_field".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"value".to_vec(),
            },
        );

        let delta = CompositeDelta {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 10,
            field_deltas,
        };

        let result = composite.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown field"));
    }

    #[tokio::test]
    async fn test_composite_schema_evolution_type_change() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

        // Register field as LWW in schema v1
        composite.register_lww_field("score".to_string());

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Apply LWW delta successfully
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "score".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"100".to_vec(),
            },
        );

        let delta1 = CompositeDelta {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 10,
            field_deltas: field_deltas.clone(),
        };

        composite.merge(&ctx, &delta1).await.unwrap();

        // Now simulate schema evolution where "score" becomes a Counter
        // This should fail since the field is registered as LWW
        let mut field_deltas2 = HashMap::new();
        field_deltas2.insert(
            "score".to_string(),
            FieldDelta::Counter {
                priority: 20,
                nonce: 12345,
                data: 50i64.to_be_bytes().to_vec(),
            },
        );

        let delta2 = CompositeDelta {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 20,
            field_deltas: field_deltas2,
        };

        let result = composite.merge(&ctx, &delta2).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("field type mismatch"));
    }

    #[tokio::test]
    async fn test_composite_doc_id_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

        composite.register_lww_field("name".to_string());

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Delta with wrong doc ID
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"Alice".to_vec(),
            },
        );

        let delta = CompositeDelta {
            doc_id: b"wrong_doc".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 10,
            field_deltas,
        };

        let result = composite.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("document ID mismatch"));
    }

    #[tokio::test]
    async fn test_composite_schema_version_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut composite = CompositeDAG::new(store.clone(), DocId::new("doc1"), "v1".to_string());

        composite.register_lww_field("name".to_string());

        let ctx = Context {
            doc_id: DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Delta with wrong schema version
        let mut field_deltas = HashMap::new();
        field_deltas.insert(
            "name".to_string(),
            FieldDelta::Lww {
                priority: 10,
                data: b"Alice".to_vec(),
            },
        );

        let delta = CompositeDelta {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v2".to_string(),
            priority: 10,
            field_deltas,
        };

        let result = composite.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema version mismatch"));
    }

    #[test]
    fn test_composite_delta_empty_doc_id_rejected() {
        let result = CompositeDelta::new(vec![], "v1".to_string(), 10);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("doc_id cannot be empty"));
    }

    #[test]
    fn test_composite_delta_empty_schema_version_rejected() {
        let result = CompositeDelta::new(b"doc1".to_vec(), "".to_string(), 10);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema_version_id cannot be empty"));
    }

    #[test]
    fn test_composite_delta_empty_field_name_rejected() {
        let mut delta = CompositeDelta::new(b"doc1".to_vec(), "v1".to_string(), 10).unwrap();
        let result = delta.add_field_delta("".to_string(), FieldDelta::Lww {
            priority: 10,
            data: b"value".to_vec(),
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field_name cannot be empty"));
    }
}
