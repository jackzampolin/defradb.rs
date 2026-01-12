//! Last-Write-Wins (LWW) Register CRDT implementation
//!
//! The LWW Register resolves conflicts using priority-based ordering.
//! When two concurrent writes occur, the one with higher priority wins.
//! On tie, lexicographic comparison of values provides deterministic resolution.

use crate::priority::{decode_priority, encode_priority};
use crate::traits::{Context, Delta, MergeResult, PriorityReader, ReplicatedData, ValueReader};
use async_trait::async_trait;
use defra_core::{store::Store, Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

/// LWW Delta - represents a change to an LWW register
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwDelta {
    /// Document ID this delta applies to
    doc_id: Vec<u8>,
    /// Field name within the document
    field_name: String,
    /// Priority for conflict resolution
    priority: u64,
    /// Schema version identifier
    schema_version_id: String,
    /// The new value (empty vec = deletion/tombstone)
    data: Vec<u8>,
}

impl LwwDelta {
    /// Create a new LWW delta
    pub fn new(
        doc_id: Vec<u8>,
        field_name: String,
        priority: u64,
        schema_version_id: String,
        data: Vec<u8>,
    ) -> Result<Self> {
        if doc_id.is_empty() {
            return Err(Error::MergeError("doc_id cannot be empty".into()));
        }
        if field_name.is_empty() {
            return Err(Error::MergeError("field_name cannot be empty".into()));
        }
        if schema_version_id.is_empty() {
            return Err(Error::MergeError(
                "schema_version_id cannot be empty".into(),
            ));
        }
        Ok(Self {
            doc_id,
            field_name,
            priority,
            schema_version_id,
            data,
        })
    }

    /// Create a deletion delta (tombstone)
    pub fn delete(
        doc_id: Vec<u8>,
        field_name: String,
        priority: u64,
        schema_version_id: String,
    ) -> Result<Self> {
        Self::new(doc_id, field_name, priority, schema_version_id, Vec::new())
    }

    /// Check if this delta is a tombstone (deletion)
    pub fn is_tombstone(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the document ID
    pub fn doc_id(&self) -> &[u8] {
        &self.doc_id
    }

    /// Get the field name
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the priority
    pub fn priority(&self) -> u64 {
        self.priority
    }

    /// Get the schema version ID
    pub fn schema_version_id(&self) -> &str {
        &self.schema_version_id
    }

    /// Get the data
    pub fn data(&self) -> &[u8] {
        &self.data
    }
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
    /// * `schema_version_id` - Schema version identifier (must not be empty)
    /// * `doc_id` - Document identifier (must not be empty)
    /// * `field_name` - Field name (must not be empty)
    ///
    /// # Errors
    /// Returns an error if schema_version_id, doc_id, or field_name is empty.
    pub fn new(
        store: Arc<dyn Store>,
        schema_version_id: String,
        doc_id: &[u8],
        field_name: String,
    ) -> Result<Self> {
        if schema_version_id.is_empty() {
            return Err(Error::MergeError(
                "schema_version_id cannot be empty".into(),
            ));
        }
        if doc_id.is_empty() {
            return Err(Error::MergeError("doc_id cannot be empty".into()));
        }
        if field_name.is_empty() {
            return Err(Error::MergeError("field_name cannot be empty".into()));
        }

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

        Ok(Self {
            store,
            value_key,
            priority_key,
            schema_version_id,
            field_name,
        })
    }

    /// Set a value with priority, implementing LWW merge logic
    async fn set_value(&mut self, data: &[u8], incoming_priority: u64) -> Result<MergeResult> {
        // Get current priority
        let current_priority = self.get_priority_internal().await?;

        // Compare priorities
        match incoming_priority.cmp(&current_priority) {
            std::cmp::Ordering::Less => {
                // LWW semantics: Reject updates with lower priority
                return Ok(MergeResult::RejectedLowerPriority {
                    current: current_priority,
                    incoming: incoming_priority,
                });
            }
            std::cmp::Ordering::Equal => {
                // Same priority - use lexicographic tie-breaking for deterministic convergence
                // Current value wins if incoming data <= current (lexicographically)
                // This means: incoming data must be strictly greater to win
                // Note: Store errors propagate via ?, None (uninitialized) treated as empty
                let current_value: Vec<u8> =
                    self.store.get(&self.value_key).await?.unwrap_or_default();
                if data <= &current_value[..] {
                    return Ok(MergeResult::RejectedTieBreak);
                }
                // Incoming data is lexicographically greater - fall through to update
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

        Ok(MergeResult::Applied)
    }

    /// Internal method to get current value
    async fn get_value_internal(&self) -> Result<Vec<u8>> {
        self.store.get(&self.value_key).await?.ok_or_else(|| {
            Error::MergeError(format!(
                "value not found for field '{}' in schema '{}'. \
                     This indicates the field has never been set or has been deleted.",
                self.field_name, self.schema_version_id
            ))
        })
    }

    /// Internal method to get current priority
    async fn get_priority_internal(&self) -> Result<u64> {
        match self.store.get(&self.priority_key).await? {
            Some(bytes) => decode_priority(&bytes),
            None => {
                // Missing priority indicates uninitialized state
                // Returning default priority 0
                Ok(0)
            }
        }
    }
}

#[async_trait]
impl ReplicatedData for Lww {
    async fn merge(&mut self, _ctx: &Context, delta: &dyn Delta) -> Result<MergeResult> {
        // Downcast to LwwDelta
        let lww_delta = delta.as_any().downcast_ref::<LwwDelta>().ok_or_else(|| {
            Error::MergeError("invalid delta type for LWW merge: expected LwwDelta".into())
        })?;

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
    use crate::test_utils::MemoryStore;

    #[tokio::test]
    async fn test_lww_higher_priority_wins() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First write with priority 10
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
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
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

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
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

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
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

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

    #[tokio::test]
    async fn test_lww_empty_data_tie_breaking() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Write value at priority 10
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
        lww.merge(&ctx, &delta1).await.unwrap();
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Delete (empty data) at same priority 10
        // Lexicographically, empty < "Alice", so "Alice" should win
        let delta2 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            Vec::new(),
        )
        .unwrap();
        lww.merge(&ctx, &delta2).await.unwrap();

        // Value should still be "Alice" (empty data lost tie-break)
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Now delete at higher priority 20
        let delta3 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            20,
            "v1".to_string(),
            Vec::new(),
        )
        .unwrap();
        lww.merge(&ctx, &delta3).await.unwrap();

        // Value should now be deleted
        assert!(lww.value().await.is_err());
    }

    #[tokio::test]
    async fn test_lww_deletion_resurrection_with_priority() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Write value at priority 20
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            20,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
        lww.merge(&ctx, &delta1).await.unwrap();

        // Try to delete at lower priority 10 (should be ignored)
        let delta2 =
            LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 10, "v1".to_string()).unwrap();
        lww.merge(&ctx, &delta2).await.unwrap();

        // Value should still exist (deletion was lower priority)
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Delete at same priority 20
        // Since priorities are equal, lexicographic tie-breaking applies
        // Empty data < "Alice", so "Alice" wins
        let delta3 =
            LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 20, "v1".to_string()).unwrap();
        lww.merge(&ctx, &delta3).await.unwrap();

        // Value should still be "Alice" (tie-break)
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Delete at higher priority 30
        let delta4 =
            LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 30, "v1".to_string()).unwrap();
        lww.merge(&ctx, &delta4).await.unwrap();

        // Value should now be deleted
        assert!(lww.value().await.is_err());

        // Try to resurrect with lower priority 25 (should fail)
        let delta5 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            25,
            "v1".to_string(),
            b"Bob".to_vec(),
        )
        .unwrap();
        lww.merge(&ctx, &delta5).await.unwrap();

        // Value should still be deleted (resurrection priority too low)
        assert!(lww.value().await.is_err());

        // Resurrect with higher priority 40
        let delta6 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            40,
            "v1".to_string(),
            b"Bob".to_vec(),
        )
        .unwrap();
        lww.merge(&ctx, &delta6).await.unwrap();

        // Value should now be resurrected
        assert_eq!(lww.value().await.unwrap(), b"Bob");
    }

    #[test]
    fn test_lww_delta_empty_field_name() {
        let result = LwwDelta::new(
            b"doc1".to_vec(),
            "".to_string(),
            10,
            "v1".to_string(),
            b"value".to_vec(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field_name"));
    }

    #[test]
    fn test_lww_delta_empty_schema_version() {
        let result = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "".to_string(),
            b"value".to_vec(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema_version_id"));
    }

    #[test]
    fn test_lww_delta_delete_empty_field_name() {
        let result = LwwDelta::delete(b"doc1".to_vec(), "".to_string(), 10, "v1".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field_name"));
    }

    #[test]
    fn test_lww_delta_delete_empty_schema_version() {
        let result = LwwDelta::delete(b"doc1".to_vec(), "name".to_string(), 10, "".to_string());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema_version_id"));
    }

    #[test]
    fn test_lww_delta_empty_doc_id() {
        let result = LwwDelta::new(
            Vec::new(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"value".to_vec(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("doc_id"));
    }

    #[tokio::test]
    async fn test_lww_wrong_delta_type() {
        use crate::CounterDelta;

        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to merge a CounterDelta into an LWW register
        let wrong_delta = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            12345,
            "v1".to_string(),
            5,
        )
        .unwrap();

        let result = lww.merge(&ctx, &wrong_delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid delta type for LWW"));
    }

    #[tokio::test]
    async fn test_lww_merge_result_applied() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        let delta = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();

        let result = lww.merge(&ctx, &delta).await.unwrap();
        assert!(matches!(result, MergeResult::Applied));
    }

    #[tokio::test]
    async fn test_lww_merge_result_rejected_lower_priority() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First write with priority 20
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            20,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
        let result1 = lww.merge(&ctx, &delta1).await.unwrap();
        assert!(matches!(result1, MergeResult::Applied));

        // Second write with lower priority 10 - should be rejected
        let delta2 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"Bob".to_vec(),
        )
        .unwrap();
        let result2 = lww.merge(&ctx, &delta2).await.unwrap();
        assert!(matches!(
            result2,
            MergeResult::RejectedLowerPriority {
                current: 20,
                incoming: 10
            }
        ));
    }

    #[tokio::test]
    async fn test_lww_merge_result_rejected_tie_break() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First write: "Bob" with priority 10
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"Bob".to_vec(),
        )
        .unwrap();
        let result1 = lww.merge(&ctx, &delta1).await.unwrap();
        assert!(matches!(result1, MergeResult::Applied));

        // Second write: "Alice" with same priority 10
        // "Alice" < "Bob" lexicographically, so should be rejected
        let delta2 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
        let result2 = lww.merge(&ctx, &delta2).await.unwrap();
        assert!(matches!(result2, MergeResult::RejectedTieBreak));
    }

    #[tokio::test]
    async fn test_lww_priority_zero() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Write with priority 0 (lowest possible)
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            0,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
        let result1 = lww.merge(&ctx, &delta1).await.unwrap();
        assert!(matches!(result1, MergeResult::Applied));
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Second write with priority 0 should use tie-breaking
        // "Bob" > "Alice" so Bob should win
        let delta2 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            0,
            "v1".to_string(),
            b"Bob".to_vec(),
        )
        .unwrap();
        let result2 = lww.merge(&ctx, &delta2).await.unwrap();
        assert!(matches!(result2, MergeResult::Applied));
        assert_eq!(lww.value().await.unwrap(), b"Bob");
    }

    #[tokio::test]
    async fn test_lww_priority_max() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Write with priority u64::MAX (highest possible)
        let delta1 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            u64::MAX,
            "v1".to_string(),
            b"Alice".to_vec(),
        )
        .unwrap();
        let result1 = lww.merge(&ctx, &delta1).await.unwrap();
        assert!(matches!(result1, MergeResult::Applied));
        assert_eq!(lww.value().await.unwrap(), b"Alice");

        // Any subsequent write with lower priority should be rejected
        let delta2 = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            u64::MAX - 1,
            "v1".to_string(),
            b"Bob".to_vec(),
        )
        .unwrap();
        let result2 = lww.merge(&ctx, &delta2).await.unwrap();
        assert!(matches!(result2, MergeResult::RejectedLowerPriority { .. }));
        assert_eq!(lww.value().await.unwrap(), b"Alice");
    }

    #[tokio::test]
    async fn test_lww_field_name_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Delta with wrong field name
        let delta = LwwDelta::new(
            b"doc1".to_vec(),
            "wrong_field".to_string(),
            10,
            "v1".to_string(),
            b"value".to_vec(),
        )
        .unwrap();

        let result = lww.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("field name mismatch"));
    }

    #[tokio::test]
    async fn test_lww_schema_version_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut lww =
            Lww::new(store.clone(), "v1".to_string(), b"doc1", "name".to_string()).unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Delta with wrong schema version
        let delta = LwwDelta::new(
            b"doc1".to_vec(),
            "name".to_string(),
            10,
            "v2".to_string(),
            b"value".to_vec(),
        )
        .unwrap();

        let result = lww.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema version mismatch"));
    }

    #[test]
    fn test_lww_constructor_empty_schema_version() {
        let store = Arc::new(MemoryStore::new());
        let result = Lww::new(store, "".to_string(), b"doc1", "name".to_string());
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .to_string()
            .contains("schema_version_id cannot be empty"));
    }

    #[test]
    fn test_lww_constructor_empty_doc_id() {
        let store = Arc::new(MemoryStore::new());
        let result = Lww::new(store, "v1".to_string(), b"", "name".to_string());
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("doc_id cannot be empty"));
    }

    #[test]
    fn test_lww_constructor_empty_field_name() {
        let store = Arc::new(MemoryStore::new());
        let result = Lww::new(store, "v1".to_string(), b"doc1", "".to_string());
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("field_name cannot be empty"));
    }
}
