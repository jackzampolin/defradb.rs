//! Counter CRDT implementation (PN-Counter)
//!
//! Supports both increment and decrement operations with commutative merge semantics.
//! Counters use nonces to ensure unique DAG blocks for idempotent delivery.

use crate::traits::{Context, Delta, ReplicatedData, ValueReader};
use async_trait::async_trait;
use defra_core::{store::Store, Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;

/// Numeric kind for counter values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumericKind {
    Int64,
    Float64,
}

/// Counter Delta - represents an increment/decrement operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterDelta {
    /// Document ID this delta applies to
    pub doc_id: Vec<u8>,
    /// Field name within the document
    pub field_name: String,
    /// Priority for ordering (not used for conflict resolution in counters)
    pub priority: u64,
    /// Random nonce for uniqueness (prevents duplicate application)
    pub nonce: i64,
    /// Schema version identifier
    pub schema_version_id: String,
    /// The increment/decrement value (can be negative)
    pub data: Vec<u8>,
}

impl Delta for CounterDelta {
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

impl CounterDelta {
    /// Decode the increment value as i64
    pub fn decode_int64(&self) -> Result<i64> {
        if self.data.len() != 8 {
            return Err(Error::MergeError("invalid counter data length".into()));
        }
        let bytes: [u8; 8] = self.data[..8].try_into().unwrap();
        Ok(i64::from_be_bytes(bytes))
    }

    /// Decode the increment value as f64
    pub fn decode_float64(&self) -> Result<f64> {
        if self.data.len() != 8 {
            return Err(Error::MergeError("invalid counter data length".into()));
        }
        let bytes: [u8; 8] = self.data[..8].try_into().unwrap();
        Ok(f64::from_be_bytes(bytes))
    }
}

/// Counter CRDT - supports increment/decrement with commutative merge
pub struct Counter {
    /// Storage backend
    store: Arc<dyn Store>,
    /// Storage key for the counter value
    value_key: Vec<u8>,
    /// Storage key for tracking applied nonces
    nonce_prefix: Vec<u8>,
    /// Schema version
    schema_version_id: String,
    /// Field name
    field_name: String,
    /// Whether decrement is allowed
    allow_decrement: bool,
    /// Numeric kind (int or float)
    kind: NumericKind,
}

impl Counter {
    /// Create a new Counter CRDT
    pub fn new(
        store: Arc<dyn Store>,
        schema_version_id: String,
        doc_id: &[u8],
        field_name: String,
        allow_decrement: bool,
        kind: NumericKind,
    ) -> Self {
        // Construct storage keys
        let mut value_key = Vec::new();
        value_key.extend_from_slice(b"/data/");
        value_key.extend_from_slice(schema_version_id.as_bytes());
        value_key.push(b'/');
        value_key.extend_from_slice(doc_id);
        value_key.push(b'/');
        value_key.extend_from_slice(field_name.as_bytes());

        // Nonce tracking prefix
        let mut nonce_prefix = value_key.clone();
        nonce_prefix.extend_from_slice(b"/nonces/");

        Self {
            store,
            value_key,
            nonce_prefix,
            schema_version_id,
            field_name,
            allow_decrement,
            kind,
        }
    }

    /// Check if a nonce has been applied
    async fn has_nonce(&self, nonce: i64) -> Result<bool> {
        let mut nonce_key = self.nonce_prefix.clone();
        nonce_key.extend_from_slice(&nonce.to_be_bytes());
        self.store.has(&nonce_key).await
    }

    /// Mark a nonce as applied
    async fn mark_nonce(&self, nonce: i64) -> Result<()> {
        let mut nonce_key = self.nonce_prefix.clone();
        nonce_key.extend_from_slice(&nonce.to_be_bytes());
        self.store.set(&nonce_key, &[1]).await
    }

    /// Get current value as i64
    async fn get_int64(&self) -> Result<i64> {
        match self.store.get(&self.value_key).await? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(Error::MergeError("invalid counter value length".into()));
                }
                let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                Ok(i64::from_be_bytes(arr))
            }
            None => Ok(0),
        }
    }

    /// Set value as i64
    async fn set_int64(&self, value: i64) -> Result<()> {
        self.store.set(&self.value_key, &value.to_be_bytes()).await
    }

    /// Get current value as f64
    async fn get_float64(&self) -> Result<f64> {
        match self.store.get(&self.value_key).await? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(Error::MergeError("invalid counter value length".into()));
                }
                let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                Ok(f64::from_be_bytes(arr))
            }
            None => Ok(0.0),
        }
    }

    /// Set value as f64
    async fn set_float64(&self, value: f64) -> Result<()> {
        self.store.set(&self.value_key, &value.to_be_bytes()).await
    }

    /// Apply an increment/decrement
    async fn apply_delta(&mut self, delta: &CounterDelta) -> Result<()> {
        // Check if nonce already applied (idempotency)
        if self.has_nonce(delta.nonce).await? {
            return Ok(()); // Already applied
        }

        // Decode and apply based on kind
        match self.kind {
            NumericKind::Int64 => {
                let increment = delta.decode_int64()?;
                if !self.allow_decrement && increment < 0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }
                let current = self.get_int64().await?;
                let new_value = current.saturating_add(increment);
                self.set_int64(new_value).await?;
            }
            NumericKind::Float64 => {
                let increment = delta.decode_float64()?;
                if !self.allow_decrement && increment < 0.0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }
                let current = self.get_float64().await?;
                let new_value = current + increment;
                self.set_float64(new_value).await?;
            }
        }

        // Mark nonce as applied
        self.mark_nonce(delta.nonce).await?;

        Ok(())
    }
}

#[async_trait]
impl ReplicatedData for Counter {
    async fn merge(&mut self, _ctx: &Context, delta: &dyn Delta) -> Result<()> {
        // Downcast to CounterDelta
        let counter_delta = delta
            .as_any()
            .downcast_ref::<CounterDelta>()
            .ok_or_else(|| Error::MergeError("invalid delta type".into()))?;

        // Validate field name
        if counter_delta.field_name != self.field_name {
            return Err(Error::MergeError(format!(
                "field name mismatch: expected {}, got {}",
                self.field_name, counter_delta.field_name
            )));
        }

        // Validate schema version
        if counter_delta.schema_version_id != self.schema_version_id {
            return Err(Error::MergeError(format!(
                "schema version mismatch: expected {}, got {}",
                self.schema_version_id, counter_delta.schema_version_id
            )));
        }

        // Apply delta
        self.apply_delta(counter_delta).await
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
impl ValueReader for Counter {
    async fn value(&self) -> Result<Vec<u8>> {
        self.store
            .get(&self.value_key)
            .await?
            .ok_or_else(|| Error::MergeError("counter value not found".into()))
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
    async fn test_counter_increment() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Increment by 5
        let delta1 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 12345,
            schema_version_id: "v1".to_string(),
            data: 5i64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta1).await.unwrap();

        // Increment by 3
        let delta2 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 20,
            nonce: 12346,
            schema_version_id: "v1".to_string(),
            data: 3i64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta2).await.unwrap();

        // Should be 8
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, 8);
    }

    #[tokio::test]
    async fn test_counter_idempotency() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Apply same delta twice
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 12345,
            schema_version_id: "v1".to_string(),
            data: 5i64.to_be_bytes().to_vec(),
        };

        counter.merge(&ctx, &delta).await.unwrap();
        counter.merge(&ctx, &delta).await.unwrap(); // Should be ignored

        // Should still be 5 (not 10)
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, 5);
    }

    #[tokio::test]
    async fn test_counter_decrement_not_allowed() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            false, // Decrement not allowed
            NumericKind::Int64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to decrement
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 12345,
            schema_version_id: "v1".to_string(),
            data: (-5i64).to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_counter_overflow_saturating() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Set counter to near max
        let delta1 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v1".to_string(),
            data: (i64::MAX - 10).to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta1).await.unwrap();

        // Try to increment beyond max - should saturate
        let delta2 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 20,
            nonce: 2,
            schema_version_id: "v1".to_string(),
            data: 20i64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta2).await.unwrap();

        // Should saturate at i64::MAX
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, i64::MAX);
    }

    #[tokio::test]
    async fn test_counter_field_name_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Delta for wrong field
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "wrong_field".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v1".to_string(),
            data: 5i64.to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("field name mismatch"));
    }

    #[tokio::test]
    async fn test_counter_schema_version_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Delta for wrong schema version
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v2".to_string(),
            data: 5i64.to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema version mismatch"));
    }
}
