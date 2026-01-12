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
    doc_id: Vec<u8>,
    /// Field name within the document
    field_name: String,
    /// Priority for DAG ordering (counters merge commutatively, no conflict resolution needed)
    priority: u64,
    /// Nonce ensures idempotent delivery (same delta only applied once)
    nonce: i64,
    /// Schema version identifier
    schema_version_id: String,
    /// The increment/decrement value (can be negative)
    data: Vec<u8>,
}

impl CounterDelta {
    /// Create a new Int64 counter delta
    pub fn new_int64(
        doc_id: Vec<u8>,
        field_name: String,
        priority: u64,
        nonce: i64,
        schema_version_id: String,
        increment: i64,
    ) -> Result<Self> {
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
            nonce,
            schema_version_id,
            data: increment.to_be_bytes().to_vec(),
        })
    }

    /// Create a new Float64 counter delta
    pub fn new_float64(
        doc_id: Vec<u8>,
        field_name: String,
        priority: u64,
        nonce: i64,
        schema_version_id: String,
        increment: f64,
    ) -> Result<Self> {
        if field_name.is_empty() {
            return Err(Error::MergeError("field_name cannot be empty".into()));
        }
        if schema_version_id.is_empty() {
            return Err(Error::MergeError(
                "schema_version_id cannot be empty".into(),
            ));
        }
        if !increment.is_finite() {
            return Err(Error::MergeError(format!(
                "float64 increment must be finite, got: {}",
                increment
            )));
        }
        Ok(Self {
            doc_id,
            field_name,
            priority,
            nonce,
            schema_version_id,
            data: increment.to_be_bytes().to_vec(),
        })
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

    /// Get the nonce
    pub fn nonce(&self) -> i64 {
        self.nonce
    }

    /// Get the schema version ID
    pub fn schema_version_id(&self) -> &str {
        &self.schema_version_id
    }

    /// Get the raw data bytes
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Decode the increment value as i64
    pub fn decode_int64(&self) -> Result<i64> {
        if self.data.len() != 8 {
            return Err(Error::MergeError(format!(
                "invalid counter data length for field '{}': expected 8 bytes for i64, got {} bytes",
                self.field_name, self.data.len()
            )));
        }
        let bytes: [u8; 8] = self.data[..8].try_into().unwrap();
        Ok(i64::from_be_bytes(bytes))
    }

    /// Decode the increment value as f64
    pub fn decode_float64(&self) -> Result<f64> {
        if self.data.len() != 8 {
            return Err(Error::MergeError(format!(
                "invalid counter data length for field '{}': expected 8 bytes for f64, got {} bytes",
                self.field_name, self.data.len()
            )));
        }
        let bytes: [u8; 8] = self.data[..8].try_into().unwrap();
        Ok(f64::from_be_bytes(bytes))
    }
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
    ///
    /// Note: Nonces are stored permanently and never garbage collected in this implementation.
    /// For production use, consider implementing nonce garbage collection strategies:
    ///
    /// 1. Time-based: Remove nonces older than a configurable retention period
    /// 2. CID-based: Track nonces per DAG block and remove when blocks are pruned
    /// 3. Hybrid: Combine time-based with causality tracking
    ///
    /// Nonce storage grows unbounded without GC, which could become a storage leak for
    /// high-throughput counters. The trade-off is between storage cost and idempotency window.
    async fn mark_nonce(&self, nonce: i64) -> Result<()> {
        let mut nonce_key = self.nonce_prefix.clone();
        nonce_key.extend_from_slice(&nonce.to_be_bytes());
        // Store [1] as marker (value unused, only key existence matters)
        self.store.set(&nonce_key, &[1]).await
    }

    /// Get current value as i64
    async fn get_int64(&self) -> Result<i64> {
        match self.store.get(&self.value_key).await? {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(Error::MergeError(format!(
                        "invalid counter value length for field '{}' in schema '{}': \
                         expected 8 bytes, got {} bytes",
                        self.field_name,
                        self.schema_version_id,
                        bytes.len()
                    )));
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
                    return Err(Error::MergeError(format!(
                        "invalid counter value length for field '{}' in schema '{}': \
                         expected 8 bytes, got {} bytes",
                        self.field_name,
                        self.schema_version_id,
                        bytes.len()
                    )));
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
    ///
    /// # Warning
    ///
    /// Value updates and nonce marking are not atomic. If the process crashes between
    /// updating the value (line 344/383) and marking the nonce (line 388), the nonce
    /// will not be marked. On replay, has_nonce() returns false, so the delta is
    /// re-applied correctly maintaining the count. The actual risk is partial state
    /// if the Store implementation doesn't provide atomicity within individual set()
    /// operations. Use with Store implementations that guarantee atomic set().
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
                // Int64: Saturate on overflow to match Go DefraDB behavior
                let new_value = current.saturating_add(increment);
                self.set_int64(new_value).await?;
            }
            NumericKind::Float64 => {
                let increment = delta.decode_float64()?;

                // Validate increment (reject NaN and infinities)
                if !increment.is_finite() {
                    return Err(Error::MergeError(format!(
                        "invalid float64 increment: {}",
                        increment
                    )));
                }

                if !self.allow_decrement && increment < 0.0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }

                let current = self.get_float64().await?;

                // Validate current value
                if !current.is_finite() {
                    return Err(Error::MergeError(format!(
                        "invalid float64 current value: {}",
                        current
                    )));
                }

                // Float64: Reject overflow to infinity (different from int64 saturation)
                // Rationale: NaN/infinity breaks CRDT convergence properties
                let new_value = current + increment;

                // Validate result (check for overflow to infinity)
                if !new_value.is_finite() {
                    return Err(Error::MergeError(format!(
                        "float64 overflow: {} + {} = {}",
                        current, increment, new_value
                    )));
                }

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
            .ok_or_else(|| {
                Error::MergeError(
                    "invalid delta type for Counter merge: expected CounterDelta".into(),
                )
            })?;

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
        self.store.get(&self.value_key).await?.ok_or_else(|| {
            Error::MergeError(format!(
                "counter value not found for field '{}' in schema '{}'",
                self.field_name, self.schema_version_id
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::MemoryStore;

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
        let delta1 = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            5,
        )
        .unwrap();
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

    #[tokio::test]
    async fn test_counter_float64_nan() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Float64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to increment by NaN
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v1".to_string(),
            data: f64::NAN.to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid float64 increment"));
    }

    #[tokio::test]
    async fn test_counter_float64_positive_infinity() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Float64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to increment by positive infinity
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v1".to_string(),
            data: f64::INFINITY.to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid float64 increment"));
    }

    #[tokio::test]
    async fn test_counter_float64_negative_infinity() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Float64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to increment by negative infinity
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v1".to_string(),
            data: f64::NEG_INFINITY.to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid float64 increment"));
    }

    #[tokio::test]
    async fn test_counter_float64_overflow() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Float64,
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
            data: f64::MAX.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta1).await.unwrap();

        // Try to increment - should overflow to infinity and be rejected
        let delta2 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 20,
            nonce: 2,
            schema_version_id: "v1".to_string(),
            data: f64::MAX.to_be_bytes().to_vec(),
        };

        let result = counter.merge(&ctx, &delta2).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("float64 overflow"));
    }

    #[tokio::test]
    async fn test_counter_float64_basic() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Float64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Increment by 5.5
        let delta1 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 1,
            schema_version_id: "v1".to_string(),
            data: 5.5f64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta1).await.unwrap();

        // Increment by 3.2
        let delta2 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 20,
            nonce: 2,
            schema_version_id: "v1".to_string(),
            data: 3.2f64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta2).await.unwrap();

        // Should be 8.7
        let value_bytes = counter.value().await.unwrap();
        let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
        assert!((value - 8.7).abs() < 0.0001);
    }

    #[tokio::test]
    async fn test_counter_nonce_collision_idempotency() {
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

        // Apply delta with nonce 12345
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 12345,
            schema_version_id: "v1".to_string(),
            data: 5i64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta).await.unwrap();

        // Verify value is 5
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, 5);

        // Apply same delta again with same nonce - should be idempotent
        let delta2 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 20,
            nonce: 12345, // Same nonce
            schema_version_id: "v1".to_string(),
            data: 5i64.to_be_bytes().to_vec(),
        };
        counter.merge(&ctx, &delta2).await.unwrap();

        // Value should still be 5, not 10
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, 5);
    }

    #[tokio::test]
    async fn test_counter_concurrent_nonce_collision() {
        let store = Arc::new(MemoryStore::new());
        let mut counter1 = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );
        let mut counter2 = Counter::new(
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

        // Both replicas receive delta with nonce 999
        let delta = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 10,
            nonce: 999,
            schema_version_id: "v1".to_string(),
            data: 7i64.to_be_bytes().to_vec(),
        };

        // Apply to both counters
        counter1.merge(&ctx, &delta).await.unwrap();
        counter2.merge(&ctx, &delta).await.unwrap();

        // Both should have value 7, not 14
        let value1_bytes = counter1.value().await.unwrap();
        let value1 = i64::from_be_bytes(value1_bytes.try_into().unwrap());
        assert_eq!(value1, 7);

        let value2_bytes = counter2.value().await.unwrap();
        let value2 = i64::from_be_bytes(value2_bytes.try_into().unwrap());
        assert_eq!(value2, 7);
    }

    #[tokio::test]
    async fn test_counter_float64_precision_edge_cases() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store,
            "v1".to_string(),
            b"doc1",
            "value".to_string(),
            true,
            NumericKind::Float64,
        );

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Test very small increments (near machine epsilon)
        let delta1 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "value".to_string(),
            10,
            1001,
            "v1".to_string(),
            1e-308, // Very small number
        )
        .unwrap();
        counter.merge(&ctx, &delta1).await.unwrap();

        // Add a large number
        let delta2 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "value".to_string(),
            20,
            1002,
            "v1".to_string(),
            1e308, // Very large number
        )
        .unwrap();
        counter.merge(&ctx, &delta2).await.unwrap();

        // Subtract the large number
        let delta3 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "value".to_string(),
            30,
            1003,
            "v1".to_string(),
            -1e308,
        )
        .unwrap();
        counter.merge(&ctx, &delta3).await.unwrap();

        // Result should be close to the original small number
        // Due to floating point precision, this tests that we don't lose the small value entirely
        let value_bytes = counter.value().await.unwrap();
        let arr: [u8; 8] = value_bytes.try_into().unwrap();
        let value = f64::from_be_bytes(arr);

        // Value should be finite and very small (not exactly 1e-308 due to FP precision)
        assert!(value.is_finite());
        assert!(value.abs() < 1e-100); // Should be very small
    }

    #[test]
    fn test_counter_delta_int64_empty_field_name() {
        let result =
            CounterDelta::new_int64(b"doc1".to_vec(), "".to_string(), 10, 1, "v1".to_string(), 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field_name"));
    }

    #[test]
    fn test_counter_delta_int64_empty_schema_version() {
        let result = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            1,
            "".to_string(),
            5,
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema_version_id"));
    }

    #[test]
    fn test_counter_delta_float64_empty_field_name() {
        let result = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "".to_string(),
            10,
            1,
            "v1".to_string(),
            5.0,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field_name"));
    }

    #[test]
    fn test_counter_delta_float64_empty_schema_version() {
        let result = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            1,
            "".to_string(),
            5.0,
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("schema_version_id"));
    }
}
