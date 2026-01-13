//! Counter CRDT implementation
//!
//! Supports increment and decrement operations with nonce-based idempotent delivery.
//! Uses commutative addition with wrapping on overflow for Int64 (matching Go DefraDB),
//! and error on overflow for Float64. This is not a traditional PN-Counter (which uses
//! separate per-replica counters); instead it uses a single value with nonce tracking.

use crate::traits::{Context, Delta, MergeResult, ReplicatedData, ValueReader};
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

/// Internal helper for computed new values (used in apply_delta)
enum NewValue {
    Int64(i64),
    Float64(f64),
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
    /// Numeric kind (Int64 or Float64)
    kind: NumericKind,
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
            nonce,
            schema_version_id,
            data: increment.to_be_bytes().to_vec(),
            kind: NumericKind::Int64,
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
            kind: NumericKind::Float64,
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

    /// Get the numeric kind
    pub fn kind(&self) -> NumericKind {
        self.kind
    }

    /// Decode the increment value as i64
    pub fn decode_int64(&self) -> Result<i64> {
        if self.data.len() != 8 {
            return Err(Error::MergeError(format!(
                "invalid counter data length for field '{}': expected 8 bytes for i64, got {} bytes",
                self.field_name, self.data.len()
            )));
        }
        let bytes: [u8; 8] = self.data[..8]
            .try_into()
            .expect("length already validated as 8 bytes");
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
        let bytes: [u8; 8] = self.data[..8]
            .try_into()
            .expect("length already validated as 8 bytes");
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
    ///
    /// # Arguments
    /// * `store` - Storage backend
    /// * `schema_version_id` - Schema version identifier (must not be empty)
    /// * `doc_id` - Document identifier (must not be empty)
    /// * `field_name` - Field name (must not be empty)
    /// * `allow_decrement` - Whether negative increments are allowed
    /// * `kind` - Numeric type (Int64 or Float64)
    ///
    /// # Errors
    /// Returns an error if schema_version_id, doc_id, or field_name is empty.
    pub fn new(
        store: Arc<dyn Store>,
        schema_version_id: String,
        doc_id: &[u8],
        field_name: String,
        allow_decrement: bool,
        kind: NumericKind,
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

        Ok(Self {
            store,
            value_key,
            nonce_prefix,
            schema_version_id,
            field_name,
            allow_decrement,
            kind,
        })
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
                let arr: [u8; 8] = bytes[..8]
                    .try_into()
                    .expect("length already validated as 8 bytes");
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
                let arr: [u8; 8] = bytes[..8]
                    .try_into()
                    .expect("length already validated as 8 bytes");
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
    /// # Crash Recovery Semantics
    ///
    /// Nonce marking and value updates are not atomic. To ensure safety:
    /// - Nonce is marked FIRST, then value is updated
    /// - If crash occurs after nonce but before value update: delta is lost (under-count)
    /// - If crash occurred with old ordering (value then nonce): would double-count
    ///
    /// Under-counting on crash is safer than over-counting because:
    /// 1. It's easier to detect missing deltas than duplicate applications
    /// 2. Over-counting violates CRDT idempotency guarantees
    ///
    /// For true atomicity, use a Store implementation with transaction support.
    async fn apply_delta(&mut self, delta: &CounterDelta) -> Result<MergeResult> {
        // Validate numeric kind matches
        if delta.kind() != self.kind {
            return Err(Error::MergeError(format!(
                "numeric kind mismatch for field '{}': counter is {:?}, delta is {:?}",
                self.field_name,
                self.kind,
                delta.kind()
            )));
        }

        // Check if nonce already applied (idempotency)
        if self.has_nonce(delta.nonce).await? {
            return Ok(MergeResult::SkippedAlreadyApplied { nonce: delta.nonce });
        }

        // Decode and validate based on kind BEFORE any state changes
        let new_value = match self.kind {
            NumericKind::Int64 => {
                let increment = delta.decode_int64()?;
                if !self.allow_decrement && increment < 0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }
                let current = self.get_int64().await?;
                // Int64: Wrap on overflow to match Go DefraDB behavior
                NewValue::Int64(current.wrapping_add(increment))
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
                let result = current + increment;

                // Validate result (check for overflow to infinity)
                if !result.is_finite() {
                    return Err(Error::MergeError(format!(
                        "float64 overflow: {} + {} = {}",
                        current, increment, result
                    )));
                }

                NewValue::Float64(result)
            }
        };

        // Mark nonce FIRST to prevent double-counting on crash recovery
        self.mark_nonce(delta.nonce).await?;

        // Then update value
        match new_value {
            NewValue::Int64(v) => self.set_int64(v).await?,
            NewValue::Float64(v) => self.set_float64(v).await?,
        }

        Ok(MergeResult::Applied)
    }
}

#[async_trait]
impl ReplicatedData for Counter {
    async fn merge(&mut self, _ctx: &Context, delta: &dyn Delta) -> Result<MergeResult> {
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
        )
        .unwrap();

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
            kind: NumericKind::Int64,
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
        )
        .unwrap();

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
            kind: NumericKind::Int64,
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
        )
        .unwrap();

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
            kind: NumericKind::Int64,
        };

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_counter_overflow_wrapping() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();

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
            kind: NumericKind::Int64,
        };
        counter.merge(&ctx, &delta1).await.unwrap();

        // Try to increment beyond max - should wrap to negative (matching Go behavior)
        let delta2 = CounterDelta {
            doc_id: b"doc1".to_vec(),
            field_name: "count".to_string(),
            priority: 20,
            nonce: 2,
            schema_version_id: "v1".to_string(),
            data: 20i64.to_be_bytes().to_vec(),
            kind: NumericKind::Int64,
        };
        counter.merge(&ctx, &delta2).await.unwrap();

        // Should wrap: (i64::MAX - 10) + 20 = i64::MIN + 9
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, (i64::MAX - 10).wrapping_add(20));
        assert_eq!(value, i64::MIN + 9); // Verify wrapping behavior
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
        )
        .unwrap();

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
            kind: NumericKind::Int64,
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
        )
        .unwrap();

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
            kind: NumericKind::Int64,
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
        )
        .unwrap();

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
            kind: NumericKind::Float64,
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
        )
        .unwrap();

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
            kind: NumericKind::Float64,
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
        )
        .unwrap();

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
            kind: NumericKind::Float64,
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
        )
        .unwrap();

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
            kind: NumericKind::Float64,
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
            kind: NumericKind::Float64,
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
        )
        .unwrap();

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
            kind: NumericKind::Float64,
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
            kind: NumericKind::Float64,
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
        )
        .unwrap();

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
            kind: NumericKind::Int64,
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
            kind: NumericKind::Int64,
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
        )
        .unwrap();
        let mut counter2 = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();

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
            kind: NumericKind::Int64,
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
        // This test documents float64 precision behavior.
        // When adding very small numbers to very large numbers,
        // the small numbers are lost due to floating-point precision limits.
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store,
            "v1".to_string(),
            b"doc1",
            "value".to_string(),
            true,
            NumericKind::Float64,
        )
        .unwrap();

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

        let value_bytes = counter.value().await.unwrap();
        let arr: [u8; 8] = value_bytes.try_into().unwrap();
        let value = f64::from_be_bytes(arr);

        // Due to floating-point precision limits:
        // 1e-308 + 1e308 = 1e308 (small number is lost)
        // 1e308 - 1e308 = 0.0
        // Result is exactly 0.0, NOT 1e-308
        // This is expected IEEE 754 behavior, not a bug.
        assert!(value.is_finite());
        assert_eq!(value, 0.0, "precision loss is expected: 1e-308 + 1e308 - 1e308 = 0");
    }

    #[tokio::test]
    async fn test_counter_float64_precision_preserved_within_range() {
        // When numbers are within similar magnitude, precision is preserved
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store,
            "v1".to_string(),
            b"doc1",
            "value".to_string(),
            true,
            NumericKind::Float64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Add numbers of similar magnitude
        let delta1 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "value".to_string(),
            10,
            1001,
            "v1".to_string(),
            1.0,
        )
        .unwrap();
        counter.merge(&ctx, &delta1).await.unwrap();

        let delta2 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "value".to_string(),
            20,
            1002,
            "v1".to_string(),
            0.1,
        )
        .unwrap();
        counter.merge(&ctx, &delta2).await.unwrap();

        let delta3 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "value".to_string(),
            30,
            1003,
            "v1".to_string(),
            0.01,
        )
        .unwrap();
        counter.merge(&ctx, &delta3).await.unwrap();

        let value_bytes = counter.value().await.unwrap();
        let arr: [u8; 8] = value_bytes.try_into().unwrap();
        let value = f64::from_be_bytes(arr);

        // 1.0 + 0.1 + 0.01 = 1.11
        // Precision is preserved because numbers are similar magnitude
        assert!((value - 1.11).abs() < 1e-10, "precision preserved: got {}", value);
    }

    #[test]
    fn test_counter_delta_validation_rejects_empty_values() {
        // Test that empty doc_id, field_name, and schema_version are rejected
        // for both int64 and float64 constructors

        // Empty doc_id
        let result =
            CounterDelta::new_int64(Vec::new(), "count".to_string(), 10, 1, "v1".to_string(), 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("doc_id"));

        let result = CounterDelta::new_float64(
            Vec::new(),
            "count".to_string(),
            10,
            1,
            "v1".to_string(),
            5.0,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("doc_id"));

        // Empty field_name
        let result =
            CounterDelta::new_int64(b"doc1".to_vec(), "".to_string(), 10, 1, "v1".to_string(), 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field_name"));

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

        // Empty schema_version_id
        let result = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            1,
            "".to_string(),
            5,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema_version_id"));

        let result = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            1,
            "".to_string(),
            5.0,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema_version_id"));
    }

    #[tokio::test]
    async fn test_counter_wrong_delta_type() {
        use crate::LwwDelta;

        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to merge an LwwDelta into a Counter
        let wrong_delta = LwwDelta::new(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            "v1".to_string(),
            b"value".to_vec(),
        )
        .unwrap();

        let result = counter.merge(&ctx, &wrong_delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid delta type for Counter"));
    }

    #[tokio::test]
    async fn test_counter_float64_decrement_not_allowed() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            false, // Decrement not allowed
            NumericKind::Float64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to decrement with float64
        let delta = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            -5.0,
        )
        .unwrap();

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("decrement not allowed"));
    }

    #[tokio::test]
    async fn test_counter_float64_successful_decrement() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true, // Decrement allowed
            NumericKind::Float64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // First increment
        let delta1 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            1,
            "v1".to_string(),
            10.0,
        )
        .unwrap();
        counter.merge(&ctx, &delta1).await.unwrap();

        // Then decrement
        let delta2 = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            20,
            2,
            "v1".to_string(),
            -3.0,
        )
        .unwrap();
        counter.merge(&ctx, &delta2).await.unwrap();

        // Should be 7.0
        let value_bytes = counter.value().await.unwrap();
        let value = f64::from_be_bytes(value_bytes.try_into().unwrap());
        assert!((value - 7.0).abs() < 0.0001);
    }

    #[tokio::test]
    async fn test_counter_numeric_kind_mismatch() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64, // Int64 counter
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to apply a Float64 delta to an Int64 counter
        let delta = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            5.0,
        )
        .unwrap();

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("numeric kind mismatch"));
    }

    #[tokio::test]
    async fn test_counter_underflow_wrapping() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true, // Allow decrement
            NumericKind::Int64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Set counter to near minimum
        let delta1 = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            1,
            "v1".to_string(),
            i64::MIN + 10,
        )
        .unwrap();
        counter.merge(&ctx, &delta1).await.unwrap();

        // Try to decrement beyond minimum - should wrap to positive (matching Go behavior)
        let delta2 = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            20,
            2,
            "v1".to_string(),
            -20,
        )
        .unwrap();
        counter.merge(&ctx, &delta2).await.unwrap();

        // Should wrap: (i64::MIN + 10) + (-20) = i64::MAX - 9
        let value_bytes = counter.value().await.unwrap();
        let value = i64::from_be_bytes(value_bytes.try_into().unwrap());
        assert_eq!(value, (i64::MIN + 10).wrapping_add(-20));
        assert_eq!(value, i64::MAX - 9); // Verify wrapping behavior
    }

    #[tokio::test]
    async fn test_counter_merge_result_applied() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        let delta = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            5,
        )
        .unwrap();

        let result = counter.merge(&ctx, &delta).await.unwrap();
        assert!(matches!(result, MergeResult::Applied));
    }

    #[tokio::test]
    async fn test_counter_merge_result_skipped_already_applied() {
        let store = Arc::new(MemoryStore::new());
        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        let delta = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            5,
        )
        .unwrap();

        // First merge should apply
        let result1 = counter.merge(&ctx, &delta).await.unwrap();
        assert!(matches!(result1, MergeResult::Applied));

        // Second merge with same nonce should be skipped
        let result2 = counter.merge(&ctx, &delta).await.unwrap();
        assert!(matches!(
            result2,
            MergeResult::SkippedAlreadyApplied { nonce: 12345 }
        ));
    }

    #[tokio::test]
    async fn test_counter_corrupted_storage_int64() {
        let store = Arc::new(MemoryStore::new());

        // Manually insert corrupted data (wrong length)
        let value_key = b"/data/v1/doc1/count".to_vec();
        store.set(&value_key, b"short").await.unwrap();

        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to apply a delta - should fail when reading corrupted current value
        let delta = CounterDelta::new_int64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            5,
        )
        .unwrap();

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected 8 bytes"));
    }

    #[tokio::test]
    async fn test_counter_corrupted_storage_float64() {
        let store = Arc::new(MemoryStore::new());

        // Manually insert corrupted data (wrong length)
        let value_key = b"/data/v1/doc1/count".to_vec();
        store.set(&value_key, b"too_short").await.unwrap();

        let mut counter = Counter::new(
            store.clone(),
            "v1".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Float64,
        )
        .unwrap();

        let ctx = Context {
            doc_id: defra_core::types::DocId::new("doc1"),
            schema_version: "v1".to_string(),
        };

        // Try to apply a delta - should fail when reading corrupted current value
        let delta = CounterDelta::new_float64(
            b"doc1".to_vec(),
            "count".to_string(),
            10,
            12345,
            "v1".to_string(),
            5.0,
        )
        .unwrap();

        let result = counter.merge(&ctx, &delta).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected 8 bytes"));
    }

    #[test]
    fn test_counter_constructor_empty_schema_version() {
        let store = Arc::new(MemoryStore::new());
        let result = Counter::new(
            store,
            "".to_string(),
            b"doc1",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .to_string()
            .contains("schema_version_id cannot be empty"));
    }

    #[test]
    fn test_counter_constructor_empty_doc_id() {
        let store = Arc::new(MemoryStore::new());
        let result = Counter::new(
            store,
            "v1".to_string(),
            b"",
            "count".to_string(),
            true,
            NumericKind::Int64,
        );
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("doc_id cannot be empty"));
    }

    #[test]
    fn test_counter_constructor_empty_field_name() {
        let store = Arc::new(MemoryStore::new());
        let result = Counter::new(
            store,
            "v1".to_string(),
            b"doc1",
            "".to_string(),
            true,
            NumericKind::Int64,
        );
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("field_name cannot be empty"));
    }
}
