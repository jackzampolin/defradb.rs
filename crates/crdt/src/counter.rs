//! Counter CRDT implementation
//!
//! Supports increment and decrement operations with nonce-based idempotent delivery.
//! Uses commutative addition with wrapping on overflow for Int64 (matching Go DefraDB),
//! and error on overflow for Float64. This is not a traditional PN-Counter (which uses
//! separate per-replica counters); instead it uses a single value with nonce tracking.

use crate::traits::{Context, Delta, MergeResult, ReplicatedData, ValueReader};
use async_trait::async_trait;
use defra_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use storage::{Reader, ReaderWriter};

/// Numeric kind for counter values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// The increment/decrement value as big-endian bytes (can be negative)
    data: [u8; 8],
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
            data: increment.to_be_bytes(),
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
            data: increment.to_be_bytes(),
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
        Ok(i64::from_be_bytes(self.data))
    }

    /// Decode the increment value as f64
    pub fn decode_float64(&self) -> Result<f64> {
        Ok(f64::from_be_bytes(self.data))
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
///
/// This CRDT does not own storage. Instead, it receives a `ReaderWriter`
/// reference for each operation, matching Go DefraDB's pattern where
/// CRDTs operate on a provided `corekv.ReaderWriter`.
pub struct Counter {
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
    /// * `schema_version_id` - Schema version identifier (must not be empty)
    /// * `doc_id` - Document identifier (must not be empty)
    /// * `field_name` - Field name (must not be empty)
    /// * `allow_decrement` - Whether negative increments are allowed
    /// * `kind` - Numeric type (Int64 or Float64)
    ///
    /// # Errors
    /// Returns an error if schema_version_id, doc_id, or field_name is empty.
    pub fn new(
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
            value_key,
            nonce_prefix,
            schema_version_id,
            field_name,
            allow_decrement,
            kind,
        })
    }

    /// Check if a nonce has been applied
    async fn has_nonce(&self, reader: &dyn Reader, nonce: i64) -> Result<bool> {
        let mut nonce_key = self.nonce_prefix.clone();
        nonce_key.extend_from_slice(&nonce.to_be_bytes());
        reader
            .has(&nonce_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
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
    async fn mark_nonce(&self, rw: &mut dyn ReaderWriter, nonce: i64) -> Result<()> {
        let mut nonce_key = self.nonce_prefix.clone();
        nonce_key.extend_from_slice(&nonce.to_be_bytes());
        // Store [1] as marker (value unused, only key existence matters)
        rw.set(&nonce_key, &[1])
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Get current value as i64
    async fn get_int64(&self, reader: &dyn Reader) -> Result<i64> {
        match reader
            .get(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
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
    async fn set_int64(&self, rw: &mut dyn ReaderWriter, value: i64) -> Result<()> {
        rw.set(&self.value_key, &value.to_be_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Get current value as f64
    async fn get_float64(&self, reader: &dyn Reader) -> Result<f64> {
        match reader
            .get(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
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
    async fn set_float64(&self, rw: &mut dyn ReaderWriter, value: f64) -> Result<()> {
        rw.set(&self.value_key, &value.to_be_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))
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
    async fn apply_delta(
        &self,
        rw: &mut dyn ReaderWriter,
        delta: &CounterDelta,
        is_create: bool,
    ) -> Result<MergeResult> {
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
        if !is_create && self.has_nonce(rw, delta.nonce).await? {
            return Ok(MergeResult::SkippedAlreadyApplied { nonce: delta.nonce });
        }

        // Decode and validate based on kind BEFORE any state changes
        let new_value = match self.kind {
            NumericKind::Int64 => {
                let increment = delta.decode_int64()?;
                if !self.allow_decrement && increment < 0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }
                let current = if is_create {
                    0
                } else {
                    self.get_int64(rw).await?
                };
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

                let current = if is_create {
                    0.0
                } else {
                    self.get_float64(rw).await?
                };

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
        self.mark_nonce(rw, delta.nonce).await?;

        // Then update value
        match new_value {
            NewValue::Int64(v) => self.set_int64(rw, v).await?,
            NewValue::Float64(v) => self.set_float64(rw, v).await?,
        }

        Ok(MergeResult::Applied)
    }

    /// Seed the counter CRDT storage from the document's current field value
    /// if it hasn't been initialized yet.
    ///
    /// Local document creation stores counter values in the document layer but not
    /// in CRDT accumulation storage. Before merging a remote delta, the CRDT storage
    /// must be seeded from the document value to ensure correct accumulation.
    /// Also marks nonce=0 as applied since the initial creation already accounts for it.
    ///
    /// Returns true if seeding was performed, false if already initialized.
    pub async fn seed_if_uninitialized_int64(
        &self,
        rw: &mut dyn ReaderWriter,
        value: i64,
    ) -> Result<bool> {
        let has_value = rw
            .has(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        if has_value {
            return Ok(false);
        }
        self.set_int64(rw, value).await?;
        self.mark_nonce(rw, 0).await?;
        Ok(true)
    }

    /// Float64 variant of `seed_if_uninitialized_int64`.
    pub async fn seed_if_uninitialized_float64(
        &self,
        rw: &mut dyn ReaderWriter,
        value: f64,
    ) -> Result<bool> {
        let has_value = rw
            .has(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        if has_value {
            return Ok(false);
        }
        self.set_float64(rw, value).await?;
        self.mark_nonce(rw, 0).await?;
        Ok(true)
    }

    /// Get current value bytes (for internal use)
    async fn get_value_internal(&self, reader: &dyn Reader) -> Result<Vec<u8>> {
        reader
            .get(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .ok_or_else(|| {
                Error::MergeError(format!(
                    "counter value not found for field '{}' in schema '{}'",
                    self.field_name, self.schema_version_id
                ))
            })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ReplicatedData for Counter {
    async fn merge(
        &self,
        rw: &mut dyn ReaderWriter,
        ctx: &Context,
        delta: &dyn Delta,
    ) -> Result<MergeResult> {
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
        self.apply_delta(rw, counter_delta, ctx.is_create).await
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ValueReader for Counter {
    async fn value(&self, reader: &dyn Reader) -> Result<Vec<u8>> {
        self.get_value_internal(reader).await
    }
}
