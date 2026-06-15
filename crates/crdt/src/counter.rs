//! Counter CRDT implementation
//!
//! Supports increment and decrement operations. Uses commutative addition with
//! wrapping on overflow for Int64 (matching Go DefraDB), and IEEE-754 addition
//! for Float64. This is not a traditional PN-Counter (which uses separate
//! per-replica counters); instead it uses a single accumulated value.
//!
//! Merge is unconditional — idempotency is enforced upstream by the blockstore
//! (`is_merged(cid)` / `get_unmerged()`) on every ingest path. Matches Go's
//! counter Merge which also ignores the delta nonce.

use crate::traits::{Context, Delta, MergeResult, ReplicatedData, ValueReader};
use async_trait::async_trait;
use defra_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use storage::{corekv::Key, keys::CRDTValueKey, Reader, ReaderWriter};

/// Numeric kind for counter values.
///
/// # Convergence warning (Float32/Float64)
///
/// Counter merge is unconditional addition. For `Int64` this is order-independent
/// (wrapping addition is associative — proven in `proofs/lean` as `word64Add_assoc`).
/// For `Float32`/`Float64` it is **NOT**: IEEE-754 addition is not associative
/// (`(0.1 + 0.2) + 0.3 != 0.1 + (0.2 + 0.3)`; proven as `float_add_not_assoc`), so two
/// replicas applying the same float deltas in different merge orders can converge to
/// **different values**. This matches Go DefraDB exactly (both use raw IEEE-754 `+`), so
/// the behavior is retained for parity — but float counter fields are not convergence-safe.
/// Prefer `Int64` (or a fixed-point/scaled integer) when cross-replica convergence matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum NumericKind {
    Int64,
    /// Non-convergent across replicas — see the type-level convergence warning above.
    Float32,
    /// Non-convergent across replicas — see the type-level convergence warning above.
    Float64,
}

/// Internal helper for computed new values (used in apply_delta)
enum NewValue {
    Int64(i64),
    Float32(f32),
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
    /// The increment/decrement value as big-endian bytes.
    /// Int64/Float64: 8 bytes, Float32: 4 bytes.
    data: Vec<u8>,
    /// Numeric kind (Int64, Float32, or Float64)
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

    /// Create a new Float32 counter delta.
    ///
    /// Matches Go's `FieldKind_NILLABLE_FLOAT32` path. Accumulates with
    /// f32 precision to produce identical results to Go's `validateAndIncrement[float32]`.
    pub fn new_float32(
        doc_id: Vec<u8>,
        field_name: String,
        priority: u64,
        nonce: i64,
        schema_version_id: String,
        increment: f32,
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
            kind: NumericKind::Float32,
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
        let arr: [u8; 8] = self.data[..].try_into().map_err(|_| {
            Error::MergeError(format!(
                "invalid Int64 data length: expected 8, got {}",
                self.data.len()
            ))
        })?;
        Ok(i64::from_be_bytes(arr))
    }

    /// Decode the increment value as f32
    pub fn decode_float32(&self) -> Result<f32> {
        let arr: [u8; 4] = self.data[..].try_into().map_err(|_| {
            Error::MergeError(format!(
                "invalid Float32 data length: expected 4, got {}",
                self.data.len()
            ))
        })?;
        Ok(f32::from_be_bytes(arr))
    }

    /// Decode the increment value as f64
    pub fn decode_float64(&self) -> Result<f64> {
        let arr: [u8; 8] = self.data[..].try_into().map_err(|_| {
            Error::MergeError(format!(
                "invalid Float64 data length: expected 8, got {}",
                self.data.len()
            ))
        })?;
        Ok(f64::from_be_bytes(arr))
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

        let value_key = CRDTValueKey::new(
            schema_version_id.clone(),
            doc_id.to_vec(),
            field_name.clone(),
        );

        Ok(Self {
            value_key: value_key.bytes(),
            schema_version_id,
            field_name,
            allow_decrement,
            kind,
        })
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

    /// Get current value as f32
    async fn get_float32(&self, reader: &dyn Reader) -> Result<f32> {
        match reader
            .get(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            Some(bytes) => {
                if bytes.len() != 4 {
                    return Err(Error::MergeError(format!(
                        "invalid counter value length for field '{}' in schema '{}': \
                         expected 4 bytes (Float32), got {} bytes",
                        self.field_name,
                        self.schema_version_id,
                        bytes.len()
                    )));
                }
                let arr: [u8; 4] = bytes[..4]
                    .try_into()
                    .expect("length already validated as 4 bytes");
                Ok(f32::from_be_bytes(arr))
            }
            None => Ok(0.0),
        }
    }

    /// Set value as f32
    async fn set_float32(&self, rw: &mut dyn ReaderWriter, value: f32) -> Result<()> {
        rw.set(&self.value_key, &value.to_be_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Set value as f64
    async fn set_float64(&self, rw: &mut dyn ReaderWriter, value: f64) -> Result<()> {
        rw.set(&self.value_key, &value.to_be_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Apply an increment/decrement.
    ///
    /// Merge is unconditional: every delta that reaches this method is
    /// applied. Per-delta idempotency is the blockstore's job via
    /// `is_merged(cid)` / `get_unmerged()` on every ingest path (PushLog,
    /// DAG traversal, crash recovery). Do not reintroduce a nonce-based
    /// dedup check here — doing so causes Rust↔Go state divergence on
    /// legitimate block retransmits (#847). The `delta.nonce` field
    /// exists only so the resulting DAG block has a unique CID.
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
            NumericKind::Float32 => {
                let increment = delta.decode_float32()?;
                if !self.allow_decrement && increment < 0.0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }
                let current = if is_create {
                    0.0f32
                } else {
                    self.get_float32(rw).await?
                };
                NewValue::Float32(current + increment)
            }
            NumericKind::Float64 => {
                let increment = delta.decode_float64()?;
                if !self.allow_decrement && increment < 0.0 {
                    return Err(Error::MergeError("decrement not allowed".into()));
                }

                let current = if is_create {
                    0.0
                } else {
                    self.get_float64(rw).await?
                };
                NewValue::Float64(current + increment)
            }
        };

        match new_value {
            NewValue::Int64(v) => self.set_int64(rw, v).await?,
            NewValue::Float32(v) => self.set_float32(rw, v).await?,
            NewValue::Float64(v) => self.set_float64(rw, v).await?,
        }

        Ok(MergeResult::Applied)
    }

    /// Initialize the CRDT accumulation store (`value_key`) from the document's
    /// current materialized value, but only if the store has no value yet.
    ///
    /// The accumulation store (`value_key`) is the single source of truth for a
    /// counter. Both local writes and merges apply their *delta* to it via a
    /// read-modify-write; the document blob merely mirrors the resulting store
    /// value. Because local writes now also advance the accumulation store, a
    /// local increment is never "stranded in the blob" relative to the store, so
    /// reconcile only needs to seed the store the *first* time the counter is
    /// touched on this node — e.g. migrating an old document whose store predates
    /// this single-store invariant, or a node that materialized the blob via a
    /// non-counter path. Once the store has a value it is authoritative and must
    /// never be overwritten from a possibly-stale/lagging blob.
    ///
    /// This is deliberately *init-if-absent*, not a value comparison: a PNCounter
    /// allows decrements, so `blob < store` is ambiguous and `max` would be wrong.
    ///
    /// NOTE: per #847, counter merge is unconditional and there is no nonce
    /// tracking. A reconcile must never call a `mark_nonce(..)` helper — that
    /// would leak dead markers into the datastore and reintroduce the dedup
    /// contract #847 removed.
    pub async fn reconcile_int64(&self, rw: &mut dyn ReaderWriter, value: i64) -> Result<()> {
        if self.has_value(rw).await? {
            return Ok(());
        }
        self.set_int64(rw, value).await
    }

    /// Float64 variant of `reconcile_int64`. Init-if-absent only — see that method.
    pub async fn reconcile_float64(&self, rw: &mut dyn ReaderWriter, value: f64) -> Result<()> {
        if self.has_value(rw).await? {
            return Ok(());
        }
        self.set_float64(rw, value).await
    }

    /// Whether the accumulation store (`value_key`) currently holds a value.
    async fn has_value(&self, reader: &dyn Reader) -> Result<bool> {
        Ok(reader
            .get(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .is_some())
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ValueReader for Counter {
    async fn value(&self, reader: &dyn Reader) -> Result<Vec<u8>> {
        self.get_value_internal(reader).await
    }
}
