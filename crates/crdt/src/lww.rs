//! Last-Write-Wins (LWW) Register CRDT implementation
//!
//! The LWW Register resolves conflicts using priority-based ordering.
//! When two concurrent writes occur, the one with higher priority wins.
//! On tie, lexicographic comparison of values provides deterministic resolution.

use crate::priority::{decode_priority, encode_priority};
use crate::traits::{Context, Delta, MergeResult, PriorityReader, ReplicatedData, ValueReader};
use async_trait::async_trait;
use defra_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use storage::{Reader, ReaderWriter};

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
///
/// This CRDT does not own storage. Instead, it receives a `ReaderWriter`
/// reference for each operation, matching Go DefraDB's pattern where
/// CRDTs operate on a provided `corekv.ReaderWriter`.
pub struct Lww {
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
    /// * `schema_version_id` - Schema version identifier (must not be empty)
    /// * `doc_id` - Document identifier (must not be empty)
    /// * `field_name` - Field name (must not be empty)
    ///
    /// # Errors
    /// Returns an error if schema_version_id, doc_id, or field_name is empty.
    pub fn new(schema_version_id: String, doc_id: &[u8], field_name: String) -> Result<Self> {
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
            value_key,
            priority_key,
            schema_version_id,
            field_name,
        })
    }

    /// Set a value with priority, implementing LWW merge logic
    async fn set_value(
        &self,
        rw: &mut dyn ReaderWriter,
        data: &[u8],
        incoming_priority: u64,
    ) -> Result<MergeResult> {
        // Get current priority
        let current_priority = self.get_priority_internal(rw).await?;

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
                let current_value: Vec<u8> = rw
                    .get(&self.value_key)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .unwrap_or_default();
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
            rw.delete(&self.value_key)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        } else {
            rw.set(&self.value_key, data)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        let priority_bytes = encode_priority(incoming_priority);
        rw.set(&self.priority_key, &priority_bytes)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(MergeResult::Applied)
    }

    /// Internal method to get current value
    async fn get_value_internal(&self, reader: &dyn Reader) -> Result<Vec<u8>> {
        reader
            .get(&self.value_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .ok_or_else(|| {
                Error::MergeError(format!(
                    "value not found for field '{}' in schema '{}'. \
                     This indicates the field has never been set or has been deleted.",
                    self.field_name, self.schema_version_id
                ))
            })
    }

    /// Internal method to get current priority
    async fn get_priority_internal(&self, reader: &dyn Reader) -> Result<u64> {
        match reader
            .get(&self.priority_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
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
    async fn merge(
        &self,
        rw: &mut dyn ReaderWriter,
        _ctx: &Context,
        delta: &dyn Delta,
    ) -> Result<MergeResult> {
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
        self.set_value(rw, &lww_delta.data, lww_delta.priority)
            .await
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
    async fn value(&self, reader: &dyn Reader) -> Result<Vec<u8>> {
        self.get_value_internal(reader).await
    }
}

#[async_trait]
impl PriorityReader for Lww {
    async fn priority(&self, reader: &dyn Reader) -> Result<u64> {
        self.get_priority_internal(reader).await
    }
}

