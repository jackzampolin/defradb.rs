//! Composite CRDT - Document-level CRDT composition
//!
//! The Composite CRDT manages document-level state by coordinating
//! multiple field-level CRDTs (LWW, Counter, etc).

use crate::counter::{Counter, CounterDelta, NumericKind};
use crate::priority::{decode_priority, encode_priority};
use crate::traits::{Context, Delta, MergeResult, ReplicatedData};
use async_trait::async_trait;
use defra_core::{types::DocId, Error, Result};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use storage::{corekv::Key, keys::CRDTValueKey, Reader, ReaderWriter};

/// Document status matching Go's `client.DocumentStatus`.
///
/// Active (1) is the default for new documents.
/// Deleted (2) represents a document that has been marked as deleted.
pub const STATUS_ACTIVE: u8 = 1;
pub const STATUS_DELETED: u8 = 2;

/// Composite Delta - represents changes to multiple fields in a document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeDelta {
    /// Document ID this delta applies to
    doc_id: Vec<u8>,
    /// Schema version identifier
    schema_version_id: String,
    /// Priority for this delta
    priority: u64,
    /// Document status (1 = Active, 2 = Deleted).
    /// Matches Go's `DocCompositeDelta.Status` (`client.DocumentStatus`).
    status: u8,
    /// Field-level deltas
    field_deltas: HashMap<String, FieldDelta>,
}

impl CompositeDelta {
    /// Create a new composite delta with Active status
    pub fn new(doc_id: Vec<u8>, schema_version_id: String, priority: u64) -> Result<Self> {
        Self::with_status(doc_id, schema_version_id, priority, STATUS_ACTIVE)
    }

    /// Create a new composite delta with the given status
    pub fn with_status(
        doc_id: Vec<u8>,
        schema_version_id: String,
        priority: u64,
        status: u8,
    ) -> Result<Self> {
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
            status,
            field_deltas: HashMap::new(),
        })
    }

    /// Get the document status (1 = Active, 2 = Deleted)
    pub fn status(&self) -> u8 {
        self.status
    }

    /// Check if this delta represents a document deletion
    pub fn is_deleted(&self) -> bool {
        self.status == STATUS_DELETED
    }

    /// Add a field delta
    pub fn add_field_delta(
        &mut self,
        field_name: impl Into<String>,
        delta: FieldDelta,
    ) -> Result<()> {
        let field_name = field_name.into();
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
#[non_exhaustive]
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
    /// Document ID
    doc_id: DocId,
    /// Schema version
    schema_version_id: String,
    /// Field-level CRDT managers
    /// Maps field_name -> CRDT instance
    field_managers: HashMap<String, FieldCrdtType>,
}

/// Types of field-level CRDTs
#[derive(Debug, Clone)]
enum FieldCrdtType {
    Lww,
    Counter {
        allow_decrement: bool,
        kind: NumericKind,
    },
}

impl CompositeDAG {
    /// Create a new CompositeDAG
    pub fn new(doc_id: DocId, schema_version_id: impl Into<String>) -> Self {
        Self {
            doc_id,
            schema_version_id: schema_version_id.into(),
            field_managers: HashMap::new(),
        }
    }

    /// Register a field as LWW
    pub fn register_lww_field(&mut self, field_name: impl Into<String>) {
        self.field_managers
            .insert(field_name.into(), FieldCrdtType::Lww);
    }

    /// Register a field as Counter
    pub fn register_counter_field(
        &mut self,
        field_name: impl Into<String>,
        allow_decrement: bool,
        kind: NumericKind,
    ) {
        self.field_managers.insert(
            field_name.into(),
            FieldCrdtType::Counter {
                allow_decrement,
                kind,
            },
        );
    }

    /// Build the value key for a field
    fn build_value_key(&self, field_name: &str) -> Vec<u8> {
        CRDTValueKey::new(
            self.schema_version_id.clone(),
            self.doc_id.as_str().as_bytes().to_vec(),
            field_name.to_string(),
        )
        .bytes()
    }

    /// Build the priority key for a field
    fn build_priority_key(&self, field_name: &str) -> Vec<u8> {
        CRDTValueKey::new(
            self.schema_version_id.clone(),
            self.doc_id.as_str().as_bytes().to_vec(),
            field_name.to_string(),
        )
        .priority_key()
        .bytes()
    }

    /// Get the current priority for a field (0 if not set)
    async fn get_field_priority(&self, reader: &dyn Reader, field_name: &str) -> Result<u64> {
        let priority_key = self.build_priority_key(field_name);
        match reader
            .get(&priority_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            Some(bytes) => decode_priority(&bytes),
            None => Ok(0),
        }
    }

    /// Set the priority for a field
    async fn set_field_priority(
        &self,
        rw: &mut dyn ReaderWriter,
        field_name: &str,
        priority: u64,
    ) -> Result<()> {
        let priority_key = self.build_priority_key(field_name);
        let priority_bytes = encode_priority(priority);
        rw.set(&priority_key, &priority_bytes)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Apply field-level delta with proper CRDT conflict resolution
    async fn apply_field_delta(
        &self,
        rw: &mut dyn ReaderWriter,
        field_name: &str,
        field_delta: &FieldDelta,
        is_create: bool,
    ) -> Result<MergeResult> {
        let crdt_type = self
            .field_managers
            .get(field_name)
            .ok_or_else(|| Error::MergeError(format!("unknown field: {}", field_name)))?;

        match (crdt_type, field_delta) {
            (FieldCrdtType::Lww, FieldDelta::Lww { priority, data }) => {
                let value_key = self.build_value_key(field_name);

                if !is_create {
                    let current_priority = self.get_field_priority(rw, field_name).await?;

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
                            let current_value = rw
                                .get(&value_key)
                                .await
                                .map_err(|e| Error::Storage(e.to_string()))?
                                .unwrap_or_default();
                            if data.as_slice() <= &current_value[..] {
                                return Ok(MergeResult::RejectedTieBreak);
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            // Higher priority - proceed to update
                        }
                    }
                }

                // Apply the update
                if data.is_empty() {
                    rw.delete(&value_key)
                        .await
                        .map_err(|e| Error::Storage(e.to_string()))?;
                } else {
                    rw.set(&value_key, data)
                        .await
                        .map_err(|e| Error::Storage(e.to_string()))?;
                }
                self.set_field_priority(rw, field_name, *priority).await?;

                Ok(MergeResult::Applied)
            }
            (
                FieldCrdtType::Counter {
                    allow_decrement,
                    kind,
                },
                FieldDelta::Counter {
                    priority,
                    nonce,
                    data,
                },
            ) => {
                let expected = match kind {
                    NumericKind::Float32 => 4,
                    NumericKind::Int64 | NumericKind::Float64 => 8,
                };
                if data.len() != expected {
                    return Err(Error::MergeError(format!(
                        "invalid counter increment data for field '{}': expected {} bytes, got {}",
                        field_name,
                        expected,
                        data.len()
                    )));
                }

                let counter = Counter::new(
                    self.schema_version_id.clone(),
                    self.doc_id.as_str().as_bytes(),
                    field_name.to_string(),
                    *allow_decrement,
                    *kind,
                )?;

                let delta = match kind {
                    NumericKind::Int64 => CounterDelta::new_int64(
                        self.doc_id.as_str().as_bytes().to_vec(),
                        field_name.to_string(),
                        *priority,
                        *nonce,
                        self.schema_version_id.clone(),
                        i64::from_be_bytes(data[..8].try_into().map_err(|_| {
                            Error::MergeError(format!(
                                "invalid counter increment data for field '{}': expected 8 bytes, got {}",
                                field_name, data.len()
                            ))
                        })?),
                    )?,
                    NumericKind::Float32 => CounterDelta::new_float32(
                        self.doc_id.as_str().as_bytes().to_vec(),
                        field_name.to_string(),
                        *priority,
                        *nonce,
                        self.schema_version_id.clone(),
                        f32::from_be_bytes(data[..4].try_into().map_err(|_| {
                            Error::MergeError(format!(
                                "invalid counter increment data for field '{}': expected 4 bytes (Float32), got {}",
                                field_name, data.len()
                            ))
                        })?),
                    )?,
                    NumericKind::Float64 => CounterDelta::new_float64(
                        self.doc_id.as_str().as_bytes().to_vec(),
                        field_name.to_string(),
                        *priority,
                        *nonce,
                        self.schema_version_id.clone(),
                        f64::from_be_bytes(data[..8].try_into().map_err(|_| {
                            Error::MergeError(format!(
                                "invalid counter increment data for field '{}': expected 8 bytes, got {}",
                                field_name, data.len()
                            ))
                        })?),
                    )?,
                };

                counter
                    .merge(
                        rw,
                        &Context {
                            doc_id: self.doc_id.clone(),
                            schema_version: self.schema_version_id.clone(),
                            is_create,
                        },
                        &delta,
                    )
                    .await
            }
            (_, FieldDelta::Delete { priority }) => {
                let value_key = self.build_value_key(field_name);

                if !is_create {
                    let current_priority = self.get_field_priority(rw, field_name).await?;

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
                            if let Some(v) = rw
                                .get(&value_key)
                                .await
                                .map_err(|e| Error::Storage(e.to_string()))?
                            {
                                if !v.is_empty() {
                                    return Ok(MergeResult::RejectedTieBreak);
                                }
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            // Higher priority - proceed to delete
                        }
                    }
                }

                // Apply deletion
                rw.delete(&value_key)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                self.set_field_priority(rw, field_name, *priority).await?;

                Ok(MergeResult::Applied)
            }
            _ => Err(Error::MergeError(format!(
                "field type mismatch for field: {}",
                field_name
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ReplicatedData for CompositeDAG {
    async fn merge(
        &self,
        rw: &mut dyn ReaderWriter,
        ctx: &Context,
        delta: &dyn Delta,
    ) -> Result<MergeResult> {
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
                | (FieldCrdtType::Counter { .. }, FieldDelta::Counter { .. })
                | (FieldCrdtType::Counter { .. }, FieldDelta::Delete { .. }) => {
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
            if let (FieldCrdtType::Counter { kind, .. }, FieldDelta::Counter { data, .. }) =
                (crdt_type, field_delta)
            {
                let expected = match kind {
                    NumericKind::Float32 => 4,
                    NumericKind::Int64 | NumericKind::Float64 => 8,
                };
                if data.len() != expected {
                    return Err(Error::MergeError(format!(
                        "invalid counter increment data for field '{}': expected {} bytes, got {}",
                        field_name,
                        expected,
                        data.len()
                    )));
                }
            }
        }

        // Apply each field delta (now that all fields are pre-validated)
        let mut any_applied = false;
        for (field_name, field_delta) in &composite_delta.field_deltas {
            let result = self
                .apply_field_delta(rw, field_name, field_delta, ctx.is_create)
                .await?;
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
}
