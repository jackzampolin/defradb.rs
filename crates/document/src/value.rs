// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Field value wrapper with CRDT type and dirty tracking

use schema::CType;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::NormalValue;

/// Wrapper around a field value with CRDT type and dirty tracking.
///
/// FieldValue combines:
/// - The actual value (NormalValue)
/// - The CRDT type used for conflict resolution
/// - A dirty flag for tracking unsaved changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    /// The CRDT type for this field
    crdt_type: CType,
    /// The actual value
    value: NormalValue,
    /// Whether this value has unsaved changes
    #[serde(skip)]
    is_dirty: bool,
}

impl FieldValue {
    /// Create a new FieldValue with the given CRDT type and value.
    /// New values are marked as dirty by default.
    pub fn new(crdt_type: CType, value: NormalValue) -> Self {
        Self {
            crdt_type,
            value,
            is_dirty: true,
        }
    }

    /// Create a new FieldValue that is not marked as dirty.
    /// Used when loading from storage.
    pub fn new_clean(crdt_type: CType, value: NormalValue) -> Self {
        Self {
            crdt_type,
            value,
            is_dirty: false,
        }
    }

    /// Get the underlying value, unwrapping any Option wrappers.
    pub fn value(&self) -> &NormalValue {
        &self.value
    }

    /// Get the NormalValue directly.
    pub fn normal_value(&self) -> &NormalValue {
        &self.value
    }

    /// Get mutable access to the value.
    /// This marks the field as dirty.
    pub fn value_mut(&mut self) -> &mut NormalValue {
        self.is_dirty = true;
        &mut self.value
    }

    /// Get the CRDT type for this field.
    pub fn crdt_type(&self) -> CType {
        self.crdt_type
    }

    /// Set the CRDT type for this field.
    pub fn set_crdt_type(&mut self, crdt_type: CType) {
        self.crdt_type = crdt_type;
    }

    /// Returns true if this value is a document.
    pub fn is_document(&self) -> bool {
        self.value.as_document().is_some()
    }

    /// Returns true if this value has unsaved changes.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Mark this value as clean (saved).
    pub fn clean(&mut self) {
        self.is_dirty = false;
    }

    /// Mark this value as dirty (has changes).
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// Set the value, marking it as dirty.
    pub fn set_value(&mut self, value: NormalValue) {
        self.value = value;
        self.is_dirty = true;
    }

    /// Encode this value to CBOR bytes.
    pub fn to_cbor(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        ciborium::into_writer(&self.value, &mut buf)
            .map_err(|e| Error::CborEncode(e.to_string()))?;
        Ok(buf)
    }

    /// Decode a value from CBOR bytes.
    pub fn from_cbor(crdt_type: CType, bytes: &[u8]) -> Result<Self> {
        let value: NormalValue =
            ciborium::from_reader(bytes).map_err(|e| Error::CborDecode(e.to_string()))?;
        Ok(Self::new_clean(crdt_type, value))
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        self.crdt_type == other.crdt_type && self.value == other.value
    }
}

impl Default for FieldValue {
    fn default() -> Self {
        Self {
            crdt_type: CType::LwwRegister,
            value: NormalValue::Null,
            is_dirty: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_dirty() {
        let fv = FieldValue::new(CType::LwwRegister, NormalValue::Int(42));
        assert!(fv.is_dirty());
    }

    #[test]
    fn test_new_clean_is_not_dirty() {
        let fv = FieldValue::new_clean(CType::LwwRegister, NormalValue::Int(42));
        assert!(!fv.is_dirty());
    }

    #[test]
    fn test_clean() {
        let mut fv = FieldValue::new(CType::LwwRegister, NormalValue::Int(42));
        assert!(fv.is_dirty());
        fv.clean();
        assert!(!fv.is_dirty());
    }

    #[test]
    fn test_value_mut_marks_dirty() {
        let mut fv = FieldValue::new_clean(CType::LwwRegister, NormalValue::Int(42));
        assert!(!fv.is_dirty());
        let _ = fv.value_mut();
        assert!(fv.is_dirty());
    }

    #[test]
    fn test_set_value_marks_dirty() {
        let mut fv = FieldValue::new_clean(CType::LwwRegister, NormalValue::Int(42));
        assert!(!fv.is_dirty());
        fv.set_value(NormalValue::Int(100));
        assert!(fv.is_dirty());
        assert_eq!(fv.value().as_int(), Some(100));
    }

    #[test]
    fn test_crdt_type() {
        let fv = FieldValue::new(CType::PnCounter, NormalValue::Int(0));
        assert_eq!(fv.crdt_type(), CType::PnCounter);
    }

    #[test]
    fn test_is_document() {
        let fv = FieldValue::new(CType::LwwRegister, NormalValue::Int(42));
        assert!(!fv.is_document());
    }

    #[test]
    fn test_cbor_roundtrip() {
        let fv = FieldValue::new(CType::LwwRegister, NormalValue::String("hello".into()));
        let bytes = fv.to_cbor().unwrap();
        let decoded = FieldValue::from_cbor(CType::LwwRegister, &bytes).unwrap();
        assert_eq!(fv.value(), decoded.value());
        assert!(!decoded.is_dirty()); // from_cbor creates clean values
    }

    #[test]
    fn test_equality_ignores_dirty() {
        let fv1 = FieldValue::new(CType::LwwRegister, NormalValue::Int(42));
        let fv2 = FieldValue::new_clean(CType::LwwRegister, NormalValue::Int(42));
        assert_eq!(fv1, fv2); // dirty flag should not affect equality
    }

    #[test]
    fn test_default() {
        let fv = FieldValue::default();
        assert_eq!(fv.crdt_type(), CType::LwwRegister);
        assert!(fv.value().is_nil());
        assert!(!fv.is_dirty());
    }
}
