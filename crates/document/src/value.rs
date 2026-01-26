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
    /// Create a new FieldValue with validation of CRDT/value compatibility.
    ///
    /// Returns an error if the value type is incompatible with the CRDT type:
    /// - `PnCounter` and `PCounter` require numeric values (Int, Float64, Float32)
    /// - `Object` and `Composite` require Document values
    /// - `LwwRegister` and `None` accept any value type
    ///
    /// New values are marked as dirty by default.
    pub fn new(crdt_type: CType, value: NormalValue) -> Result<Self> {
        Self::validate_compatibility(crdt_type, &value)?;
        Ok(Self {
            crdt_type,
            value,
            is_dirty: true,
        })
    }

    /// Create a new FieldValue with LwwRegister CRDT type.
    ///
    /// LwwRegister accepts any value type, so this never fails.
    /// New values are marked as dirty by default.
    pub fn new_lww(value: NormalValue) -> Self {
        Self {
            crdt_type: CType::LwwRegister,
            value,
            is_dirty: true,
        }
    }

    /// Create a new FieldValue that is not marked as dirty.
    ///
    /// Returns an error if the value type is incompatible with the CRDT type.
    /// Used when loading from storage.
    pub fn new_clean(crdt_type: CType, value: NormalValue) -> Result<Self> {
        Self::validate_compatibility(crdt_type, &value)?;
        Ok(Self {
            crdt_type,
            value,
            is_dirty: false,
        })
    }

    /// Create a new clean FieldValue with LwwRegister CRDT type.
    ///
    /// LwwRegister accepts any value type, so this never fails.
    /// Used when loading from storage.
    pub fn new_clean_lww(value: NormalValue) -> Self {
        Self {
            crdt_type: CType::LwwRegister,
            value,
            is_dirty: false,
        }
    }

    /// Validate that a value is compatible with a CRDT type.
    fn validate_compatibility(crdt_type: CType, value: &NormalValue) -> Result<()> {
        match crdt_type {
            CType::PnCounter | CType::PCounter => {
                // Counters require numeric values
                if !Self::is_numeric_value(value) {
                    return Err(Error::IncompatibleCrdtType {
                        crdt_type,
                        value_type: Self::value_type_name(value),
                    });
                }
            }
            CType::Object | CType::Composite => {
                // Object/Composite require document values
                if value.as_document().is_none() && !value.is_nil() {
                    return Err(Error::IncompatibleCrdtType {
                        crdt_type,
                        value_type: Self::value_type_name(value),
                    });
                }
            }
            CType::LwwRegister | CType::None => {
                // LWW Register and None accept any value
            }
        }
        Ok(())
    }

    /// Check if a value is a numeric type suitable for counters.
    fn is_numeric_value(value: &NormalValue) -> bool {
        matches!(
            value,
            NormalValue::Int(_)
                | NormalValue::Float64(_)
                | NormalValue::Float32(_)
                | NormalValue::NillableInt(_)
                | NormalValue::NillableFloat64(_)
                | NormalValue::NillableFloat32(_)
                | NormalValue::Null
        )
    }

    /// Get a human-readable name for a value's type.
    fn value_type_name(value: &NormalValue) -> String {
        match value {
            NormalValue::Null => "Null".to_string(),
            NormalValue::Bool(_) => "Bool".to_string(),
            NormalValue::Int(_) => "Int".to_string(),
            NormalValue::Float64(_) => "Float64".to_string(),
            NormalValue::Float32(_) => "Float32".to_string(),
            NormalValue::String(_) => "String".to_string(),
            NormalValue::Bytes(_) => "Bytes".to_string(),
            NormalValue::Time(_) => "Time".to_string(),
            NormalValue::Document(_) => "Document".to_string(),
            NormalValue::Json(_) => "Json".to_string(),
            NormalValue::NillableBool(_) => "NillableBool".to_string(),
            NormalValue::NillableInt(_) => "NillableInt".to_string(),
            NormalValue::NillableFloat64(_) => "NillableFloat64".to_string(),
            NormalValue::NillableFloat32(_) => "NillableFloat32".to_string(),
            NormalValue::NillableString(_) => "NillableString".to_string(),
            NormalValue::NillableBytes(_) => "NillableBytes".to_string(),
            NormalValue::NillableTime(_) => "NillableTime".to_string(),
            NormalValue::NillableDocument(_) => "NillableDocument".to_string(),
            _ if value.is_array() => "Array".to_string(),
            _ => "Unknown".to_string(),
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
    ///
    /// Note: This does not validate CRDT/value compatibility.
    /// Use `set_crdt_type_validated` for validation.
    pub fn set_crdt_type(&mut self, crdt_type: CType) {
        self.crdt_type = crdt_type;
    }

    /// Set the CRDT type with validation of CRDT/value compatibility.
    ///
    /// Returns an error if the current value is incompatible with the new CRDT type.
    pub fn set_crdt_type_validated(&mut self, crdt_type: CType) -> Result<()> {
        Self::validate_compatibility(crdt_type, &self.value)?;
        self.crdt_type = crdt_type;
        Ok(())
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
        Self::new_clean(crdt_type, value)
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
        let fv = FieldValue::new_lww(NormalValue::Int(42));
        assert!(fv.is_dirty());
    }

    #[test]
    fn test_new_clean_is_not_dirty() {
        let fv = FieldValue::new_clean_lww(NormalValue::Int(42));
        assert!(!fv.is_dirty());
    }

    #[test]
    fn test_clean() {
        let mut fv = FieldValue::new_lww(NormalValue::Int(42));
        assert!(fv.is_dirty());
        fv.clean();
        assert!(!fv.is_dirty());
    }

    #[test]
    fn test_value_mut_marks_dirty() {
        let mut fv = FieldValue::new_clean_lww(NormalValue::Int(42));
        assert!(!fv.is_dirty());
        let _ = fv.value_mut();
        assert!(fv.is_dirty());
    }

    #[test]
    fn test_set_value_marks_dirty() {
        let mut fv = FieldValue::new_clean_lww(NormalValue::Int(42));
        assert!(!fv.is_dirty());
        fv.set_value(NormalValue::Int(100));
        assert!(fv.is_dirty());
        assert_eq!(fv.value().as_int(), Some(100));
    }

    #[test]
    fn test_crdt_type() {
        let fv = FieldValue::new(CType::PnCounter, NormalValue::Int(0)).unwrap();
        assert_eq!(fv.crdt_type(), CType::PnCounter);
    }

    #[test]
    fn test_is_document() {
        let fv = FieldValue::new_lww(NormalValue::Int(42));
        assert!(!fv.is_document());
    }

    #[test]
    fn test_cbor_roundtrip() {
        let fv = FieldValue::new_lww(NormalValue::String("hello".into()));
        let bytes = fv.to_cbor().unwrap();
        let decoded = FieldValue::from_cbor(CType::LwwRegister, &bytes).unwrap();
        assert_eq!(fv.value(), decoded.value());
        assert!(!decoded.is_dirty()); // from_cbor creates clean values
    }

    #[test]
    fn test_equality_ignores_dirty() {
        let fv1 = FieldValue::new_lww(NormalValue::Int(42));
        let fv2 = FieldValue::new_clean_lww(NormalValue::Int(42));
        assert_eq!(fv1, fv2); // dirty flag should not affect equality
    }

    #[test]
    fn test_default() {
        let fv = FieldValue::default();
        assert_eq!(fv.crdt_type(), CType::LwwRegister);
        assert!(fv.value().is_nil());
        assert!(!fv.is_dirty());
    }

    // === Validation tests ===

    #[test]
    fn test_new_counter_with_int() {
        let fv = FieldValue::new(CType::PnCounter, NormalValue::Int(42));
        assert!(fv.is_ok());
    }

    #[test]
    fn test_new_counter_with_float() {
        let fv = FieldValue::new(CType::PnCounter, NormalValue::Float64(3.14));
        assert!(fv.is_ok());
    }

    #[test]
    fn test_new_counter_with_string_fails() {
        let result = FieldValue::new(CType::PnCounter, NormalValue::String("hello".into()));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::IncompatibleCrdtType { .. }
        ));
    }

    #[test]
    fn test_new_counter_with_null_ok() {
        // Null is allowed for counters (represents unset)
        let fv = FieldValue::new(CType::PnCounter, NormalValue::Null);
        assert!(fv.is_ok());
    }

    #[test]
    fn test_new_lww_accepts_any() {
        // LWW Register accepts any value type
        assert!(FieldValue::new(CType::LwwRegister, NormalValue::Int(42)).is_ok());
        assert!(FieldValue::new(CType::LwwRegister, NormalValue::String("hello".into())).is_ok());
        assert!(FieldValue::new(CType::LwwRegister, NormalValue::Bool(true)).is_ok());
    }

    #[test]
    fn test_set_crdt_type_validated_fails_incompatible() {
        let mut fv = FieldValue::new_lww(NormalValue::String("hello".into()));
        let result = fv.set_crdt_type_validated(CType::PnCounter);
        assert!(result.is_err());
    }

    #[test]
    fn test_set_crdt_type_validated_succeeds_compatible() {
        let mut fv = FieldValue::new_lww(NormalValue::Int(42));
        let result = fv.set_crdt_type_validated(CType::PnCounter);
        assert!(result.is_ok());
        assert_eq!(fv.crdt_type(), CType::PnCounter);
    }

    // === CBOR error path tests ===

    #[test]
    fn test_from_cbor_invalid_bytes() {
        // Random garbage bytes should fail
        let result = FieldValue::from_cbor(CType::LwwRegister, &[0xff, 0xfe, 0xfd]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::CborDecode(_)));
    }

    #[test]
    fn test_from_cbor_empty_bytes() {
        let result = FieldValue::from_cbor(CType::LwwRegister, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_cbor_truncated() {
        // Start of a CBOR map but truncated
        let result = FieldValue::from_cbor(CType::LwwRegister, &[0xa2, 0x63]);
        assert!(result.is_err());
    }
}
