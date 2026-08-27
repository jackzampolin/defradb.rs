//! Integration tests for FieldValue

use document::{Error, FieldValue, NormalValue};
use schema::CType;

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
    let fv = FieldValue::new(CType::PnCounter, NormalValue::Float64(3.15));
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
