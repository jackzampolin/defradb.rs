//! Tests for FieldKind, ScalarKind, and ScalarArrayKind types.
//!
//! These tests verify:
//! - Go-compatible JSON serialization/deserialization
//! - Type classification methods (is_numeric, is_array, etc.)
//! - Scalar-to-array conversions

use schema::{FieldKind, ScalarArrayKind, ScalarKind};

// ============================================================================
// Repr Value Tests - Ensure Go Compatibility
// ============================================================================

#[test]
fn test_scalar_repr_values_match_go() {
    // These values MUST match Go DefraDB for datastore compatibility
    assert_eq!(ScalarKind::None as u8, 0);
    assert_eq!(ScalarKind::DocID as u8, 1);
    assert_eq!(ScalarKind::Bool as u8, 2);
    assert_eq!(ScalarKind::Int as u8, 4);
    assert_eq!(ScalarKind::Float64 as u8, 6);
    assert_eq!(ScalarKind::Float32 as u8, 8);
    assert_eq!(ScalarKind::DateTime as u8, 10);
    assert_eq!(ScalarKind::String as u8, 11);
    assert_eq!(ScalarKind::Blob as u8, 13);
    assert_eq!(ScalarKind::Json as u8, 14);
    assert_eq!(ScalarKind::NonNillableBool as u8, 15);
    assert_eq!(ScalarKind::NonNillableInt as u8, 23);
    assert_eq!(ScalarKind::NonNillableFloat64 as u8, 24);
    assert_eq!(ScalarKind::NonNillableFloat32 as u8, 25);
    assert_eq!(ScalarKind::NonNillableString as u8, 26);
    assert_eq!(ScalarKind::NonNillableDateTime as u8, 27);
    assert_eq!(ScalarKind::NonNillableBlob as u8, 28);
    assert_eq!(ScalarKind::NonNillableJson as u8, 29);
}

#[test]
fn test_array_repr_values_match_go() {
    assert_eq!(ScalarArrayKind::BoolArray as u8, 3);
    assert_eq!(ScalarArrayKind::IntArray as u8, 5);
    assert_eq!(ScalarArrayKind::Float64Array as u8, 7);
    assert_eq!(ScalarArrayKind::Float32Array as u8, 9);
    assert_eq!(ScalarArrayKind::StringArray as u8, 12);
    assert_eq!(ScalarArrayKind::NillableBoolArray as u8, 18);
    assert_eq!(ScalarArrayKind::NillableIntArray as u8, 19);
    assert_eq!(ScalarArrayKind::NillableFloat64Array as u8, 20);
    assert_eq!(ScalarArrayKind::NillableStringArray as u8, 21);
    assert_eq!(ScalarArrayKind::NillableFloat32Array as u8, 22);
    assert_eq!(ScalarArrayKind::DateTimeArray as u8, 30);
    assert_eq!(ScalarArrayKind::NillableDateTimeArray as u8, 31);
}

// ============================================================================
// Type Classification Tests
// ============================================================================

#[test]
fn test_is_numeric() {
    assert!(FieldKind::int().is_numeric());
    assert!(FieldKind::float64().is_numeric());
    assert!(FieldKind::float32().is_numeric());
    assert!(!FieldKind::string().is_numeric());
    assert!(!FieldKind::bool().is_numeric());
}

#[test]
fn test_is_array() {
    assert!(FieldKind::int_array().is_array());
    assert!(FieldKind::string_array().is_array());
    assert!(FieldKind::nillable_int_array().is_array());
    assert!(!FieldKind::int().is_array());
    assert!(FieldKind::relation("users", true).is_array());
    assert!(!FieldKind::relation("users", false).is_array());
}

#[test]
fn test_is_relation() {
    assert!(FieldKind::relation("users", false).is_relation());
    assert!(FieldKind::self_ref("parent", false).is_relation());
    assert!(FieldKind::named("User", false).is_relation());
    assert!(!FieldKind::string().is_relation());
}

#[test]
fn test_is_nillable() {
    assert!(FieldKind::string().is_nillable());
    assert!(FieldKind::int().is_nillable());
    assert!(FieldKind::doc_id().is_nillable());
    assert!(!FieldKind::Scalar(ScalarKind::NonNillableString).is_nillable());
    assert!(FieldKind::int_array().is_nillable());
    assert!(FieldKind::nillable_int_array().is_nillable());
    assert!(FieldKind::relation("users", false).is_nillable());
}

#[test]
fn test_has_nillable_elements() {
    // has_nillable_elements checks if array elements can be null
    assert!(FieldKind::nillable_int_array().has_nillable_elements());
    assert!(FieldKind::nillable_string_array().has_nillable_elements());
    assert!(!FieldKind::int_array().has_nillable_elements());
    assert!(!FieldKind::string_array().has_nillable_elements());
    // Non-arrays return false
    assert!(!FieldKind::int().has_nillable_elements());
}

#[test]
fn test_scalar_to_array() {
    assert_eq!(
        ScalarKind::Bool.to_array_kind(),
        Some(ScalarArrayKind::BoolArray)
    );
    assert_eq!(
        ScalarKind::Int.to_array_kind(),
        Some(ScalarArrayKind::IntArray)
    );
    assert_eq!(ScalarKind::DocID.to_array_kind(), None);
    assert_eq!(
        ScalarKind::DateTime.to_array_kind(),
        Some(ScalarArrayKind::DateTimeArray)
    );
}

#[test]
fn test_array_element_kind() {
    assert_eq!(ScalarArrayKind::IntArray.element_kind(), ScalarKind::Int);
    assert_eq!(
        ScalarArrayKind::NillableIntArray.element_kind(),
        ScalarKind::Int
    );
    assert_eq!(
        ScalarArrayKind::Float64Array.element_kind(),
        ScalarKind::Float64
    );
}

// ============================================================================
// Serialization Roundtrip Tests
// ============================================================================

#[test]
fn test_serialization_roundtrip() {
    let kinds = vec![
        FieldKind::doc_id(),
        FieldKind::bool(),
        FieldKind::int(),
        FieldKind::float64(),
        FieldKind::float32(),
        FieldKind::string(),
        FieldKind::int_array(),
        FieldKind::nillable_int_array(),
        FieldKind::relation("users", true),
        FieldKind::self_ref("parent", false),
        FieldKind::named("User", false),
    ];

    for kind in kinds {
        let json = serde_json::to_string(&kind).unwrap();
        let parsed: FieldKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, parsed);
    }
}

// ============================================================================
// Go Compatibility Tests - JSON Serialization Format
// ============================================================================

#[test]
fn test_go_compat_scalar_serializes_as_integer() {
    // Go serializes ScalarKind as just the integer value
    assert_eq!(serde_json::to_string(&FieldKind::bool()).unwrap(), "2");
    assert_eq!(serde_json::to_string(&FieldKind::int()).unwrap(), "4");
    assert_eq!(serde_json::to_string(&FieldKind::float64()).unwrap(), "6");
    assert_eq!(serde_json::to_string(&FieldKind::string()).unwrap(), "11");
    assert_eq!(serde_json::to_string(&FieldKind::doc_id()).unwrap(), "1");
}

#[test]
fn test_go_compat_array_serializes_as_integer() {
    // Go serializes ScalarArrayKind as just the integer value
    assert_eq!(
        serde_json::to_string(&FieldKind::bool_array()).unwrap(),
        "3"
    );
    assert_eq!(serde_json::to_string(&FieldKind::int_array()).unwrap(), "5");
    assert_eq!(
        serde_json::to_string(&FieldKind::float64_array()).unwrap(),
        "7"
    );
    assert_eq!(
        serde_json::to_string(&FieldKind::string_array()).unwrap(),
        "12"
    );
    assert_eq!(
        serde_json::to_string(&FieldKind::nillable_int_array()).unwrap(),
        "19"
    );
}

#[test]
fn test_go_compat_relation_serializes_as_object() {
    // Go serializes CollectionKind as {"Array": bool, "CollectionID": string}
    let relation = FieldKind::relation("bafkreiabc123", true);
    let json = serde_json::to_string(&relation).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert!(parsed.is_object());
    assert_eq!(parsed["Array"], true);
    assert_eq!(parsed["CollectionID"], "bafkreiabc123");
}

#[test]
fn test_go_compat_selfref_serializes_as_object() {
    // Go serializes SelfKind as {"RelativeID": string, "Array": bool}
    let self_ref = FieldKind::self_ref("parent", false);
    let json = serde_json::to_string(&self_ref).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert!(parsed.is_object());
    assert_eq!(parsed["RelativeID"], "parent");
    assert_eq!(parsed["Array"], false);
}

#[test]
fn test_go_compat_named_serializes_as_object() {
    // Go serializes NamedKind as {"Name": string, "Array": bool}
    let named = FieldKind::named("User", true);
    let json = serde_json::to_string(&named).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert!(parsed.is_object());
    assert_eq!(parsed["Name"], "User");
    assert_eq!(parsed["Array"], true);
}

#[test]
fn test_go_compat_deserialize_integer() {
    // Go's parseFieldKind accepts raw integers
    assert_eq!(
        serde_json::from_str::<FieldKind>("2").unwrap(),
        FieldKind::bool()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>("4").unwrap(),
        FieldKind::int()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>("5").unwrap(),
        FieldKind::int_array()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>("19").unwrap(),
        FieldKind::nillable_int_array()
    );
}

#[test]
fn test_go_compat_deserialize_string() {
    // Go's parseFieldKind accepts string names
    assert_eq!(
        serde_json::from_str::<FieldKind>(r#""Boolean""#).unwrap(),
        FieldKind::bool()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>(r#""Int""#).unwrap(),
        FieldKind::int()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>(r#""String""#).unwrap(),
        FieldKind::string()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>(r#""[Int!]""#).unwrap(),
        FieldKind::int_array()
    );
    assert_eq!(
        serde_json::from_str::<FieldKind>(r#""[Int]""#).unwrap(),
        FieldKind::nillable_int_array()
    );
}

#[test]
fn test_go_compat_deserialize_self_string() {
    // Go uses "Self" from request.SelfTypeName
    let result: FieldKind = serde_json::from_str(r#""Self""#).unwrap();
    assert_eq!(
        result,
        FieldKind::SelfRef {
            relative_id: String::new(),
            is_array: false
        }
    );

    let result_array: FieldKind = serde_json::from_str(r#""[Self]""#).unwrap();
    assert_eq!(
        result_array,
        FieldKind::SelfRef {
            relative_id: String::new(),
            is_array: true
        }
    );
}

#[test]
fn test_go_compat_deserialize_named_string() {
    // Unknown strings become NamedKind
    let result: FieldKind = serde_json::from_str(r#""Author""#).unwrap();
    assert_eq!(
        result,
        FieldKind::Named {
            name: "Author".to_string(),
            is_array: false
        }
    );

    let result_array: FieldKind = serde_json::from_str(r#""[Author]""#).unwrap();
    assert_eq!(
        result_array,
        FieldKind::Named {
            name: "Author".to_string(),
            is_array: true
        }
    );
}

#[test]
fn test_go_compat_deserialize_collection_object() {
    // Go's CollectionKind object format
    let json = r#"{"Array": true, "CollectionID": "bafkrei123"}"#;
    let result: FieldKind = serde_json::from_str(json).unwrap();
    assert_eq!(
        result,
        FieldKind::Relation {
            collection_id: "bafkrei123".to_string(),
            is_array: true
        }
    );
}

#[test]
fn test_go_compat_deserialize_selfkind_object() {
    // Go's SelfKind object format
    let json = r#"{"RelativeID": "parent", "Array": false}"#;
    let result: FieldKind = serde_json::from_str(json).unwrap();
    assert_eq!(
        result,
        FieldKind::SelfRef {
            relative_id: "parent".to_string(),
            is_array: false
        }
    );
}

#[test]
fn test_go_compat_deserialize_named_object() {
    // Go's NamedKind object format
    let json = r#"{"Name": "User", "Array": true}"#;
    let result: FieldKind = serde_json::from_str(json).unwrap();
    assert_eq!(
        result,
        FieldKind::Named {
            name: "User".to_string(),
            is_array: true
        }
    );
}

#[test]
fn test_go_compat_all_scalar_kinds_roundtrip() {
    // All user-facing scalar kinds should roundtrip through both accepted encodings.
    let scalars = vec![
        (FieldKind::Scalar(ScalarKind::DocID), 1, "ID"),
        (FieldKind::Scalar(ScalarKind::Bool), 2, "Boolean"),
        (FieldKind::Scalar(ScalarKind::Int), 4, "Int"),
        (FieldKind::Scalar(ScalarKind::Float64), 6, "Float64"),
        (FieldKind::Scalar(ScalarKind::Float32), 8, "Float32"),
        (FieldKind::Scalar(ScalarKind::DateTime), 10, "DateTime"),
        (FieldKind::Scalar(ScalarKind::String), 11, "String"),
        (FieldKind::Scalar(ScalarKind::Blob), 13, "Blob"),
        (FieldKind::Scalar(ScalarKind::Json), 14, "JSON"),
        (
            FieldKind::Scalar(ScalarKind::NonNillableBool),
            15,
            "Boolean!",
        ),
        (FieldKind::Scalar(ScalarKind::NonNillableInt), 23, "Int!"),
        (
            FieldKind::Scalar(ScalarKind::NonNillableFloat64),
            24,
            "Float64!",
        ),
        (
            FieldKind::Scalar(ScalarKind::NonNillableFloat32),
            25,
            "Float32!",
        ),
        (
            FieldKind::Scalar(ScalarKind::NonNillableString),
            26,
            "String!",
        ),
        (
            FieldKind::Scalar(ScalarKind::NonNillableDateTime),
            27,
            "DateTime!",
        ),
        (FieldKind::Scalar(ScalarKind::NonNillableBlob), 28, "Blob!"),
        (FieldKind::Scalar(ScalarKind::NonNillableJson), 29, "JSON!"),
    ];

    for (kind, expected_int, canonical) in scalars {
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, expected_int.to_string());
        let parsed_numeric: FieldKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, parsed_numeric);

        let string_json = serde_json::to_string(canonical).unwrap();
        let parsed_string: FieldKind = serde_json::from_str(&string_json).unwrap();
        assert_eq!(kind, parsed_string);
    }

    let none = FieldKind::Scalar(ScalarKind::None);
    let parsed_none: FieldKind = serde_json::from_str("0").unwrap();
    assert_eq!(none, parsed_none);
}

#[test]
fn test_go_compat_all_array_kinds_roundtrip() {
    // All array kinds should roundtrip through both accepted encodings.
    let arrays = vec![
        (
            FieldKind::ScalarArray(ScalarArrayKind::BoolArray),
            3,
            "[Boolean!]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::IntArray),
            5,
            "[Int!]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::Float64Array),
            7,
            "[Float64!]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::Float32Array),
            9,
            "[Float32!]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::StringArray),
            12,
            "[String!]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::NillableBoolArray),
            18,
            "[Boolean]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::NillableIntArray),
            19,
            "[Int]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::NillableFloat64Array),
            20,
            "[Float64]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::NillableStringArray),
            21,
            "[String]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::NillableFloat32Array),
            22,
            "[Float32]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::DateTimeArray),
            30,
            "[DateTime!]",
        ),
        (
            FieldKind::ScalarArray(ScalarArrayKind::NillableDateTimeArray),
            31,
            "[DateTime]",
        ),
    ];

    for (kind, expected_int, canonical) in arrays {
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, expected_int.to_string());
        let parsed_numeric: FieldKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, parsed_numeric);

        let string_json = serde_json::to_string(canonical).unwrap();
        let parsed_string: FieldKind = serde_json::from_str(&string_json).unwrap();
        assert_eq!(kind, parsed_string);
    }
}
