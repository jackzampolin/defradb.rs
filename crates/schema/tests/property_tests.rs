//! Property-based tests for schema validation.
//!
//! These tests use proptest to verify that schema types maintain invariants
//! across a wide range of randomly generated inputs.

use proptest::prelude::*;
use schema::{
    validate_schema, CType, CollectionBuilder, CollectionVersion, FieldDescription, FieldKind,
    ScalarArrayKind, ScalarKind,
};
use std::collections::HashMap;

// ============================================================================
// Arbitrary implementations for schema types
// ============================================================================

fn arb_scalar_kind() -> impl Strategy<Value = ScalarKind> {
    prop_oneof![
        Just(ScalarKind::None),
        Just(ScalarKind::DocID),
        Just(ScalarKind::Bool),
        Just(ScalarKind::Int),
        Just(ScalarKind::Float64),
        Just(ScalarKind::Float32),
        Just(ScalarKind::DateTime),
        Just(ScalarKind::String),
        Just(ScalarKind::Blob),
        Just(ScalarKind::Json),
    ]
}

fn arb_scalar_array_kind() -> impl Strategy<Value = ScalarArrayKind> {
    prop_oneof![
        Just(ScalarArrayKind::BoolArray),
        Just(ScalarArrayKind::IntArray),
        Just(ScalarArrayKind::Float64Array),
        Just(ScalarArrayKind::Float32Array),
        Just(ScalarArrayKind::StringArray),
        Just(ScalarArrayKind::NillableBoolArray),
        Just(ScalarArrayKind::NillableIntArray),
        Just(ScalarArrayKind::NillableFloat64Array),
        Just(ScalarArrayKind::NillableStringArray),
        Just(ScalarArrayKind::NillableFloat32Array),
    ]
}

fn arb_field_kind() -> impl Strategy<Value = FieldKind> {
    prop_oneof![
        arb_scalar_kind().prop_map(FieldKind::Scalar),
        arb_scalar_array_kind().prop_map(FieldKind::ScalarArray),
        ("[a-z]{1,20}", any::<bool>()).prop_map(|(id, is_array)| FieldKind::Relation {
            collection_id: id,
            is_array
        }),
        ("[a-z]{0,10}", any::<bool>()).prop_map(|(id, is_array)| FieldKind::SelfRef {
            relative_id: id,
            is_array
        }),
        ("[A-Z][a-z]{1,15}", any::<bool>())
            .prop_map(|(name, is_array)| FieldKind::Named { name, is_array }),
    ]
}

fn arb_ctype() -> impl Strategy<Value = CType> {
    prop_oneof![
        Just(CType::None),
        Just(CType::LwwRegister),
        Just(CType::Object),
        Just(CType::Composite),
        Just(CType::PnCounter),
        Just(CType::PCounter),
    ]
}

fn arb_valid_field_name() -> impl Strategy<Value = String> {
    // Valid field names: alphanumeric starting with letter or underscore
    "[_a-zA-Z][_a-zA-Z0-9]{0,30}"
}

fn arb_valid_collection_name() -> impl Strategy<Value = String> {
    "[A-Z][a-zA-Z0-9]{1,30}"
}

// ============================================================================
// FieldKind Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Property: All FieldKind values should roundtrip through JSON serialization
    #[test]
    fn field_kind_json_roundtrip(kind in arb_field_kind()) {
        let json = serde_json::to_string(&kind).unwrap();
        let parsed: FieldKind = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(kind, parsed);
    }

    /// Property: Scalar kinds are never arrays
    #[test]
    fn scalar_kinds_are_not_arrays(scalar in arb_scalar_kind()) {
        let kind = FieldKind::Scalar(scalar);
        prop_assert!(!kind.is_array());
    }

    /// Property: Array kinds are always arrays
    #[test]
    fn array_kinds_are_arrays(array in arb_scalar_array_kind()) {
        let kind = FieldKind::ScalarArray(array);
        prop_assert!(kind.is_array());
    }

    /// Property: Only Int, Float64, Float32 are numeric
    #[test]
    fn only_numeric_types_are_numeric(kind in arb_field_kind()) {
        let is_numeric = kind.is_numeric();
        let expected_numeric = matches!(
            kind,
            FieldKind::Scalar(ScalarKind::Int)
            | FieldKind::Scalar(ScalarKind::Float64)
            | FieldKind::Scalar(ScalarKind::Float32)
        );
        prop_assert_eq!(is_numeric, expected_numeric);
    }

    /// Property: Relation, SelfRef, Named are all relation types
    #[test]
    fn relation_types_are_relations(kind in arb_field_kind()) {
        let is_relation = kind.is_relation();
        let expected_relation = matches!(
            kind,
            FieldKind::Relation { .. } | FieldKind::SelfRef { .. } | FieldKind::Named { .. }
        );
        prop_assert_eq!(is_relation, expected_relation);
    }

    /// Property: Only scalars are scalar types
    #[test]
    fn only_scalars_are_scalar(kind in arb_field_kind()) {
        let is_scalar = kind.is_scalar();
        let expected_scalar = matches!(kind, FieldKind::Scalar(_));
        prop_assert_eq!(is_scalar, expected_scalar);
    }
}

// ============================================================================
// CType Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: CType roundtrips through JSON
    #[test]
    fn ctype_json_roundtrip(ctype in arb_ctype()) {
        let json = serde_json::to_string(&ctype).unwrap();
        let parsed: CType = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(ctype, parsed);
    }

    /// Property: CType compatibility follows expected rules
    #[test]
    fn ctype_compatibility_rules(ctype in arb_ctype(), kind in arb_field_kind()) {
        let compatible = ctype.is_compatible_with(&kind);

        match ctype {
            // Counters are only compatible with numeric types
            CType::PnCounter | CType::PCounter => {
                prop_assert_eq!(compatible, kind.is_numeric());
            }
            // Object type requires object/relation kinds
            CType::Object => {
                prop_assert_eq!(compatible, kind.is_object());
            }
            // None, LwwRegister, Composite are compatible with everything
            CType::None | CType::LwwRegister | CType::Composite => {
                prop_assert!(compatible);
            }
            // Unknown type - compatibility depends on implementation
            CType::Unknown(_) => {
                // No assertion - unknown types have undefined compatibility
            }
        }
    }

    /// Property: Only PnCounter and PCounter are counter types
    #[test]
    fn only_counter_types_are_counters(ctype in arb_ctype()) {
        let is_counter = ctype.is_counter();
        let expected = matches!(ctype, CType::PnCounter | CType::PCounter);
        prop_assert_eq!(is_counter, expected);
    }
}

// ============================================================================
// Collection Validation Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Property: Valid collections always pass validation
    #[test]
    fn valid_collections_pass_validation(
        name in arb_valid_collection_name(),
        field_name in arb_valid_field_name(),
    ) {
        let coll = CollectionBuilder::new(&name, &format!("coll-{}", name.to_lowercase()))
            .scalar("1", "_docID", FieldKind::doc_id())
            .scalar("2", &field_name, FieldKind::string())
            .build();

        let mut collections = HashMap::new();
        collections.insert(name.clone(), coll);
        let result = validate_schema(&collections);
        prop_assert!(result.is_ok());
    }

    /// Property: Collections with duplicate field names fail validation
    #[test]
    fn duplicate_field_names_fail(
        name in arb_valid_collection_name(),
        field_name in arb_valid_field_name(),
    ) {
        let coll = CollectionBuilder::new(&name, &format!("coll-{}", name.to_lowercase()))
            .scalar("1", "_docID", FieldKind::doc_id())
            .scalar("2", &field_name, FieldKind::string())
            .scalar("3", &field_name, FieldKind::int()) // Duplicate!
            .build();

        prop_assert!(coll.validate().is_err());
    }

    /// Property: Counter fields must have numeric types
    #[test]
    fn counter_requires_numeric_type(
        _name in arb_valid_collection_name(),
        scalar in arb_scalar_kind(),
    ) {
        let kind = FieldKind::Scalar(scalar);
        let is_numeric = kind.is_numeric();

        let field = FieldDescription::new("1", "counter_field", kind.clone())
            .with_crdt_type(CType::PnCounter);
        let valid = field.validate().is_ok();

        // Counter should be valid only with numeric types
        prop_assert_eq!(valid, is_numeric);
    }
}

// ============================================================================
// Schema Validation Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Schemas with duplicate collection names fail validation
    #[test]
    fn duplicate_collection_names_fail(name in arb_valid_collection_name()) {
        let coll1 = CollectionBuilder::new(&name, "coll-1")
            .scalar("1", "_docID", FieldKind::doc_id())
            .build();
        let mut coll2 = CollectionBuilder::new(&name, "coll-2") // Same name!
            .scalar("1", "_docID", FieldKind::doc_id())
            .build();
        coll2.collection_id = "coll-2".to_string();

        let mut collections = HashMap::new();
        collections.insert("key1".to_string(), coll1);
        collections.insert("key2".to_string(), coll2);
        let result = validate_schema(&collections);
        prop_assert!(result.is_err());
    }

    /// Property: Empty schemas are always valid
    #[test]
    fn empty_schema_is_valid(_dummy: u8) {
        let collections: HashMap<String, CollectionVersion> = HashMap::new();
        let result = validate_schema(&collections);
        prop_assert!(result.is_ok());
    }
}

// ============================================================================
// Serialization Format Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Property: Scalar kinds serialize to their integer values
    #[test]
    fn scalar_serializes_to_integer(scalar in arb_scalar_kind()) {
        let kind = FieldKind::Scalar(scalar);
        let json = serde_json::to_string(&kind).unwrap();

        // Should be just a number, not an object or string
        let first_char = json.chars().next().unwrap();
        prop_assert!(first_char.is_ascii_digit(), "Expected number, got: {}", json);

        // Should parse as a number
        let num: u8 = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(num, scalar as u8);
    }

    /// Property: Array kinds serialize to their integer values
    #[test]
    fn array_serializes_to_integer(array in arb_scalar_array_kind()) {
        let kind = FieldKind::ScalarArray(array);
        let json = serde_json::to_string(&kind).unwrap();

        // Should be just a number
        let num: u8 = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(num, array as u8);
    }

    /// Property: Relation kinds serialize to objects with CollectionID
    #[test]
    fn relation_serializes_with_collection_id(
        collection_id in "[a-z]{1,20}",
        is_array in any::<bool>(),
    ) {
        let kind = FieldKind::Relation { collection_id: collection_id.clone(), is_array };
        let json = serde_json::to_string(&kind).unwrap();

        // Should be an object (starts with open brace)
        let first_char = json.chars().next().unwrap();
        prop_assert_eq!(first_char, '{', "Expected object, got: {}", json);

        // Should contain CollectionID
        prop_assert!(json.contains("CollectionID"), "Missing CollectionID in: {}", json);
        prop_assert!(json.contains(&collection_id), "Missing collection_id value in: {}", json);
    }
}

// ============================================================================
// Cross-Format Deserialization Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Property: Integer deserialization produces valid FieldKind
    #[test]
    fn integer_deserializes_to_valid_kind(n in 0u8..30) {
        let json = n.to_string();
        let result: Result<FieldKind, _> = serde_json::from_str(&json);

        // Should always succeed (unknown values become None)
        prop_assert!(result.is_ok());
    }

    /// Property: String type names deserialize correctly
    #[test]
    fn string_type_names_deserialize(
        type_name in prop_oneof![
            Just("Boolean"),
            Just("Int"),
            Just("String"),
            Just("Float64"),
            Just("Float32"),
            Just("DateTime"),
            Just("Blob"),
            Just("JSON"),
            Just("ID"),
        ]
    ) {
        let json = format!(r#""{}""#, type_name);
        let result: Result<FieldKind, _> = serde_json::from_str(&json);
        prop_assert!(result.is_ok());

        // Should be a scalar type
        let kind = result.unwrap();
        prop_assert!(kind.is_scalar());
    }

    /// Property: Array type names deserialize correctly
    #[test]
    fn array_type_names_deserialize(
        type_name in prop_oneof![
            Just("[Int!]"),
            Just("[Int]"),
            Just("[String!]"),
            Just("[String]"),
            Just("[Boolean!]"),
            Just("[Boolean]"),
            Just("[Float64!]"),
            Just("[Float64]"),
        ]
    ) {
        let json = format!(r#""{}""#, type_name);
        let result: Result<FieldKind, _> = serde_json::from_str(&json);
        prop_assert!(result.is_ok());

        // Should be an array type
        let kind = result.unwrap();
        prop_assert!(kind.is_array());
    }
}

// ============================================================================
// Relation Field Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Property: _id field generation produces correct field names
    #[test]
    fn id_field_naming_consistent(field_name in "[a-z]{1,20}") {
        let id_name = CollectionVersion::relation_id_field_name(&field_name);
        prop_assert_eq!(id_name, format!("_{}ID", field_name));
    }

    /// Property: Non-array relation fields always get _id fields
    #[test]
    fn non_array_relations_get_id_fields(
        collection_name in arb_valid_collection_name(),
        field_name in "[a-z]{1,15}",
        target_collection in "[a-z]{1,15}",
    ) {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", &field_name, FieldKind::relation(&target_collection, false))
                .with_relation_name("test_relation"),
        ];
        let mut coll = CollectionVersion::new(&collection_name, "v1", "coll-test", fields);

        let mut counter = 100;
        coll.add_relation_id_fields(|| {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        let expected_id_name = format!("_{}ID", field_name);
        let id_field = coll.field_by_name(&expected_id_name);
        prop_assert!(id_field.is_some(), "Expected _id field for non-array relation");
    }

    /// Property: Array relation fields never get _id fields
    #[test]
    fn array_relations_no_id_fields(
        collection_name in arb_valid_collection_name(),
        field_name in "[a-z]{1,15}",
        target_collection in "[a-z]{1,15}",
    ) {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", &field_name, FieldKind::relation(&target_collection, true))
                .with_relation_name("test_relation"),
        ];
        let mut coll = CollectionVersion::new(&collection_name, "v1", "coll-test", fields);

        coll.add_relation_id_fields(|| "gen-999".to_string()).unwrap();

        let expected_id_name = format!("_{}ID", field_name);
        let id_field = coll.field_by_name(&expected_id_name);
        prop_assert!(id_field.is_none(), "Array relations should NOT get _id fields");
    }

    /// Property: _id fields inherit is_primary from relation field
    #[test]
    fn id_fields_inherit_primary_status(
        field_name in "[a-z]{1,15}",
        is_primary in any::<bool>(),
    ) {
        let mut rel_field = FieldDescription::new("1", &field_name, FieldKind::relation("target", false))
            .with_relation_name("test_relation");
        if is_primary {
            rel_field = rel_field.as_primary();
        }

        let fields = vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            rel_field,
        ];
        let mut coll = CollectionVersion::new("test", "v1", "coll-test", fields);

        coll.add_relation_id_fields(|| "gen-999".to_string()).unwrap();

        let id_field = coll.field_by_name(&format!("_{}ID", field_name)).unwrap();
        prop_assert_eq!(id_field.is_primary, is_primary, "is_primary should be inherited");
    }

    /// Property: _id fields always have LwwRegister CRDT type
    #[test]
    fn id_fields_have_lww_crdt(field_name in "[a-z]{1,15}") {
        let fields = vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            FieldDescription::new("1", &field_name, FieldKind::relation("target", false))
                .with_relation_name("test_relation"),
        ];
        let mut coll = CollectionVersion::new("test", "v1", "coll-test", fields);

        coll.add_relation_id_fields(|| "gen-999".to_string()).unwrap();

        let id_field = coll.field_by_name(&format!("_{}ID", field_name)).unwrap();
        prop_assert_eq!(id_field.crdt_type, CType::LwwRegister);
    }

    /// Property: add_relation_id_fields is idempotent
    #[test]
    fn id_field_generation_idempotent(field_name in "[a-z]{1,15}") {
        let fields = vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            FieldDescription::new("1", &field_name, FieldKind::relation("target", false))
                .with_relation_name("test_relation"),
        ];
        let mut coll = CollectionVersion::new("test", "v1", "coll-test", fields);

        let mut counter = 100;
        coll.add_relation_id_fields(|| {
            counter += 1;
            counter.to_string()
        })
        .unwrap();
        let field_count_after_first = coll.fields.len();

        coll.add_relation_id_fields(|| {
            counter += 1;
            counter.to_string()
        })
        .unwrap();
        let field_count_after_second = coll.fields.len();

        prop_assert_eq!(
            field_count_after_first, field_count_after_second,
            "Idempotent: field count should not change"
        );
    }

    /// Property: _id fields inherit relation_name from relation field
    #[test]
    fn id_fields_inherit_relation_name(
        field_name in "[a-z]{1,15}",
        relation_name in "[a-z_]{3,20}",
    ) {
        let fields = vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            FieldDescription::new("1", &field_name, FieldKind::relation("target", false))
                .with_relation_name(&relation_name),
        ];
        let mut coll = CollectionVersion::new("test", "v1", "coll-test", fields);

        coll.add_relation_id_fields(|| "gen-999".to_string()).unwrap();

        let id_field = coll.field_by_name(&format!("_{}ID", field_name)).unwrap();
        prop_assert_eq!(
            id_field.relation_name.clone(),
            Some(relation_name),
            "relation_name should be inherited"
        );
    }
}

// ============================================================================
// Primary Validation Property Tests
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Exactly one primary in two-sided relation passes validation
    #[test]
    fn one_primary_valid(
        col1_name in arb_valid_collection_name(),
        col2_name in arb_valid_collection_name(),
        relation_name in "[a-z_]{3,20}",
        primary_on_first in any::<bool>(),
    ) {
        // Ensure different names
        prop_assume!(col1_name != col2_name);

        let mut field1 = FieldDescription::new("1", "rel", FieldKind::relation(&col2_name.to_lowercase(), false))
            .with_relation_name(&relation_name);
        let mut field2 = FieldDescription::new("1", "rel", FieldKind::relation(&col1_name.to_lowercase(), false))
            .with_relation_name(&relation_name);

        if primary_on_first {
            field1 = field1.as_primary();
        } else {
            field2 = field2.as_primary();
        }

        let coll1 = CollectionVersion::new(&col1_name, "v1", "coll-1", vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            field1,
        ]);
        let coll2 = CollectionVersion::new(&col2_name, "v1", "coll-2", vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            field2,
        ]);

        let mut collections = HashMap::new();
        collections.insert(col1_name.to_lowercase(), coll1);
        collections.insert(col2_name.to_lowercase(), coll2);

        let result = validate_schema(&collections);
        prop_assert!(result.is_ok(), "One primary should pass: {:?}", result.err());
    }

    /// Property: Both sides primary in two-sided relation fails validation
    #[test]
    fn both_primary_invalid(
        col1_name in arb_valid_collection_name(),
        col2_name in arb_valid_collection_name(),
        relation_name in "[a-z_]{3,20}",
    ) {
        // Ensure different names
        prop_assume!(col1_name != col2_name);

        let field1 = FieldDescription::new("1", "rel", FieldKind::relation(&col2_name.to_lowercase(), false))
            .with_relation_name(&relation_name)
            .as_primary();
        let field2 = FieldDescription::new("1", "rel", FieldKind::relation(&col1_name.to_lowercase(), false))
            .with_relation_name(&relation_name)
            .as_primary(); // BOTH primary!

        let coll1 = CollectionVersion::new(&col1_name, "v1", "coll-1", vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            field1,
        ]);
        let coll2 = CollectionVersion::new(&col2_name, "v1", "coll-2", vec![
            FieldDescription::new("0", "_docID", FieldKind::doc_id()),
            field2,
        ]);

        let mut collections = HashMap::new();
        collections.insert(col1_name.to_lowercase(), coll1);
        collections.insert(col2_name.to_lowercase(), coll2);

        let result = validate_schema(&collections);
        prop_assert!(result.is_err(), "Both primary should fail");
    }
}
