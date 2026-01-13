//! Integration tests with Go-generated JSON fixtures.
//!
//! These tests verify that Rust can correctly deserialize JSON produced by Go DefraDB,
//! ensuring cross-language compatibility for schema definitions.
//!
//! The JSON fixtures in this file were generated from Go DefraDB using:
//! `go test -v -run TestGenerateRustFixtures ./client/`

use schema::{CType, CollectionVersion, FieldDescription, FieldKind, ScalarArrayKind, ScalarKind};

// ============================================================================
// FieldKind JSON Fixtures from Go
// ============================================================================

/// Test fixture: Go serializes ScalarKind as integer values
/// Go code: json.Marshal(FieldKind_NILLABLE_BOOL) -> 2
#[test]
fn test_go_fixture_scalar_kinds() {
    // Go produces bare integers for scalar kinds
    let test_cases = vec![
        ("0", FieldKind::Scalar(ScalarKind::None)),
        ("1", FieldKind::Scalar(ScalarKind::DocID)),
        ("2", FieldKind::Scalar(ScalarKind::Bool)),
        ("4", FieldKind::Scalar(ScalarKind::Int)),
        ("6", FieldKind::Scalar(ScalarKind::Float64)),
        ("8", FieldKind::Scalar(ScalarKind::Float32)),
        ("10", FieldKind::Scalar(ScalarKind::DateTime)),
        ("11", FieldKind::Scalar(ScalarKind::String)),
        ("13", FieldKind::Scalar(ScalarKind::Blob)),
        ("14", FieldKind::Scalar(ScalarKind::Json)),
    ];

    for (go_json, expected) in test_cases {
        let parsed: FieldKind = serde_json::from_str(go_json)
            .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", go_json, e));
        assert_eq!(
            parsed, expected,
            "Mismatch for Go JSON '{}': expected {:?}, got {:?}",
            go_json, expected, parsed
        );
    }
}

/// Test fixture: Go serializes ScalarArrayKind as integer values
/// Go code: json.Marshal(FieldKind_INT_ARRAY) -> 5
#[test]
fn test_go_fixture_array_kinds() {
    let test_cases = vec![
        ("3", FieldKind::ScalarArray(ScalarArrayKind::BoolArray)),
        ("5", FieldKind::ScalarArray(ScalarArrayKind::IntArray)),
        ("7", FieldKind::ScalarArray(ScalarArrayKind::Float64Array)),
        ("9", FieldKind::ScalarArray(ScalarArrayKind::Float32Array)),
        ("12", FieldKind::ScalarArray(ScalarArrayKind::StringArray)),
        (
            "18",
            FieldKind::ScalarArray(ScalarArrayKind::NillableBoolArray),
        ),
        (
            "19",
            FieldKind::ScalarArray(ScalarArrayKind::NillableIntArray),
        ),
        (
            "20",
            FieldKind::ScalarArray(ScalarArrayKind::NillableFloat64Array),
        ),
        (
            "21",
            FieldKind::ScalarArray(ScalarArrayKind::NillableStringArray),
        ),
        (
            "22",
            FieldKind::ScalarArray(ScalarArrayKind::NillableFloat32Array),
        ),
    ];

    for (go_json, expected) in test_cases {
        let parsed: FieldKind = serde_json::from_str(go_json)
            .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", go_json, e));
        assert_eq!(parsed, expected);
    }
}

/// Test fixture: Go CollectionKind JSON format
/// Go code: json.Marshal(&CollectionKind{Array: true, CollectionID: "bafkrei123"})
#[test]
fn test_go_fixture_collection_kind() {
    // Go produces this exact JSON format for CollectionKind
    let go_json = r#"{"Array":false,"CollectionID":"bafkrei123"}"#;

    let parsed: FieldKind = serde_json::from_str(go_json).unwrap();
    assert_eq!(
        parsed,
        FieldKind::Relation {
            collection_id: "bafkrei123".to_string(),
            is_array: false
        }
    );

    // Also test with Array: true
    let go_json_array = r#"{"Array":true,"CollectionID":"bafkrei456"}"#;
    let parsed_array: FieldKind = serde_json::from_str(go_json_array).unwrap();
    assert_eq!(
        parsed_array,
        FieldKind::Relation {
            collection_id: "bafkrei456".to_string(),
            is_array: true
        }
    );
}

/// Test fixture: Go SelfKind JSON format
/// Go code: json.Marshal(&SelfKind{RelativeID: "parent", Array: false})
#[test]
fn test_go_fixture_self_kind() {
    // Go produces this exact JSON format for SelfKind
    let go_json = r#"{"RelativeID":"parent","Array":false}"#;

    let parsed: FieldKind = serde_json::from_str(go_json).unwrap();
    assert_eq!(
        parsed,
        FieldKind::SelfRef {
            relative_id: "parent".to_string(),
            is_array: false
        }
    );

    // Empty RelativeID (self-reference to own collection)
    let go_json_self = r#"{"RelativeID":"","Array":false}"#;
    let parsed_self: FieldKind = serde_json::from_str(go_json_self).unwrap();
    assert_eq!(
        parsed_self,
        FieldKind::SelfRef {
            relative_id: String::new(),
            is_array: false
        }
    );

    // Array self reference
    let go_json_array = r#"{"RelativeID":"","Array":true}"#;
    let parsed_array: FieldKind = serde_json::from_str(go_json_array).unwrap();
    assert_eq!(
        parsed_array,
        FieldKind::SelfRef {
            relative_id: String::new(),
            is_array: true
        }
    );
}

/// Test fixture: Go NamedKind JSON format
/// Go code: json.Marshal(&NamedKind{Name: "User", Array: false})
#[test]
fn test_go_fixture_named_kind() {
    let go_json = r#"{"Name":"User","Array":false}"#;

    let parsed: FieldKind = serde_json::from_str(go_json).unwrap();
    assert_eq!(
        parsed,
        FieldKind::Named {
            name: "User".to_string(),
            is_array: false
        }
    );

    let go_json_array = r#"{"Name":"Author","Array":true}"#;
    let parsed_array: FieldKind = serde_json::from_str(go_json_array).unwrap();
    assert_eq!(
        parsed_array,
        FieldKind::Named {
            name: "Author".to_string(),
            is_array: true
        }
    );
}

/// Test fixture: Go string type representations
/// Go's FieldKindStringToEnumMapping allows string inputs
#[test]
fn test_go_fixture_string_type_names() {
    let test_cases = vec![
        (r#""ID""#, FieldKind::Scalar(ScalarKind::DocID)),
        (r#""Boolean""#, FieldKind::Scalar(ScalarKind::Bool)),
        (r#""Int""#, FieldKind::Scalar(ScalarKind::Int)),
        (r#""Float""#, FieldKind::Scalar(ScalarKind::Float64)),
        (r#""Float64""#, FieldKind::Scalar(ScalarKind::Float64)),
        (r#""Float32""#, FieldKind::Scalar(ScalarKind::Float32)),
        (r#""String""#, FieldKind::Scalar(ScalarKind::String)),
        (r#""DateTime""#, FieldKind::Scalar(ScalarKind::DateTime)),
        (r#""Blob""#, FieldKind::Scalar(ScalarKind::Blob)),
        (r#""JSON""#, FieldKind::Scalar(ScalarKind::Json)),
        // Array types
        (
            r#""[Boolean!]""#,
            FieldKind::ScalarArray(ScalarArrayKind::BoolArray),
        ),
        (
            r#""[Boolean]""#,
            FieldKind::ScalarArray(ScalarArrayKind::NillableBoolArray),
        ),
        (
            r#""[Int!]""#,
            FieldKind::ScalarArray(ScalarArrayKind::IntArray),
        ),
        (
            r#""[Int]""#,
            FieldKind::ScalarArray(ScalarArrayKind::NillableIntArray),
        ),
        (
            r#""[String!]""#,
            FieldKind::ScalarArray(ScalarArrayKind::StringArray),
        ),
        (
            r#""[String]""#,
            FieldKind::ScalarArray(ScalarArrayKind::NillableStringArray),
        ),
        (
            r#""[Float64!]""#,
            FieldKind::ScalarArray(ScalarArrayKind::Float64Array),
        ),
        (
            r#""[Float64]""#,
            FieldKind::ScalarArray(ScalarArrayKind::NillableFloat64Array),
        ),
        (
            r#""[Float32!]""#,
            FieldKind::ScalarArray(ScalarArrayKind::Float32Array),
        ),
        (
            r#""[Float32]""#,
            FieldKind::ScalarArray(ScalarArrayKind::NillableFloat32Array),
        ),
        // Self reference (Go uses "Self" from request.SelfTypeName)
        (
            r#""Self""#,
            FieldKind::SelfRef {
                relative_id: String::new(),
                is_array: false,
            },
        ),
        (
            r#""[Self]""#,
            FieldKind::SelfRef {
                relative_id: String::new(),
                is_array: true,
            },
        ),
    ];

    for (go_json, expected) in test_cases {
        let parsed: FieldKind = serde_json::from_str(go_json)
            .unwrap_or_else(|e| panic!("Failed to parse '{}': {}", go_json, e));
        assert_eq!(parsed, expected, "Mismatch for Go JSON '{}'", go_json);
    }
}

/// Test fixture: Unknown string types become NamedKind
/// Go's parseFieldKind converts unknown strings to NamedKind
#[test]
fn test_go_fixture_named_string_fallback() {
    // Unknown type names become NamedKind
    let parsed: FieldKind = serde_json::from_str(r#""Author""#).unwrap();
    assert_eq!(
        parsed,
        FieldKind::Named {
            name: "Author".to_string(),
            is_array: false
        }
    );

    // Array syntax "[Name]" is detected
    let parsed_array: FieldKind = serde_json::from_str(r#""[Publisher]""#).unwrap();
    assert_eq!(
        parsed_array,
        FieldKind::Named {
            name: "Publisher".to_string(),
            is_array: true
        }
    );
}

// ============================================================================
// FieldDescription JSON Fixtures from Go (ACTUAL Go format with PascalCase)
// ============================================================================

/// Test fixture: Go FieldDescription JSON format - simple scalar field
/// Generated from: CollectionFieldDescription{FieldID: "1", Name: "_docID", Kind: FieldKind_DocID, Typ: LWW_REGISTER}
#[test]
fn test_go_fixture_field_description_scalar() {
    // This is the ACTUAL Go output format
    let go_json = r#"{"FieldID":"1","Name":"_docID","Kind":1,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0}"#;

    let parsed: FieldDescription = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.id, "1");
    assert_eq!(parsed.name, "_docID");
    assert_eq!(parsed.kind, FieldKind::doc_id());
    assert_eq!(parsed.crdt_type, CType::LwwRegister);
    assert_eq!(parsed.relation_name, None);
    assert!(!parsed.is_primary);
    assert_eq!(parsed.default_value, None);
    assert_eq!(parsed.size, 0);
}

/// Test fixture: Go FieldDescription with string default
/// Generated from: CollectionFieldDescription{..., DefaultValue: "anonymous"}
#[test]
fn test_go_fixture_field_description_with_default() {
    let go_json = r#"{"FieldID":"2","Name":"username","Kind":11,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":"anonymous","Size":0}"#;

    let parsed: FieldDescription = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.name, "username");
    assert_eq!(parsed.kind, FieldKind::string());
    assert_eq!(parsed.default_value, Some(serde_json::json!("anonymous")));
}

/// Test fixture: Go FieldDescription with relation
/// Generated from: CollectionFieldDescription{..., Kind: CollectionKind, RelationName: "post_author", IsPrimary: true}
#[test]
fn test_go_fixture_field_description_relation() {
    let go_json = r#"{"FieldID":"4","Name":"author","Kind":{"Array":false,"CollectionID":"users-v1"},"Typ":2,"RelationName":"post_author","IsPrimary":true,"DefaultValue":null,"Size":0}"#;

    let parsed: FieldDescription = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.name, "author");
    assert_eq!(
        parsed.kind,
        FieldKind::Relation {
            collection_id: "users-v1".to_string(),
            is_array: false
        }
    );
    assert_eq!(parsed.crdt_type, CType::Object);
    assert_eq!(parsed.relation_name, Some("post_author".to_string()));
    assert!(parsed.is_primary);
}

/// Test fixture: Go FieldDescription with array kind
/// Generated from: CollectionFieldDescription{..., Kind: FieldKind_STRING_ARRAY, Size: 10}
#[test]
fn test_go_fixture_field_description_array() {
    let go_json = r#"{"FieldID":"5","Name":"tags","Kind":12,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":10}"#;

    let parsed: FieldDescription = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.name, "tags");
    assert_eq!(parsed.kind, FieldKind::string_array());
    assert_eq!(parsed.size, 10);
}

/// Test fixture: Go FieldDescription with counter type
/// Generated from: CollectionFieldDescription{..., Kind: FieldKind_NILLABLE_INT, Typ: PN_COUNTER}
#[test]
fn test_go_fixture_field_description_counter() {
    let go_json = r#"{"FieldID":"3","Name":"view_count","Kind":4,"Typ":4,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0}"#;

    let parsed: FieldDescription = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.name, "view_count");
    assert_eq!(parsed.kind, FieldKind::int());
    assert_eq!(parsed.crdt_type, CType::PnCounter);
}

// ============================================================================
// CollectionVersion JSON Fixtures from Go (ACTUAL Go format)
// ============================================================================

/// Test fixture: Complete Go collection JSON
/// Note: Go includes many additional fields that we handle with serde(default)
#[test]
fn test_go_fixture_collection_version() {
    // This is ACTUAL Go output - note all the extra fields Go includes
    let go_json = r#"{"Name":"users","VersionID":"v1","CollectionID":"bafkreiusers123","CollectionSet":null,"Query":null,"PreviousVersion":null,"Fields":[{"FieldID":"1","Name":"_docID","Kind":1,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0},{"FieldID":"2","Name":"name","Kind":11,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0},{"FieldID":"3","Name":"age","Kind":4,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0}],"Indexes":null,"EncryptedIndexes":null,"Policy":null,"IsActive":true,"IsMaterialized":false,"IsBranchable":false,"IsEmbeddedOnly":false,"IsPlaceholder":false,"VectorEmbeddings":null}"#;

    let parsed: CollectionVersion = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.name, "users");
    assert_eq!(parsed.version_id, "v1");
    assert_eq!(parsed.collection_id, "bafkreiusers123");
    assert!(parsed.is_active);
    assert_eq!(parsed.fields.len(), 3);

    // Verify fields
    assert_eq!(parsed.fields[0].name, "_docID");
    assert_eq!(parsed.fields[0].kind, FieldKind::doc_id());
    assert_eq!(parsed.fields[1].name, "name");
    assert_eq!(parsed.fields[1].kind, FieldKind::string());
    assert_eq!(parsed.fields[2].name, "age");
    assert_eq!(parsed.fields[2].kind, FieldKind::int());
}

/// Test fixture: Go collection with relations
#[test]
fn test_go_fixture_collection_with_relations() {
    let go_json = r#"{"Name":"posts","VersionID":"v1","CollectionID":"bafkreiposts456","CollectionSet":null,"Query":null,"PreviousVersion":null,"Fields":[{"FieldID":"1","Name":"_docID","Kind":1,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0},{"FieldID":"2","Name":"title","Kind":11,"Typ":1,"RelationName":null,"IsPrimary":false,"DefaultValue":null,"Size":0},{"FieldID":"3","Name":"author","Kind":{"Array":false,"CollectionID":"bafkreiusers123"},"Typ":2,"RelationName":"user_posts","IsPrimary":true,"DefaultValue":null,"Size":0}],"Indexes":null,"EncryptedIndexes":null,"Policy":null,"IsActive":true,"IsMaterialized":false,"IsBranchable":false,"IsEmbeddedOnly":false,"IsPlaceholder":false,"VectorEmbeddings":null}"#;

    let parsed: CollectionVersion = serde_json::from_str(go_json).unwrap();
    assert_eq!(parsed.name, "posts");
    assert_eq!(parsed.fields.len(), 3);

    // Verify relation field
    let author_field = &parsed.fields[2];
    assert_eq!(author_field.name, "author");
    assert_eq!(
        author_field.kind,
        FieldKind::Relation {
            collection_id: "bafkreiusers123".to_string(),
            is_array: false
        }
    );
    assert_eq!(author_field.crdt_type, CType::Object);
    assert_eq!(author_field.relation_name, Some("user_posts".to_string()));
    assert!(author_field.is_primary);
}

// ============================================================================
// Roundtrip Tests (Rust -> JSON -> Rust)
// ============================================================================

/// Verify Rust serialization produces Go-compatible format
#[test]
fn test_rust_serialization_matches_go_format() {
    // Scalar: should serialize to integer
    let scalar = FieldKind::int();
    let json = serde_json::to_string(&scalar).unwrap();
    assert_eq!(json, "4", "Int should serialize to '4'");

    // Array: should serialize to integer
    let array = FieldKind::string_array();
    let json = serde_json::to_string(&array).unwrap();
    assert_eq!(json, "12", "StringArray should serialize to '12'");

    // Relation: should serialize to object with CollectionID
    let relation = FieldKind::relation("users-v1", true);
    let json = serde_json::to_string(&relation).unwrap();
    assert!(json.contains("CollectionID"), "Should contain CollectionID");
    assert!(
        json.contains("users-v1"),
        "Should contain collection ID value"
    );
    assert!(json.contains("Array"), "Should contain Array key");

    // SelfRef: should serialize to object with RelativeID
    let self_ref = FieldKind::self_ref("parent", false);
    let json = serde_json::to_string(&self_ref).unwrap();
    assert!(json.contains("RelativeID"), "Should contain RelativeID");
    assert!(json.contains("parent"), "Should contain relative ID value");

    // Named: should serialize to object with Name
    let named = FieldKind::named("Author", true);
    let json = serde_json::to_string(&named).unwrap();
    assert!(json.contains("Name"), "Should contain Name");
    assert!(json.contains("Author"), "Should contain name value");
}

/// Verify FieldDescription serialization uses Go's PascalCase keys
#[test]
fn test_field_description_serialization_go_format() {
    let field = FieldDescription::new("1", "name", FieldKind::string());
    let json = serde_json::to_string(&field).unwrap();

    // Should use Go's PascalCase keys
    assert!(json.contains("\"FieldID\""), "Should use FieldID not id");
    assert!(json.contains("\"Name\""), "Should use Name not name");
    assert!(json.contains("\"Kind\""), "Should use Kind not kind");
    assert!(json.contains("\"Typ\""), "Should use Typ not crdt_type");
    assert!(json.contains("\"RelationName\""), "Should use RelationName");
    assert!(json.contains("\"IsPrimary\""), "Should use IsPrimary");
    assert!(json.contains("\"DefaultValue\""), "Should use DefaultValue");
    assert!(json.contains("\"Size\""), "Should use Size");
}

/// Verify CollectionVersion serialization uses Go's PascalCase keys
#[test]
fn test_collection_version_serialization_go_format() {
    let collection = CollectionVersion::new(
        "users",
        "v1",
        "bafkrei123",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    );
    let json = serde_json::to_string(&collection).unwrap();

    // Should use Go's PascalCase keys
    assert!(json.contains("\"Name\""), "Should use Name");
    assert!(json.contains("\"VersionID\""), "Should use VersionID");
    assert!(json.contains("\"CollectionID\""), "Should use CollectionID");
    assert!(json.contains("\"Fields\""), "Should use Fields");
    assert!(json.contains("\"IsActive\""), "Should use IsActive");
}

/// Verify full collection roundtrip maintains Go compatibility
#[test]
fn test_collection_roundtrip_go_compatible() {
    let collection = CollectionVersion::new(
        "articles",
        "v1",
        "bafkreiarticles",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "views", FieldKind::int()).with_crdt_type(CType::PnCounter),
            FieldDescription::new("4", "tags", FieldKind::string_array()),
            FieldDescription::new("5", "author", FieldKind::relation("users", false))
                .with_relation_name("article_author")
                .as_primary(),
        ],
    );

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&collection).unwrap();

    // Parse back
    let parsed: CollectionVersion = serde_json::from_str(&json).unwrap();

    // Verify equality
    assert_eq!(collection.name, parsed.name);
    assert_eq!(collection.version_id, parsed.version_id);
    assert_eq!(collection.collection_id, parsed.collection_id);
    assert_eq!(collection.fields.len(), parsed.fields.len());

    for (orig, parsed) in collection.fields.iter().zip(parsed.fields.iter()) {
        assert_eq!(orig.id, parsed.id);
        assert_eq!(orig.name, parsed.name);
        assert_eq!(orig.kind, parsed.kind);
        assert_eq!(orig.crdt_type, parsed.crdt_type);
        assert_eq!(orig.relation_name, parsed.relation_name);
        assert_eq!(orig.is_primary, parsed.is_primary);
    }
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

/// Test handling of unknown/future field kind integers
#[test]
fn test_unknown_field_kind_integer() {
    // Go might introduce new field kinds in the future
    // Unknown integers should deserialize to ScalarKind::None
    let unknown_json = "99";
    let parsed: FieldKind = serde_json::from_str(unknown_json).unwrap();
    assert_eq!(parsed, FieldKind::Scalar(ScalarKind::None));
}

/// Test handling of deprecated field kind integers (15, 16, 17)
#[test]
fn test_deprecated_field_kind_integers() {
    // Go has reserved/deprecated values 15, 16, 17
    // They should deserialize gracefully to None
    for deprecated in [15u8, 16, 17] {
        let json = deprecated.to_string();
        let parsed: FieldKind = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed,
            FieldKind::Scalar(ScalarKind::None),
            "Deprecated value {} should become None",
            deprecated
        );
    }
}

/// Test null value handling
#[test]
fn test_null_field_kind() {
    let null_json = "null";
    let parsed: FieldKind = serde_json::from_str(null_json).unwrap();
    assert_eq!(parsed, FieldKind::Scalar(ScalarKind::None));
}

/// Test minimal field description (only required fields in Go format)
#[test]
fn test_minimal_go_field_description() {
    // Go always includes all fields (with null/zero values), but we should
    // handle cases where only some fields are present
    let minimal_json = r#"{
        "FieldID": "1",
        "Name": "test",
        "Kind": 11
    }"#;

    let parsed: FieldDescription = serde_json::from_str(minimal_json).unwrap();
    assert_eq!(parsed.id, "1");
    assert_eq!(parsed.name, "test");
    assert_eq!(parsed.kind, FieldKind::string());
    assert_eq!(parsed.crdt_type, CType::LwwRegister); // default
    assert_eq!(parsed.relation_name, None);
    assert!(!parsed.is_primary);
    assert_eq!(parsed.default_value, None);
    assert_eq!(parsed.size, 0);
}

/// Test that Go's extra CollectionVersion fields are ignored gracefully
#[test]
fn test_go_extra_collection_fields_ignored() {
    // Go CollectionVersion has many fields we don't implement yet.
    // We should be able to parse Go JSON even with these extra fields.
    let go_json_with_extras = r#"{
        "Name": "test",
        "VersionID": "v1",
        "CollectionID": "coll-1",
        "Fields": [],
        "IsActive": true,
        "CollectionSet": null,
        "Query": null,
        "PreviousVersion": null,
        "Indexes": [],
        "EncryptedIndexes": null,
        "Policy": null,
        "IsMaterialized": false,
        "IsBranchable": true,
        "IsEmbeddedOnly": false,
        "IsPlaceholder": false,
        "VectorEmbeddings": null
    }"#;

    let parsed: CollectionVersion = serde_json::from_str(go_json_with_extras).unwrap();
    assert_eq!(parsed.name, "test");
    assert_eq!(parsed.version_id, "v1");
    assert!(parsed.is_active);
}
