//! Integration tests for Document type

use document::{Document, NormalValue, SDN_NAMESPACE_V0};

#[test]
fn test_new_document() {
    let doc = Document::new();
    assert!(doc.id().is_none());
    assert!(doc.is_empty());
    assert!(doc.is_dirty());
}

#[test]
fn test_set_and_get() {
    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    doc.set("active", true);

    assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("Alice"));
    assert_eq!(doc.get("age").and_then(|v| v.as_int()), Some(30));
    assert_eq!(doc.get("active").and_then(|v| v.as_bool()), Some(true));
    assert!(doc.get("missing").is_none());
}

#[test]
fn test_from_json() {
    let json = r#"{"name": "Bob", "age": 25, "active": true}"#;
    let doc = Document::from_json_str(json).unwrap();

    assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("Bob"));
    assert_eq!(doc.get("age").and_then(|v| v.as_int()), Some(25));
    assert_eq!(doc.get("active").and_then(|v| v.as_bool()), Some(true));
    // No client-side DocID generation: identity is assigned at save time
    // from the genesis composite block CID.
    assert!(doc.id().is_none());
}

#[test]
fn test_from_json_with_doc_id() {
    let genesis_cid: cid::Cid = "bafyreie7rtdexuf47f633477mfieshkeh5rwnjeommkgqrzl22n6g4bfmm"
        .parse()
        .unwrap();
    let doc_id_str = document::DocID::new_v0(genesis_cid).to_string();

    let json = format!(r#"{{"_docID": "{}", "name": "Test2"}}"#, doc_id_str);
    let doc = Document::from_json_str(&json).unwrap();

    assert_eq!(doc.id().unwrap().to_string(), doc_id_str);
}

#[test]
fn test_to_map() {
    let mut doc = Document::new();
    doc.set("name", "Charlie");
    doc.set("count", 42i64);

    let map = doc.to_map().unwrap();
    assert_eq!(
        map.get("name"),
        Some(&serde_json::Value::String("Charlie".into()))
    );
    assert_eq!(map.get("count"), Some(&serde_json::json!(42)));
}

#[test]
fn test_to_map_non_finite_float_error() {
    let mut doc = Document::new();
    doc.set("name", "test");
    doc.set("value", f64::NAN);

    let result = doc.to_map();
    assert!(result.is_err());
}

#[test]
fn test_to_map_infinity_error() {
    let mut doc = Document::new();
    doc.set("value", f64::INFINITY);

    let result = doc.to_map();
    assert!(result.is_err());
}

#[test]
fn test_dirty_tracking() {
    let mut doc = Document::new();
    assert!(doc.is_dirty());

    doc.clean();
    assert!(!doc.is_dirty());

    doc.set("field", "value");
    assert!(doc.is_dirty());
}

#[test]
fn test_dirty_fields() {
    let mut doc = Document::new();
    doc.set("field1", "value1");
    doc.set("field2", "value2");
    doc.clean();

    doc.set("field1", "new_value");

    let dirty: Vec<_> = doc.dirty_fields().collect();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].0, "field1");
}

#[test]
fn test_remove_field() {
    let mut doc = Document::new();
    doc.set("name", "Alice");
    assert!(doc.has_field("name"));

    doc.remove("name");
    assert!(!doc.has_field("name"));
}

#[test]
fn test_len_and_is_empty() {
    let mut doc = Document::new();
    assert!(doc.is_empty());
    assert_eq!(doc.len(), 0);

    doc.set("field", "value");
    assert!(!doc.is_empty());
    assert_eq!(doc.len(), 1);
}

#[test]
fn test_cbor_field_order_independence() {
    // Documents with same content but different field insertion order
    // produce identical canonical CBOR
    let mut doc1 = Document::new();
    doc1.set("a", "first");
    doc1.set("b", "second");
    doc1.set("c", "third");

    let mut doc2 = Document::new();
    doc2.set("c", "third"); // Different insertion order
    doc2.set("a", "first");
    doc2.set("b", "second");

    let mut doc3 = Document::new();
    doc3.set("b", "second"); // Another order
    doc3.set("c", "third");
    doc3.set("a", "first");

    // Canonical CBOR encoding is insertion-order independent — this keeps
    // genesis composite CIDs (and therefore DocIDs) deterministic.
    assert_eq!(doc1.to_cbor().unwrap(), doc2.to_cbor().unwrap());
    assert_eq!(doc2.to_cbor().unwrap(), doc3.to_cbor().unwrap());
}

#[test]
fn test_cbor_encoding() {
    let mut doc = Document::new();
    doc.set("name", "Test");
    doc.set("count", 42i64);

    let cbor = doc.to_cbor().unwrap();
    assert!(!cbor.is_empty());

    // CBOR should be deterministic
    let cbor2 = doc.to_cbor().unwrap();
    assert_eq!(cbor, cbor2);

    // First byte should be 0xa2 (map with 2 entries)
    assert_eq!(cbor[0], 0xa2, "CBOR should start with map marker");
}

#[test]
fn test_cbor_roundtrip() {
    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.set("age", 30i64);
    doc.set("active", true);
    doc.set("score", 95.5);

    let cbor = doc.to_cbor().unwrap();
    let decoded = Document::from_cbor(&cbor).unwrap();

    assert_eq!(
        doc.get("name").and_then(|v| v.as_str()),
        decoded.get("name").and_then(|v| v.as_str())
    );
    assert_eq!(
        doc.get("age").and_then(|v| v.as_int()),
        decoded.get("age").and_then(|v| v.as_int())
    );
    assert_eq!(
        doc.get("active").and_then(|v| v.as_bool()),
        decoded.get("active").and_then(|v| v.as_bool())
    );
    assert_eq!(
        doc.get("score").and_then(|v| v.as_float64()),
        decoded.get("score").and_then(|v| v.as_float64())
    );

    // Decoded document should not be dirty (loaded from storage)
    assert!(!decoded.is_dirty());
}

#[test]
fn test_cbor_roundtrip_empty_doc() {
    let doc = Document::new();
    let cbor = doc.to_cbor().unwrap();
    let decoded = Document::from_cbor(&cbor).unwrap();

    assert!(decoded.is_empty());
    assert!(!decoded.is_dirty());
}

#[test]
fn test_cbor_roundtrip_arrays() {
    let mut doc = Document::new();
    doc.set("ints", NormalValue::IntArray(vec![1, 2, 3]));
    doc.set(
        "strings",
        NormalValue::StringArray(vec!["a".into(), "b".into()]),
    );
    doc.set("bools", NormalValue::BoolArray(vec![true, false]));

    let cbor = doc.to_cbor().unwrap();
    let decoded = Document::from_cbor(&cbor).unwrap();

    assert!(decoded.get("ints").unwrap().is_array());
    assert!(decoded.get("strings").unwrap().is_array());
    assert!(decoded.get("bools").unwrap().is_array());
}

#[test]
fn test_from_cbor_invalid_bytes() {
    let result = Document::from_cbor(&[0xff, 0xfe, 0xfd]);
    assert!(result.is_err());
}

#[test]
fn test_from_cbor_not_a_map() {
    // CBOR integer instead of map
    let result = Document::from_cbor(&[0x18, 0x2a]); // Integer 42
    assert!(result.is_err());
}

#[test]
fn test_cbor_roundtrip_go_simple_doc() {
    // Decode Go's CBOR and verify we can read it
    let decoded = Document::from_cbor(&[
        0xa2, 0x63, 0x41, 0x67, 0x65, 0x18, 0x1a, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x64, 0x4a, 0x6f,
        0x68, 0x6e,
    ])
    .unwrap();

    assert_eq!(decoded.get("Name").and_then(|v| v.as_str()), Some("John"));
    assert_eq!(decoded.get("Age").and_then(|v| v.as_int()), Some(26));
}

#[test]
fn test_array_fields() {
    let json = r#"{"tags": ["a", "b", "c"], "numbers": [1, 2, 3]}"#;
    let doc = Document::from_json_str(json).unwrap();

    let tags = doc.get("tags");
    assert!(tags.is_some());
    assert!(tags.unwrap().is_array());

    let numbers = doc.get("numbers");
    assert!(numbers.is_some());
    assert!(numbers.unwrap().is_array());
}

// ==========================================================================
// Golden tests for Go wire compatibility
// Generated from Go DefraDB using: go test -v -run TestGenerateRustFixtures ./client/
// ==========================================================================

// SDN Namespace UUID for verification (must match Go's SDNNamespaceV0)
const GO_SDN_NAMESPACE: &str = "c94acbfa-dd53-40d0-97f3-29ce16c333fc";

// Test case 1: Simple document {"Name": "John", "Age": 26}
const GO_SIMPLE_DOC_CBOR: &[u8] = &[
    0xa2, 0x63, 0x41, 0x67, 0x65, 0x18, 0x1a, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x64, 0x4a, 0x6f, 0x68,
    0x6e,
];

// Test case 2: String-only document {"Name": "Alice"}
const GO_STRING_DOC_CBOR: &[u8] = &[
    0xa1, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x65, 0x41, 0x6c, 0x69, 0x63, 0x65,
];

// Test case 3: Boolean document {"Active": true}
const GO_BOOL_DOC_CBOR: &[u8] = &[0xa1, 0x66, 0x41, 0x63, 0x74, 0x69, 0x76, 0x65, 0xf5];

// Test case 4: Empty document {}
const GO_EMPTY_DOC_CBOR: &[u8] = &[0xa0];

#[test]
fn test_sdn_namespace_matches_go() {
    assert_eq!(
        SDN_NAMESPACE_V0.to_string(),
        GO_SDN_NAMESPACE,
        "SDN namespace UUID must match Go's SDNNamespaceV0"
    );
}

#[test]
fn test_cbor_matches_go_simple_doc() {
    // Go test case: {"Name": "John", "Age": 26}
    let mut doc = Document::new();
    doc.set("Name", "John");
    doc.set("Age", 26i64);

    let cbor = doc.to_cbor().unwrap();
    assert_eq!(
        cbor, GO_SIMPLE_DOC_CBOR,
        "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
        cbor, GO_SIMPLE_DOC_CBOR
    );
}

#[test]
fn test_cbor_matches_go_string_doc() {
    // Go test case: {"Name": "Alice"}
    let mut doc = Document::new();
    doc.set("Name", "Alice");

    let cbor = doc.to_cbor().unwrap();
    assert_eq!(
        cbor, GO_STRING_DOC_CBOR,
        "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
        cbor, GO_STRING_DOC_CBOR
    );
}

#[test]
fn test_cbor_matches_go_bool_doc() {
    // Go test case: {"Active": true}
    let mut doc = Document::new();
    doc.set("Active", true);

    let cbor = doc.to_cbor().unwrap();
    assert_eq!(
        cbor, GO_BOOL_DOC_CBOR,
        "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
        cbor, GO_BOOL_DOC_CBOR
    );
}

#[test]
fn test_cbor_matches_go_empty_doc() {
    // Go test case: {}
    let doc = Document::new();

    let cbor = doc.to_cbor().unwrap();
    assert_eq!(
        cbor, GO_EMPTY_DOC_CBOR,
        "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
        cbor, GO_EMPTY_DOC_CBOR
    );
}

#[test]
fn test_canonical_cbor_key_ordering() {
    // Verify canonical CBOR ordering: shorter keys first, then lexicographic
    // Keys: "z" (1 char), "aa" (2 chars), "ab" (2 chars)
    let mut doc = Document::new();
    doc.set("ab", 3i64);
    doc.set("z", 1i64);
    doc.set("aa", 2i64);

    let cbor = doc.to_cbor().unwrap();

    // Expected order: z (1 char), aa (2 chars), ab (2 chars)
    // a3 = map with 3 entries
    // 61 7a = "z"
    // 01 = 1
    // 62 61 61 = "aa"
    // 02 = 2
    // 62 61 62 = "ab"
    // 03 = 3
    let expected = &[
        0xa3, 0x61, 0x7a, 0x01, 0x62, 0x61, 0x61, 0x02, 0x62, 0x61, 0x62, 0x03,
    ];
    assert_eq!(
        cbor, expected,
        "Keys should be sorted by length first, then lexicographically.\nGot: {:02x?}\nExpected: {:02x?}",
        cbor, expected
    );
}

#[test]
fn test_doc_id_string_format() {
    // DocID string should start with "bae-" (base32 encoded version 0x01)
    let genesis_cid: cid::Cid = "bafyreie7rtdexuf47f633477mfieshkeh5rwnjeommkgqrzl22n6g4bfmm"
        .parse()
        .unwrap();
    let doc = Document::with_id(document::DocID::new_v0(genesis_cid));

    let doc_id_str = doc.id().unwrap().to_string();
    assert!(
        doc_id_str.starts_with("bae-"),
        "DocID should start with 'bae-' prefix, got: {}",
        doc_id_str
    );

    // Should contain a valid UUID after the prefix
    let uuid_part = &doc_id_str[4..]; // Skip "bae-"
    assert!(
        uuid::Uuid::parse_str(uuid_part).is_ok(),
        "DocID should contain valid UUID after prefix, got: {}",
        uuid_part
    );
}

#[test]
fn test_cbor_encoding_matches_go_nested_json() {
    // Go's JSON type encodes numbers as float64, which CBOR canonical
    // encoding with ShortestFloat16 encodes as float16 when possible.
    //
    // Go canonical CBOR output for:
    // {"name": "John", "custom": {"tree": "maple", "age": 250}}
    // where "custom" is stored as NormalValue::Json (Go's JSON type)
    //
    // Expected bytes (37 bytes):
    // a2646e616d65644a6f686e66637573746f6da263616765f95bd06474726565656d61706c65
    //
    // Breakdown:
    // a2                 - map(2)
    // 64 6e616d65        - text(4) "name"
    // 64 4a6f686e        - text(4) "John"
    // 66 637573746f6d    - text(6) "custom"
    // a2                 - map(2)
    // 63 616765          - text(3) "age"
    // f9 5bd0            - float16(250.0)
    // 64 74726565        - text(4) "tree"
    // 65 6d61706c65      - text(5) "maple"
    let expected_hex = "a2646e616d65644a6f686e66637573746f6da263616765f95bd06474726565656d61706c65";
    let expected_bytes: Vec<u8> = (0..expected_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&expected_hex[i..i + 2], 16).unwrap())
        .collect();

    // Create the same document in Rust
    let json = r#"{"name": "John", "custom": {"tree": "maple", "age": 250}}"#;
    let doc = Document::from_json_str(json).expect("should parse");

    let cbor_bytes = doc.to_cbor().expect("should encode");

    assert_eq!(
        cbor_bytes, expected_bytes,
        "CBOR encoding should match Go's JSON-type canonical encoding"
    );
}

#[test]
fn get_mut_does_not_dirty_on_field_miss() {
    // Regression for #812: Document::get_mut used to unconditionally set
    // is_dirty = true before checking whether the field existed. A caller
    // that probes for an optional field with get_mut and gets None would
    // trigger unnecessary saves + CRDT delta generation without ever
    // modifying the document. Match Go's document.go, which only sets
    // the dirty flag inside doc.set() after a value is written.
    let json = r#"{"name": "Alice"}"#;
    let mut doc = Document::from_json_str(json).expect("should parse");
    doc.clean(); // start from a fully-clean state

    // Probe a field that doesn't exist.
    let missing = doc.get_mut("does_not_exist");
    assert!(missing.is_none(), "missing field must return None");
    assert!(
        !doc.is_dirty(),
        "probing a missing field must not mark the document dirty"
    );

    // Probe a field that exists — now it should mark dirty.
    let present = doc.get_mut("name");
    assert!(present.is_some(), "existing field must return Some");
    assert!(
        doc.is_dirty(),
        "mutating an existing field via get_mut must mark the document dirty"
    );
}
