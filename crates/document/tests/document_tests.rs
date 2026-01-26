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
    assert!(doc.id().is_some()); // DocID should be generated
}

#[test]
fn test_from_json_with_doc_id() {
    // First create a document to get a valid DocID
    let doc1 = Document::from_json_str(r#"{"name": "Test"}"#).unwrap();
    let doc_id_str = doc1.id().unwrap().to_string();

    // Now create a document with that DocID
    let json = format!(r#"{{"_docID": "{}", "name": "Test2"}}"#, doc_id_str);
    let doc2 = Document::from_json_str(&json).unwrap();

    assert_eq!(doc2.id().unwrap().to_string(), doc_id_str);
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
fn test_generate_doc_id_deterministic() {
    let mut doc1 = Document::new();
    doc1.set("name", "Test");
    doc1.set("value", 123i64);

    let mut doc2 = Document::new();
    doc2.set("name", "Test");
    doc2.set("value", 123i64);

    let id1 = doc1.generate_doc_id().unwrap();
    let id2 = doc2.generate_doc_id().unwrap();

    assert_eq!(id1, id2);
}

#[test]
fn test_different_content_different_id() {
    let mut doc1 = Document::new();
    doc1.set("name", "Alice");

    let mut doc2 = Document::new();
    doc2.set("name", "Bob");

    let id1 = doc1.generate_doc_id().unwrap();
    let id2 = doc2.generate_doc_id().unwrap();

    assert_ne!(id1, id2);
}

#[test]
fn test_doc_id_field_order_independence() {
    // Verify that documents with same content but different field insertion order
    // produce the same DocID (canonical CBOR ordering ensures this)
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

    let id1 = doc1.generate_doc_id().unwrap();
    let id2 = doc2.generate_doc_id().unwrap();
    let id3 = doc3.generate_doc_id().unwrap();

    assert_eq!(
        id1, id2,
        "DocID should be same regardless of field insertion order"
    );
    assert_eq!(
        id2, id3,
        "DocID should be same regardless of field insertion order"
    );

    // Also verify CBOR encoding is identical
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
    let mut doc = Document::new();
    doc.set("test", "value");
    doc.generate_and_set_doc_id().unwrap();

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
