use db::merge::se::generate_doc_artifacts;
use document::NormalValue;
use schema::CollectionVersion;
use schema::EncryptedIndexDescription;
use std::collections::HashMap;

fn test_schema(encrypted_fields: Vec<&str>) -> CollectionVersion {
    let mut col = CollectionVersion::new("test", "col_v1", "col_v1", vec![]);
    col.encrypted_indexes = encrypted_fields
        .into_iter()
        .map(EncryptedIndexDescription::new)
        .collect();
    col
}

#[test]
fn test_no_encrypted_indexes_generates_nothing() {
    let schema = test_schema(vec![]);
    let fields = HashMap::new();
    let artifacts = generate_doc_artifacts(
        &schema.collection_id,
        "doc1",
        &schema.encrypted_indexes,
        &[],
        &fields,
        None,
        &[0u8; 32],
    )
    .unwrap();
    assert!(artifacts.is_empty());
}

#[test]
fn test_no_matching_values_generates_nothing() {
    let schema = test_schema(vec!["age"]);
    let fields = HashMap::new(); // no "age" field value
    let artifacts = generate_doc_artifacts(
        &schema.collection_id,
        "doc1",
        &schema.encrypted_indexes,
        &[],
        &fields,
        None,
        &[0u8; 32],
    )
    .unwrap();
    assert!(artifacts.is_empty());
}

#[test]
fn test_matching_encrypted_field_generates_artifact() {
    let schema = test_schema(vec!["age"]);
    let mut fields = HashMap::new();
    fields.insert("age".to_string(), NormalValue::Int(25));
    let artifacts = generate_doc_artifacts(
        &schema.collection_id,
        "doc1",
        &schema.encrypted_indexes,
        &[],
        &fields,
        None,
        &[1u8; 32],
    )
    .unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].index_id, "age");
    assert_eq!(artifacts[0].doc_id, "doc1");
    assert_eq!(artifacts[0].search_tag.len(), 16);
}

#[test]
fn test_multiple_encrypted_fields() {
    let schema = test_schema(vec!["age", "city"]);
    let mut fields = HashMap::new();
    fields.insert("age".to_string(), NormalValue::Int(30));
    fields.insert("city".to_string(), NormalValue::String("NYC".to_string()));
    let artifacts = generate_doc_artifacts(
        &schema.collection_id,
        "doc2",
        &schema.encrypted_indexes,
        &[],
        &fields,
        None,
        &[2u8; 32],
    )
    .unwrap();
    assert_eq!(artifacts.len(), 2);
}
