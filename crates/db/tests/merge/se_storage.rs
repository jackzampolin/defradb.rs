use db::merge::se::storage::*;

#[test]
fn test_extract_doc_id_from_key() {
    let key = "/se/col1/age/a1b2c3d4/bae123";
    let doc_id = extract_doc_id_from_key(key);
    assert_eq!(doc_id, Some("bae123".to_string()));
}

#[test]
fn test_extract_doc_id_invalid_key() {
    let key = "/se/col1/age";
    let doc_id = extract_doc_id_from_key(key);
    assert_eq!(doc_id, None);
}

#[test]
fn test_field_query_new() {
    let query = FieldQuery::new("age", "age", vec![1, 2, 3]);
    assert_eq!(query.field_name, "age");
    assert_eq!(query.index_id, "age");
    assert_eq!(query.search_tag, vec![1, 2, 3]);
}
