use super::*;
use document::Document;
use serde_json::Value;

#[test]
fn test_doc_to_lens_doc_conversion() {
    let mut doc = Document::new();
    doc.set("name", Value::String("Alice".to_string()));
    doc.set("age", Value::Number(30.into()));

    let lens_doc = LensedDocFetcher::<storage::MemoryStore>::doc_to_lens_doc(&doc).unwrap();

    assert_eq!(
        lens_doc.get("name").unwrap(),
        &Value::String("Alice".to_string())
    );
    assert_eq!(lens_doc.get("age").unwrap(), &Value::Number(30.into()));
}
