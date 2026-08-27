//! Integration tests for JsonPath

use document::{JsonPath, JsonPathPart};

#[test]
fn test_json_path_new() {
    let path = JsonPath::new();
    assert!(path.is_empty());
    assert_eq!(path.len(), 0);
}

#[test]
fn test_json_path_append_property() {
    let path = JsonPath::new();
    let path = path.append_property("custom");
    let path = path.append_property("height");

    assert_eq!(path.len(), 2);
    assert_eq!(
        path.0,
        vec![
            JsonPathPart::Property("custom".to_string()),
            JsonPathPart::Property("height".to_string()),
        ]
    );
}

#[test]
fn test_json_path_append_index() {
    let path = JsonPath::new();
    let path = path.append_property("tags");
    let path = path.append_index();

    assert_eq!(path.len(), 2);
    assert_eq!(
        path.0,
        vec![
            JsonPathPart::Property("tags".to_string()),
            JsonPathPart::Index,
        ]
    );
}

#[test]
fn test_json_path_immutable() {
    let path1 = JsonPath::new().append_property("a");
    let path2 = path1.append_property("b");

    // path1 should be unchanged
    assert_eq!(path1.len(), 1);
    assert_eq!(path2.len(), 2);
}
