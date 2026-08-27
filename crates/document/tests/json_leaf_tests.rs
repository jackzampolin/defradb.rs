//! Integration tests for JSON leaf values

use document::{JsonLeafValue, JsonPath, JsonScalarValue};
use serde_json::json;

#[test]
fn test_scalar_from_null() {
    let value = json!(null);
    let scalar = JsonScalarValue::from_json_value(&value);
    assert_eq!(scalar, Some(JsonScalarValue::Null));
}

#[test]
fn test_scalar_from_bool() {
    assert_eq!(
        JsonScalarValue::from_json_value(&json!(true)),
        Some(JsonScalarValue::Bool(true))
    );
    assert_eq!(
        JsonScalarValue::from_json_value(&json!(false)),
        Some(JsonScalarValue::Bool(false))
    );
}

#[test]
fn test_scalar_from_number() {
    assert_eq!(
        JsonScalarValue::from_json_value(&json!(42)),
        Some(JsonScalarValue::Number(42.0))
    );
    assert_eq!(
        JsonScalarValue::from_json_value(&json!(3.15)),
        Some(JsonScalarValue::Number(3.15))
    );
    assert_eq!(
        JsonScalarValue::from_json_value(&json!(-100)),
        Some(JsonScalarValue::Number(-100.0))
    );
}

#[test]
fn test_scalar_from_string() {
    assert_eq!(
        JsonScalarValue::from_json_value(&json!("hello")),
        Some(JsonScalarValue::String("hello".to_string()))
    );
    assert_eq!(
        JsonScalarValue::from_json_value(&json!("")),
        Some(JsonScalarValue::String(String::new()))
    );
}

#[test]
fn test_scalar_from_object_returns_none() {
    let value = json!({"key": "value"});
    assert_eq!(JsonScalarValue::from_json_value(&value), None);
}

#[test]
fn test_scalar_from_array_returns_none() {
    let value = json!([1, 2, 3]);
    assert_eq!(JsonScalarValue::from_json_value(&value), None);
}

#[test]
fn test_json_leaf_from_json() {
    let path = JsonPath::new()
        .append_property("custom")
        .append_property("height");
    let value = json!(168);

    let leaf = JsonLeafValue::from_json(path.clone(), &value);
    assert!(leaf.is_some());

    let leaf = leaf.unwrap();
    assert_eq!(leaf.path, path);
    assert_eq!(leaf.value, JsonScalarValue::Number(168.0));
}

#[test]
fn test_json_leaf_from_object_returns_none() {
    let path = JsonPath::new().append_property("data");
    let value = json!({"nested": true});

    let leaf = JsonLeafValue::from_json(path, &value);
    assert!(leaf.is_none());
}
