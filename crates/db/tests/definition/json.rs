use db::definition::patch::json::*;
use serde_json::json;

#[test]
fn test_json_pointer_set_object() {
    let mut json = json!({"foo": {"bar": 1}});
    json_pointer_set(&mut json, "/foo/bar", json!(2)).unwrap();
    assert_eq!(json, json!({"foo": {"bar": 2}}));
}

#[test]
fn test_json_pointer_set_array_insert() {
    let mut json = json!({"arr": [1, 2, 3]});
    json_pointer_set(&mut json, "/arr/1", json!(99)).unwrap();
    assert_eq!(json, json!({"arr": [1, 99, 2, 3]}));
}

#[test]
fn test_json_pointer_replace_array() {
    let mut json = json!({"arr": [1, 2, 3]});
    json_pointer_replace(&mut json, "/arr/1", json!(99)).unwrap();
    assert_eq!(json, json!({"arr": [1, 99, 3]}));
}

#[test]
fn test_json_pointer_set_array_append() {
    let mut json = json!({"arr": [1, 2]});
    json_pointer_set(&mut json, "/arr/-", json!(3)).unwrap();
    assert_eq!(json, json!({"arr": [1, 2, 3]}));
}

#[test]
fn test_json_pointer_remove_object() {
    let mut json = json!({"foo": {"bar": 1, "baz": 2}});
    json_pointer_remove(&mut json, "/foo/bar").unwrap();
    assert_eq!(json, json!({"foo": {"baz": 2}}));
}

#[test]
fn test_json_pointer_remove_array() {
    let mut json = json!({"arr": [1, 2, 3]});
    json_pointer_remove(&mut json, "/arr/1").unwrap();
    assert_eq!(json, json!({"arr": [1, 3]}));
}

#[test]
fn test_json_pointer_get() {
    let json = json!({"foo": {"bar": [1, 2, 3]}});
    assert_eq!(json_pointer_get(&json, "/foo/bar/1"), Some(json!(2)));
    assert_eq!(json_pointer_get(&json, "/foo/bar"), Some(json!([1, 2, 3])));
    assert_eq!(json_pointer_get(&json, "/foo/missing"), None);
}

#[test]
fn test_extract_field_name_from_path() {
    assert_eq!(
        extract_field_name_from_path("/Fields/email"),
        Some("email".to_string())
    );
    assert_eq!(
        extract_field_name_from_path("/Fields/email/Name"),
        Some("email".to_string())
    );
    assert_eq!(extract_field_name_from_path("/Fields/0"), None);
    assert_eq!(extract_field_name_from_path("/Fields/-"), None);
    assert_eq!(extract_field_name_from_path("/Name"), None);
}
