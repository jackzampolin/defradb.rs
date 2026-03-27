//! JSON Patch (RFC 6902) utilities.
//!
//! This module provides pure JSON Pointer operations for navigating and
//! manipulating JSON values. These utilities are used by the schema patching
//! system but have no dependencies on database types.

use serde_json::Value;

/// Error type for JSON Patch operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum JsonPatchError {
    #[error("invalid patch: {0}")]
    InvalidPath(String),

    #[error("path not found: {0}")]
    PathNotFound(String),

    #[error("cannot navigate path: {0}")]
    CannotNavigate(String),
}

pub type Result<T> = std::result::Result<T, JsonPatchError>;

/// Set a value at a JSON Pointer path (RFC 6901).
///
/// Navigates to the specified path and sets the value. Supports both object
/// properties and array indices. Use "-" as an array index to append.
///
/// When `insert` is true, array operations INSERT at the index (shifting elements right),
/// matching RFC 6902 `add`/`copy`/`move` semantics. When false, replaces the element.
///
/// # Arguments
/// * `json` - The JSON value to modify
/// * `path` - JSON Pointer path (e.g., "/foo/bar/0")
/// * `value` - The value to set
///
/// # Errors
/// Returns an error if the path is invalid or cannot be navigated.
pub fn json_pointer_set(json: &mut Value, path: &str, value: Value) -> Result<()> {
    json_pointer_set_impl(json, path, value, true)
}

/// Like `json_pointer_set` but replaces array elements instead of inserting.
pub fn json_pointer_replace(json: &mut Value, path: &str, value: Value) -> Result<()> {
    json_pointer_set_impl(json, path, value, false)
}

fn json_pointer_set_impl(json: &mut Value, path: &str, value: Value, insert: bool) -> Result<()> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Err(JsonPatchError::InvalidPath("empty path".to_string()));
    }

    let mut current = json;
    for (i, segment) in segments.iter().enumerate() {
        if i == segments.len() - 1 {
            // Last segment - set the value
            match current {
                Value::Object(map) => {
                    map.insert(segment.to_string(), value);
                    return Ok(());
                }
                Value::Array(arr) => {
                    // JSON Pointer uses "-" to mean "append to end of array"
                    if *segment == "-" {
                        arr.push(value);
                    } else {
                        let idx: usize = segment.parse().map_err(|_| {
                            JsonPatchError::InvalidPath(format!("invalid array index: {}", segment))
                        })?;
                        if idx >= arr.len() {
                            arr.push(value);
                        } else if insert {
                            // RFC 6902 add/copy/move: INSERT at index, shifting right
                            arr.insert(idx, value);
                        } else {
                            // RFC 6902 replace: REPLACE at index
                            arr[idx] = value;
                        }
                    }
                    return Ok(());
                }
                _ => {
                    return Err(JsonPatchError::CannotNavigate(format!(
                        "cannot set value at path {}",
                        path
                    )));
                }
            }
        } else {
            // Navigate to the next level
            match current {
                Value::Object(map) => {
                    current = map
                        .get_mut(*segment)
                        .ok_or_else(|| JsonPatchError::PathNotFound(path.to_string()))?;
                }
                Value::Array(arr) => {
                    let idx: usize = segment.parse().map_err(|_| {
                        JsonPatchError::InvalidPath(format!("invalid array index: {}", segment))
                    })?;
                    current = arr
                        .get_mut(idx)
                        .ok_or_else(|| JsonPatchError::PathNotFound(path.to_string()))?;
                }
                _ => {
                    return Err(JsonPatchError::CannotNavigate(format!(
                        "cannot navigate path: {}",
                        path
                    )));
                }
            }
        }
    }

    Err(JsonPatchError::InvalidPath(
        "failed to set value".to_string(),
    ))
}

/// Remove a value at a JSON Pointer path (RFC 6901).
///
/// Navigates to the specified path and removes the value.
///
/// # Arguments
/// * `json` - The JSON value to modify
/// * `path` - JSON Pointer path (e.g., "/foo/bar/0")
///
/// # Errors
/// Returns an error if the path is invalid or cannot be navigated.
pub fn json_pointer_remove(json: &mut Value, path: &str) -> Result<()> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Err(JsonPatchError::InvalidPath("empty path".to_string()));
    }

    let mut current = json;
    for (i, segment) in segments.iter().enumerate() {
        if i == segments.len() - 1 {
            // Last segment - remove the value
            match current {
                Value::Object(map) => {
                    map.remove(*segment);
                    return Ok(());
                }
                Value::Array(arr) => {
                    let idx: usize = segment.parse().map_err(|_| {
                        JsonPatchError::InvalidPath(format!("invalid array index: {}", segment))
                    })?;
                    if idx < arr.len() {
                        arr.remove(idx);
                    }
                    return Ok(());
                }
                _ => {
                    return Err(JsonPatchError::CannotNavigate(format!(
                        "cannot remove value at path {}",
                        path
                    )));
                }
            }
        } else {
            // Navigate to the next level
            match current {
                Value::Object(map) => {
                    current = map
                        .get_mut(*segment)
                        .ok_or_else(|| JsonPatchError::PathNotFound(path.to_string()))?;
                }
                Value::Array(arr) => {
                    let idx: usize = segment.parse().map_err(|_| {
                        JsonPatchError::InvalidPath(format!("invalid array index: {}", segment))
                    })?;
                    current = arr
                        .get_mut(idx)
                        .ok_or_else(|| JsonPatchError::PathNotFound(path.to_string()))?;
                }
                _ => {
                    return Err(JsonPatchError::CannotNavigate(format!(
                        "cannot navigate path: {}",
                        path
                    )));
                }
            }
        }
    }

    Err(JsonPatchError::InvalidPath(
        "failed to remove value".to_string(),
    ))
}

/// Get a value at a JSON Pointer path (RFC 6901).
///
/// Navigates to the specified path and returns a clone of the value.
///
/// # Arguments
/// * `json` - The JSON value to query
/// * `path` - JSON Pointer path (e.g., "/foo/bar/0")
///
/// # Returns
/// The value at the path, or None if the path doesn't exist.
pub fn json_pointer_get(json: &Value, path: &str) -> Option<Value> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return None;
    }

    let mut current = json;
    for segment in segments.iter() {
        match current {
            Value::Object(map) => {
                current = map.get(*segment)?;
            }
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

/// Extract a field name from a path like `/Fields/email` or `/Fields/email/Name`.
///
/// Returns None if the segment after /Fields/ is numeric, "-", or /Fields/ isn't present.
pub fn extract_field_name_from_path(path: &str) -> Option<String> {
    let segments: Vec<&str> = path.split('/').collect();
    for (i, seg) in segments.iter().enumerate() {
        if *seg == "Fields" && i + 1 < segments.len() {
            let next = segments[i + 1];
            if next.parse::<usize>().is_ok() || next == "-" {
                return None;
            }
            return Some(next.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
