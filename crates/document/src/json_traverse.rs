//! JSON traversal for indexing
//!
//! Provides traversal of JSON values to extract leaf values with their paths,
//! matching Go's TraverseJSON behavior.

use crate::json_path::JsonPath;
use crate::Result;

/// Options for JSON traversal, matching Go's TraverseJSON options.
#[derive(Debug, Clone)]
pub struct TraverseOptions {
    /// Only visit leaf nodes (scalars), skip objects and arrays.
    /// Corresponds to Go's TraverseJSONOnlyLeaves.
    pub only_leaves: bool,
    /// Include array index in the path for array elements.
    /// Corresponds to Go's TraverseJSONWithArrayIndexInPath.
    pub array_index_in_path: bool,
    /// Visit individual array elements.
    /// Corresponds to Go's TraverseJSONVisitArrayElements.
    pub visit_array_elements: bool,
}

impl Default for TraverseOptions {
    fn default() -> Self {
        Self {
            only_leaves: true,
            array_index_in_path: true,
            visit_array_elements: true,
        }
    }
}

/// Options preset for JSON field indexing (matches Go's JSONFieldGenerator).
pub fn index_traverse_options() -> TraverseOptions {
    TraverseOptions {
        only_leaves: true,
        array_index_in_path: true,
        visit_array_elements: true,
    }
}

/// Traverse JSON and call visitor for each node based on options.
///
/// Matches Go's TraverseJSON behavior:
/// - With only_leaves=true, only scalars (null, bool, number, string) are visited
/// - Empty objects `{}` and arrays `[]` produce NO entries
/// - Array elements get Index marker in path when array_index_in_path=true
pub fn traverse_json<F>(
    json: &serde_json::Value,
    mut visitor: F,
    options: &TraverseOptions,
) -> Result<()>
where
    F: FnMut(&JsonPath, &serde_json::Value) -> Result<()>,
{
    traverse_internal(json, &JsonPath::new(), &mut visitor, options)
}

fn traverse_internal<F>(
    value: &serde_json::Value,
    path: &JsonPath,
    visitor: &mut F,
    options: &TraverseOptions,
) -> Result<()>
where
    F: FnMut(&JsonPath, &serde_json::Value) -> Result<()>,
{
    match value {
        // Objects: recurse into properties
        serde_json::Value::Object(map) => {
            if !options.only_leaves {
                visitor(path, value)?;
            }
            for (key, val) in map {
                let child_path = path.append_property(key);
                traverse_internal(val, &child_path, visitor, options)?;
            }
        }

        // Arrays: recurse into elements if visit_array_elements is true
        serde_json::Value::Array(arr) => {
            if !options.only_leaves {
                visitor(path, value)?;
            }
            if options.visit_array_elements {
                for element in arr {
                    let child_path = if options.array_index_in_path {
                        path.append_index()
                    } else {
                        path.clone()
                    };
                    traverse_internal(element, &child_path, visitor, options)?;
                }
            }
        }

        // Leaf values: null, bool, number, string
        _ => {
            visitor(path, value)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_traverse_simple_object() {
        let json = json!({"height": 168, "weight": 70});
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        assert_eq!(results.len(), 2);
        // Results may be in any order due to HashMap iteration
    }

    #[test]
    fn test_traverse_nested_object() {
        let json = json!({"custom": {"height": 168}});
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        let (path, value) = &results[0];
        assert_eq!(path.len(), 2);
        assert_eq!(*value, json!(168));
    }

    #[test]
    fn test_traverse_array() {
        let json = json!({"tags": ["a", "b", "c"]});
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        assert_eq!(results.len(), 3);
        // Each element should have path [Property("tags"), Index]
        for (path, _) in &results {
            assert_eq!(path.len(), 2);
        }
    }

    #[test]
    fn test_traverse_empty_object() {
        let json = json!({});
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        // Empty object produces no entries
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_traverse_empty_array() {
        let json = json!([]);
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        // Empty array produces no entries
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_traverse_null() {
        let json = json!(null);
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].0.is_empty());
        assert!(results[0].1.is_null());
    }

    #[test]
    fn test_traverse_scalar_types() {
        let test_cases = vec![
            json!(true),
            json!(false),
            json!(42),
            json!(3.14),
            json!("hello"),
            json!(null),
        ];

        for json in test_cases {
            let mut results = Vec::new();
            traverse_json(
                &json,
                |path, value| {
                    results.push((path.clone(), value.clone()));
                    Ok(())
                },
                &index_traverse_options(),
            )
            .unwrap();

            assert_eq!(results.len(), 1);
            assert!(results[0].0.is_empty());
        }
    }

    #[test]
    fn test_traverse_nested_array() {
        let json = json!({"data": [[1, 2], [3, 4]]});
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        // Should visit leaf scalars: 1, 2, 3, 4
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_traverse_mixed_array() {
        let json = json!([1, "a", true, null]);
        let mut results = Vec::new();

        traverse_json(
            &json,
            |path, value| {
                results.push((path.clone(), value.clone()));
                Ok(())
            },
            &index_traverse_options(),
        )
        .unwrap();

        assert_eq!(results.len(), 4);
        // Each has Index in path
        for (path, _) in &results {
            assert_eq!(path.len(), 1);
        }
    }
}
