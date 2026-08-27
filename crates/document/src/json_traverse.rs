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
