use std::ffi::c_char;
use std::fs;

use crate::ffi_entry;
use crate::helpers::{get_rt, require_c_str};
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{ffi_async_ok, try_ffi, ERR_INVALID_NODE_HANDLE};

/// Import documents from a JSON backup file.
///
/// The file must be a JSON object mapping collection names to arrays of documents:
/// ```json
/// {
///     "User": [{"_docID": "...", "_docIDNew": "...", "name": "John", "age": 30}],
///     "Address": [{"_docID": "...", "_docIDNew": "...", "street": "...", "city": "..."}]
/// }
/// ```
///
/// Self-referencing FK fields are stripped before creation and applied
/// via update afterward, matching Go DefraDB behavior.
///
/// # Safety
///
/// `filepath` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn basic_import(node_ptr: usize, filepath: *const c_char) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let path_str = try_ffi!(require_c_str(filepath, "filepath"));

        let (database, runner) = match NODES.get(node_ptr, |state| {
            (state.database.clone(), state.query_runner.clone())
        }) {
            Some(r) => r,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async_ok!(rt, {
            let content = fs::read_to_string(&path_str).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    format!("failed to open file '{}': {}", path_str, e)
                } else {
                    format!("failed to read file '{}': {}", path_str, e)
                }
            })?;

            db_backup::import_database(&database, &runner, &content)
                .await
                .map(|_| ())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    fn setup_node_with_schema() -> usize {
        assert!(crate::runtime::init_runtime());
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0, "new_node failed");
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String, age: Int }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema failed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        node
    }

    #[test]
    fn test_basic_import_invalid_json_array() {
        let node = setup_node_with_schema();

        let dir = std::env::temp_dir();
        let path = dir.join("defra_test_import_array.json");
        fs::write(&path, "[1, 2, 3]").unwrap();

        let path_c = CString::new(path.to_str().unwrap()).unwrap();
        let result = unsafe { basic_import(node, path_c.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for array root");

        let _ = fs::remove_file(&path);
        node_close(node);
    }

    #[test]
    fn test_basic_import_invalid_filepath() {
        let node = setup_node_with_schema();

        let path_c = CString::new("/nonexistent/path/file.json").unwrap();
        let result = unsafe { basic_import(node, path_c.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for missing file");

        node_close(node);
    }

    #[test]
    fn test_basic_import_invalid_collection() {
        let node = setup_node_with_schema();

        let dir = std::env::temp_dir();
        let path = dir.join("defra_test_import_bad_col.json");
        fs::write(&path, r#"{"NonExistent": [{"field": "value"}]}"#).unwrap();

        let path_c = CString::new(path.to_str().unwrap()).unwrap();
        let result = unsafe { basic_import(node, path_c.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for invalid collection");

        let _ = fs::remove_file(&path);
        node_close(node);
    }
}
