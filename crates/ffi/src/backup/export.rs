use std::ffi::c_char;
use std::fs;

use crate::helpers::{get_rt, require_c_str};
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{ffi_async_ok, try_ffi, ERR_INVALID_NODE_HANDLE};

use super::BackupConfig;

/// Export the database to a JSON file.
///
/// The config_json parameter is a JSON string matching Go's BackupConfig:
/// ```json
/// {
///     "filepath": "/path/to/backup.json",
///     "pretty": false,
///     "collections": ["User", "Address"]
/// }
/// ```
///
/// If collections is empty, all collections are exported.
///
/// # Safety
///
/// `config_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn basic_export(node_ptr: usize, config_json: *const c_char) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let config_str = try_ffi!(require_c_str(config_json, "config_json"));

    let config: BackupConfig = match serde_json::from_str(&config_str) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(format!("failed to parse backup config: {}", e)),
    };

    let (database, runner) = match NODES.get(node_ptr, |state| {
        (state.database.clone(), state.query_runner.clone())
    }) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    ffi_async_ok!(rt, {
        let json_output =
            db::backup::export_database(&database, &runner, &config.collections, config.pretty)
                .await?;

        // Write via temp file for atomic operation
        let temp_path = format!("{}.temp", config.filepath);
        fs::write(&temp_path, &json_output)
            .map_err(|e| format!("failed to create file '{}': {}", temp_path, e))?;
        fs::rename(&temp_path, &config.filepath).map_err(|e| {
            let _ = fs::remove_file(&temp_path);
            format!("failed to rename temp file: {}", e)
        })?;

        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::query::exec_request;
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use serde_json::Value as JsonValue;
    use std::ffi::CString;
    use std::ptr;

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

    fn create_user(node: usize, name: &str, age: i32) {
        let mutation = CString::new(format!(
            r#"mutation {{ create_User(input: {{name: "{}", age: {}}}) {{ _docID }} }}"#,
            name, age
        ))
        .unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                mutation.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "create failed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }
    }

    #[test]
    fn test_basic_export_and_import() {
        let node = setup_node_with_schema();

        // Create documents
        create_user(node, "Alice", 30);
        create_user(node, "Bob", 25);

        // Export
        let dir = std::env::temp_dir();
        let export_path = dir.join("defra_test_export.json");
        let config = format!(
            r#"{{"filepath": "{}", "pretty": false}}"#,
            export_path.display()
        );
        let config_c = CString::new(config).unwrap();
        let result = unsafe { basic_export(node, config_c.as_ptr()) };
        assert_eq!(result.status, 0, "export failed");

        // Verify export file
        let content = fs::read_to_string(&export_path).unwrap();
        let parsed: JsonValue = serde_json::from_str(&content).unwrap();
        let users = parsed["User"].as_array().unwrap();
        assert_eq!(users.len(), 2);

        // Each doc should have _docID and _docIDNew
        for doc in users {
            assert!(doc.get("_docID").is_some());
            assert!(doc.get("_docIDNew").is_some());
            assert!(doc.get("name").is_some());
            assert!(doc.get("age").is_some());
        }

        // Clean up export file
        let _ = fs::remove_file(&export_path);

        // Import into a fresh node
        let node2 = setup_node_with_schema();

        // Write import file
        let import_path = dir.join("defra_test_import.json");
        fs::write(&import_path, &content).unwrap();

        let path_c = CString::new(import_path.to_str().unwrap()).unwrap();
        let result = unsafe { crate::backup::basic_import(node2, path_c.as_ptr()) };
        assert_eq!(result.status, 0, "import failed");

        // Verify imported documents
        let query_str = CString::new("{ User { name age } }").unwrap();
        let result = unsafe {
            exec_request(
                node2,
                ptr::null(),
                query_str.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "query failed");
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "should contain Alice");
        assert!(value.contains("Bob"), "should contain Bob");
        unsafe { crate::types::defra_free_string(result.value) };

        // Clean up
        let _ = fs::remove_file(&import_path);
        node_close(node);
        node_close(node2);
    }

    #[test]
    fn test_basic_export_single_collection() {
        assert!(crate::runtime::init_runtime());
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add two schemas
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let sdl2 = CString::new("type Address { city: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl2.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Create documents in both
        let mutation =
            CString::new(r#"mutation { create_User(input: {name: "Alice"}) { _docID } }"#).unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                mutation.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let mutation =
            CString::new(r#"mutation { create_Address(input: {city: "NYC"}) { _docID } }"#)
                .unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                mutation.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Export only Address
        let dir = std::env::temp_dir();
        let export_path = dir.join("defra_test_export_single.json");
        let config = format!(
            r#"{{"filepath": "{}", "collections": ["Address"]}}"#,
            export_path.display()
        );
        let config_c = CString::new(config).unwrap();
        let result = unsafe { basic_export(node, config_c.as_ptr()) };
        assert_eq!(result.status, 0, "export failed");

        let content = fs::read_to_string(&export_path).unwrap();
        let parsed: JsonValue = serde_json::from_str(&content).unwrap();
        assert!(parsed.get("Address").is_some(), "should have Address");
        assert!(parsed.get("User").is_none(), "should not have User");

        let _ = fs::remove_file(&export_path);
        node_close(node);
    }
}
