use std::ffi::c_char;
use std::fs;

use serde_json::Value as JsonValue;

use crate::document::json_to_graphql_input;
use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

use super::classify_schema_fields;

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
    let rt = get_runtime!(FfiResult);

    let path_str = match c_str_to_string(filepath) {
        Some(s) => s,
        None => return FfiResult::error("filepath is null"),
    };

    let (database, runner) = match NODES.get(node_ptr, |state| {
        (state.database.clone(), state.query_runner.clone())
    }) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Read file
        let content = fs::read_to_string(&path_str).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                format!("failed to open file '{}': {}", path_str, e)
            } else {
                format!("failed to read file '{}': {}", path_str, e)
            }
        })?;

        // Parse JSON
        let parsed: JsonValue = serde_json::from_str(&content)
            .map_err(|e| format!("failed to parse JSON: {}", e))?;

        // Must be an object at root level
        let root = match parsed.as_object() {
            Some(obj) => obj,
            None => {
                return Err(
                    "invalid JSON: expected JSON object at root, got array or primitive"
                        .to_string(),
                )
            }
        };

        // Process each collection
        for (collection_name, docs_value) in root {
            // Get collection and schema
            let collection = database
                .get_collection(collection_name)
                .map_err(|e| format!("failed to get collection: {}", e))?
                .ok_or_else(|| {
                    format!(
                        "failed to get collection: key not found. Name: {}",
                        collection_name
                    )
                })?;

            let schema = collection.schema();
            let fields = classify_schema_fields(schema);

            // Identify self-referencing FK field names (_<fieldName>ID)
            let self_ref_fk_names: Vec<String> = fields
                .iter()
                .filter(|f| f.is_self_ref && !f.is_array)
                .map(|f| format!("_{}ID", f.name))
                .collect();

            // Build map of relation field name → FK field name for non-array relations.
            // Import data may use relation names (e.g., "author": "bae-...")
            // which need to be converted to FK names (e.g., "_authorID": "bae-...").
            // Go's NewDocFromMap handles this internally; we do it explicitly here.
            let relation_to_fk: Vec<(String, String)> = fields
                .iter()
                .filter(|f| f.is_relation && !f.is_array)
                .map(|f| (f.name.clone(), format!("_{}ID", f.name)))
                .collect();

            // Documents must be an array
            let docs = match docs_value.as_array() {
                Some(arr) => arr,
                None => {
                    return Err(format!(
                        "invalid JSON: expected JSON array for collection '{}', got object",
                        collection_name
                    ))
                }
            };

            // Pre-validate all documents' field names before creating any
            let valid_field_names: Vec<&str> =
                schema.fields.iter().map(|f| f.name.as_str()).collect();
            for doc in docs {
                if let Some(doc_obj) = doc.as_object() {
                    for key in doc_obj.keys() {
                        if key == "_docID" || key == "_docIDNew" {
                            continue;
                        }
                        if !valid_field_names.contains(&key.as_str()) {
                            return Err(format!(
                                "failed to create document in '{}': the given field does not exist. Name: {}",
                                collection_name, key
                            ));
                        }
                    }
                }
            }

            // Create each document
            for doc in docs {
                let mut doc_map = match doc.as_object() {
                    Some(m) => m.clone(),
                    None => continue,
                };

                // Strip backup metadata fields
                doc_map.remove("_docID");
                doc_map.remove("_docIDNew");

                // Convert relation field names to FK field names.
                // e.g., "author": "bae-..." → "_authorID": "bae-..."
                for (rel_name, fk_name) in &relation_to_fk {
                    if let Some(value) = doc_map.remove(rel_name) {
                        if !value.is_null() {
                            doc_map.insert(fk_name.clone(), value);
                        }
                    }
                }

                // Extract self-referencing FK fields (strip before create, apply after)
                let mut self_ref_values: Vec<(String, JsonValue)> = Vec::new();
                for fk_name in &self_ref_fk_names {
                    if let Some(value) = doc_map.remove(fk_name) {
                        if !value.is_null() {
                            self_ref_values.push((fk_name.clone(), value));
                        }
                    }
                }

                // Build GraphQL create mutation (without self-ref FK fields)
                let input = json_to_graphql_input(&JsonValue::Object(doc_map));
                let mutation = format!(
                    "mutation {{ create_{}(input: {}) {{ _docID }} }}",
                    collection_name, input
                );

                let request = query::QueryRequest::new(mutation);
                let response = runner.execute(request).await;

                if !response.errors.is_empty() {
                    let errs: Vec<String> =
                        response.errors.iter().map(|e| e.message.clone()).collect();
                    let err_msg = errs.join("; ");
                    // Match Go's error format for duplicate documents
                    if err_msg.contains("already exists") {
                        return Err(
                            "a document with the given ID already exists".to_string()
                        );
                    }
                    return Err(format!(
                        "failed to create document in '{}': {}",
                        collection_name, err_msg
                    ));
                }

                // Apply self-referencing FK fields via update mutation
                if !self_ref_values.is_empty() {
                    // Extract the new document's _docID from create response
                    let response_json = serde_json::to_value(&response.data)
                        .map_err(|e| format!("failed to serialize response: {}", e))?;

                    let create_key = format!("create_{}", collection_name);
                    let new_doc_id = response_json
                        .get(&create_key)
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|v| v.get("_docID"))
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            "failed to get _docID from create response".to_string()
                        })?;

                    // Build update mutation with self-ref FK fields
                    let mut update_parts = Vec::new();
                    for (fk_name, value) in &self_ref_values {
                        let val_str = match value.as_str() {
                            Some(s) => format!("{}: \"{}\"", fk_name, s),
                            None => format!("{}: {}", fk_name, value),
                        };
                        update_parts.push(val_str);
                    }

                    let update_mutation = format!(
                        "mutation {{ update_{}(docID: \"{}\", input: {{{}}}) {{ _docID }} }}",
                        collection_name,
                        new_doc_id,
                        update_parts.join(", ")
                    );

                    let request = query::QueryRequest::new(update_mutation);
                    let response = runner.execute(request).await;

                    if !response.errors.is_empty() {
                        let errs: Vec<String> =
                            response.errors.iter().map(|e| e.message.clone()).collect();
                        return Err(format!(
                            "failed to update self-ref fields in '{}': {}",
                            collection_name,
                            errs.join("; ")
                        ));
                    }
                }
            }
        }

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
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
