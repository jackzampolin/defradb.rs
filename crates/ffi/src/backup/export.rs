use std::collections::HashMap;
use std::ffi::c_char;
use std::fs;

use serde_json::{Map, Value as JsonValue};

use schema::CollectionVersion;

use crate::helpers::{get_rt, require_c_str};
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{ffi_async_ok, try_ffi, ERR_INVALID_NODE_HANDLE};

use super::{classify_schema_fields, compute_doc_id_new, BackupConfig};

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
        // Get all collection names
        let all_names = database
            .list_collections()
            .map_err(|e| format!("failed to list collections: {}", e))?;

        // Filter to requested collections (or all)
        let filtered_names: Vec<String> = if config.collections.is_empty() {
            all_names
        } else {
            for name in &config.collections {
                if !all_names.contains(name) {
                    return Err(format!(
                        "failed to get collection: key not found. Name: {}",
                        name
                    ));
                }
            }
            config.collections.clone()
        };

        // Sort collections by collection_id (CID) to match Go's ordering.
        // Go's getCollections iterates by storage key which orders by collection_id.
        let mut name_cid_pairs: Vec<(String, String)> = Vec::new();
        for name in &filtered_names {
            let col = database
                .get_collection(name)
                .map_err(|e| format!("failed to get collection '{}': {}", name, e))?
                .ok_or_else(|| {
                    format!("failed to get collection: key not found. Name: {}", name)
                })?;
            name_cid_pairs.push((name.clone(), col.schema().collection_id.clone()));
        }
        name_cid_pairs.sort_by(|a, b| a.1.cmp(&b.1));
        let collection_names: Vec<String> = name_cid_pairs.into_iter().map(|(n, _)| n).collect();

        // Three-phase export:
        // Phase 1: Query all docs, compute initial _docIDNew (including FK fields)
        // Phase 2: Remap FK values to _docIDNew and recompute _docIDNew
        // Phase 3: Build export output

        struct DocEntry {
            doc_map: Map<String, JsonValue>,
            own_doc_id: String,
            self_ref_excludes: Vec<String>,
        }

        struct CollectionData {
            name: String,
            schema: CollectionVersion,
            docs: Vec<DocEntry>,
            fk_field_names: Vec<String>,
        }

        let mut all_collections: Vec<CollectionData> = Vec::new();
        let mut doc_id_map: HashMap<String, String> = HashMap::new();

        // Phase 1: Query all docs and compute initial _docIDNew
        for name in &collection_names {
            let collection = database
                .get_collection(name)
                .map_err(|e| format!("failed to get collection '{}': {}", name, e))?
                .ok_or_else(|| {
                    format!("failed to get collection: key not found. Name: {}", name)
                })?;

            let schema = collection.schema().clone();
            let fields = classify_schema_fields(&schema);

            // Build GraphQL query field list
            let mut query_parts = vec!["_docID".to_string()];
            let mut relation_field_names: Vec<String> = Vec::new();
            let mut fk_field_names: Vec<String> = Vec::new();
            let mut self_ref_candidate_fks: Vec<String> = Vec::new();

            for field in &fields {
                if field.is_relation {
                    // Only include primary (non-secondary) relation fields.
                    // Secondary relation fields don't store FK values.
                    if !field.is_array && field.is_primary {
                        query_parts.push(format!("{} {{ _docID }}", field.name));
                        relation_field_names.push(field.name.clone());
                        let fk_name = format!("_{}ID", field.name);
                        fk_field_names.push(fk_name.clone());
                        if field.is_self_ref {
                            self_ref_candidate_fks.push(fk_name);
                        }
                    }
                } else {
                    query_parts.push(field.name.clone());
                }
            }

            let query = format!("{{ {} {{ {} }} }}", name, query_parts.join(" "));

            let request = query::QueryRequest::new(query);
            let response = runner.execute(request).await;

            if !response.errors.is_empty() {
                let errs: Vec<String> = response.errors.iter().map(|e| e.message.clone()).collect();
                return Err(format!("query errors for '{}': {}", name, errs.join("; ")));
            }

            let response_json = serde_json::to_value(&response.data)
                .map_err(|e| format!("failed to serialize response: {}", e))?;

            let docs = response_json
                .get(name.as_str())
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let mut doc_entries = Vec::new();
            for doc in docs {
                let mut doc_map = match doc.as_object() {
                    Some(m) => m.clone(),
                    None => continue,
                };

                // Transform relation fields: {author: {_docID: "..."}} → {_authorID: "..."}
                for rel_name in &relation_field_names {
                    if let Some(related) = doc_map.remove(rel_name) {
                        if related.is_null() {
                            continue;
                        }
                        if let Some(related_id) = related.get("_docID") {
                            let fk_name = format!("_{}ID", rel_name);
                            doc_map.insert(fk_name, related_id.clone());
                        }
                    }
                }

                let own_doc_id = doc_map
                    .get("_docID")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                // Detect document-level self-references (doc referencing itself)
                // Go only excludes FK fields where the value equals the doc's own _docID
                let mut self_ref_excludes: Vec<String> = Vec::new();
                for fk_name in &self_ref_candidate_fks {
                    if let Some(fk_value) = doc_map.get(fk_name).and_then(|v| v.as_str()) {
                        if fk_value == own_doc_id {
                            self_ref_excludes.push(fk_name.clone());
                        }
                    }
                }

                // Compute initial _docIDNew (including FK fields, excluding self-refs)
                let doc_id_new = compute_doc_id_new(&doc_map, &self_ref_excludes, &schema)?;

                doc_id_map.insert(own_doc_id.clone(), doc_id_new.clone());
                doc_map.insert("_docIDNew".to_string(), JsonValue::String(doc_id_new));

                // Strip null fields (Go omits them in export)
                doc_map.retain(|_, v| !v.is_null());

                doc_entries.push(DocEntry {
                    doc_map,
                    own_doc_id,
                    self_ref_excludes,
                });
            }

            all_collections.push(CollectionData {
                name: name.clone(),
                schema,
                docs: doc_entries,
                fk_field_names,
            });
        }

        // Phase 2: Remap FK values to _docIDNew and recompute _docIDNew
        // This handles cross-collection references where the referenced doc's
        // _docIDNew differs from its _docID (because it was updated).
        for col_data in &mut all_collections {
            if col_data.fk_field_names.is_empty() {
                continue;
            }

            for entry in &mut col_data.docs {
                let mut needs_recompute = false;

                for fk_name in &col_data.fk_field_names {
                    if let Some(fk_value) = entry
                        .doc_map
                        .get(fk_name)
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                    {
                        if let Some(new_id) = doc_id_map.get(&fk_value) {
                            if new_id != &fk_value {
                                entry
                                    .doc_map
                                    .insert(fk_name.clone(), JsonValue::String(new_id.clone()));
                                needs_recompute = true;
                            }
                        }
                    }
                }

                if needs_recompute {
                    let doc_id_new = compute_doc_id_new(
                        &entry.doc_map,
                        &entry.self_ref_excludes,
                        &col_data.schema,
                    )?;

                    doc_id_map.insert(entry.own_doc_id.clone(), doc_id_new.clone());
                    entry
                        .doc_map
                        .insert("_docIDNew".to_string(), JsonValue::String(doc_id_new));
                }
            }
        }

        // Phase 3: Build export output
        // Build JSON manually to preserve collection ordering (matching Go).
        // Go builds JSON by iterating collections and writing each one in order,
        // not by marshaling a map (which would sort keys alphabetically).
        let mut collection_json_parts: Vec<String> = Vec::new();
        for col_data in all_collections {
            let export_docs: Vec<JsonValue> = col_data
                .docs
                .into_iter()
                .map(|entry| JsonValue::Object(entry.doc_map))
                .collect();
            let docs_json = serde_json::to_string(&export_docs)
                .map_err(|e| format!("failed to serialize docs: {}", e))?;
            collection_json_parts.push(format!("\"{}\":{}", col_data.name, docs_json));
        }

        let json_output = if config.pretty {
            // For pretty output, re-serialize each collection's docs with indentation
            let mut pretty_parts: Vec<String> = Vec::new();
            for part in &collection_json_parts {
                // Parse and re-serialize with pretty printing
                let val: JsonValue = serde_json::from_str(&format!("{{{}}}", part))
                    .map_err(|e| format!("failed to parse for pretty print: {}", e))?;
                let pretty = serde_json::to_string_pretty(&val)
                    .map_err(|e| format!("failed to pretty print: {}", e))?;
                // Strip outer braces and newlines, keeping inner content
                let inner = pretty.trim().strip_prefix('{').unwrap_or(&pretty);
                let inner = inner.strip_suffix('}').unwrap_or(inner);
                pretty_parts.push(inner.trim_end().to_string());
            }
            format!("{{\n{}\n}}", pretty_parts.join(",\n"))
        } else {
            format!("{{{}}}", collection_json_parts.join(","))
        };

        // Write via temp file for atomic operation
        let temp_path = format!("{}.temp", config.filepath);
        fs::write(&temp_path, &json_output)
            .map_err(|e| format!("failed to create file '{}': {}", temp_path, e))?;
        fs::rename(&temp_path, &config.filepath).map_err(|e| {
            // Clean up temp file on rename failure
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
