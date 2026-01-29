//! Backup operations for FFI.
//!
//! This module implements BasicExport and BasicImport for database
//! backup/restore via JSON files, matching Go DefraDB behavior.

use std::collections::HashMap;
use std::ffi::c_char;
use std::fs;

use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value as JsonValue};

use db::Document;
use schema::{CollectionVersion, FieldKind};

use crate::document::json_to_graphql_input;
use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Deserialize null or missing JSON values as an empty Vec.
/// Go sends `"collections": null` when the slice is nil.
fn null_to_empty_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// BackupConfig matches Go's client.BackupConfig.
#[derive(Deserialize)]
struct BackupConfig {
    filepath: String,
    #[serde(default)]
    pretty: bool,
    #[serde(default, deserialize_with = "null_to_empty_vec")]
    collections: Vec<String>,
}

/// Classified field information from a collection schema.
struct FieldInfo {
    name: String,
    is_relation: bool,
    is_self_ref: bool,
    is_array: bool,
}

/// Classify fields from the typed schema, detecting scalar, relation,
/// and self-referencing fields.
fn classify_schema_fields(schema: &CollectionVersion) -> Vec<FieldInfo> {
    let collection_id = &schema.collection_id;
    let mut result = Vec::new();

    for field in &schema.fields {
        // Skip internal fields
        if field.name == "_docID" {
            continue;
        }

        match &field.kind {
            FieldKind::Scalar(_) | FieldKind::ScalarArray(_) => {
                // Skip relation backing FK fields (e.g., author_id)
                if field.relation_name.is_none() {
                    result.push(FieldInfo {
                        name: field.name.clone(),
                        is_relation: false,
                        is_self_ref: false,
                        is_array: field.kind.is_array(),
                    });
                }
            }
            FieldKind::SelfRef { is_array, .. } => {
                result.push(FieldInfo {
                    name: field.name.clone(),
                    is_relation: true,
                    is_self_ref: true,
                    is_array: *is_array,
                });
            }
            FieldKind::Relation {
                collection_id: target_id,
                is_array,
            } => {
                // A relation to the same collection is also self-referencing
                let is_self_ref = target_id == collection_id;
                result.push(FieldInfo {
                    name: field.name.clone(),
                    is_relation: true,
                    is_self_ref,
                    is_array: *is_array,
                });
            }
            FieldKind::Named { is_array, .. } => {
                result.push(FieldInfo {
                    name: field.name.clone(),
                    is_relation: true,
                    is_self_ref: false,
                    is_array: *is_array,
                });
            }
        }
    }
    result
}

/// Compute `_docIDNew` from current field values.
///
/// Creates a Document from the field values (minus FK fields),
/// sets the collection, and generates the content-addressed ID.
fn compute_doc_id_new(
    doc_map: &Map<String, JsonValue>,
    fk_names: &[String],
    schema: &CollectionVersion,
) -> Result<String, String> {
    let mut fields: HashMap<String, JsonValue> = HashMap::new();

    for (key, value) in doc_map {
        // Skip metadata fields
        if key == "_docID" || key == "_docIDNew" {
            continue;
        }
        // Skip FK fields (relationship metadata, not document data)
        if fk_names.contains(key) {
            continue;
        }
        // Skip null values (they don't contribute to CID)
        if value.is_null() {
            continue;
        }
        fields.insert(key.clone(), value.clone());
    }

    let mut doc =
        Document::from_map(fields).map_err(|e| format!("failed to create document: {}", e))?;
    doc.set_collection(schema.clone());

    let doc_id = doc
        .generate_doc_id()
        .map_err(|e| format!("failed to generate doc ID: {}", e))?;

    Ok(doc_id.to_string())
}

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
pub unsafe extern "C" fn basic_export(
    node_ptr: usize,
    config_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let config_str = match c_str_to_string(config_json) {
        Some(s) => s,
        None => return FfiResult::error("config_json is null"),
    };

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

    let result = rt.block_on(async {
        // Get all collection names
        let all_names = database
            .list_collections()
            .map_err(|e| format!("failed to list collections: {}", e))?;

        // Filter to requested collections (or all)
        let collection_names: Vec<String> = if config.collections.is_empty() {
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
                    if !field.is_array {
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
                let errs: Vec<String> =
                    response.errors.iter().map(|e| e.message.clone()).collect();
                return Err(format!(
                    "query errors for '{}': {}",
                    name,
                    errs.join("; ")
                ));
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
                let doc_id_new =
                    compute_doc_id_new(&doc_map, &self_ref_excludes, &schema)?;

                doc_id_map.insert(own_doc_id.clone(), doc_id_new.clone());
                doc_map.insert(
                    "_docIDNew".to_string(),
                    JsonValue::String(doc_id_new),
                );

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
                                entry.doc_map.insert(
                                    fk_name.clone(),
                                    JsonValue::String(new_id.clone()),
                                );
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

                    doc_id_map
                        .insert(entry.own_doc_id.clone(), doc_id_new.clone());
                    entry.doc_map.insert(
                        "_docIDNew".to_string(),
                        JsonValue::String(doc_id_new),
                    );
                }
            }
        }

        // Phase 3: Build export output
        let mut export_data = Map::new();
        for col_data in all_collections {
            let export_docs: Vec<JsonValue> = col_data
                .docs
                .into_iter()
                .map(|entry| JsonValue::Object(entry.doc_map))
                .collect();
            export_data.insert(col_data.name, JsonValue::Array(export_docs));
        }

        // Serialize to JSON
        let json_output = if config.pretty {
            serde_json::to_string_pretty(&JsonValue::Object(export_data))
                .map_err(|e| format!("failed to serialize export: {}", e))?
        } else {
            serde_json::to_string(&JsonValue::Object(export_data))
                .map_err(|e| format!("failed to serialize export: {}", e))?
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

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

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
pub unsafe extern "C" fn basic_import(
    node_ptr: usize,
    filepath: *const c_char,
) -> FfiResult {
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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
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
        let result = unsafe { exec_request(node, mutation.as_ptr(), ptr::null(), ptr::null()) };
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
        let result = unsafe { basic_import(node2, path_c.as_ptr()) };
        assert_eq!(result.status, 0, "import failed");

        // Verify imported documents
        let query_str = CString::new("{ User { name age } }").unwrap();
        let result =
            unsafe { exec_request(node2, query_str.as_ptr(), ptr::null(), ptr::null()) };
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

    #[test]
    fn test_basic_export_single_collection() {
        assert!(crate::runtime::init_runtime());
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add two schemas
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let sdl2 = CString::new("type Address { city: String }").unwrap();
        let result = unsafe { add_schema(node, sdl2.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Create documents in both
        let mutation = CString::new(
            r#"mutation { create_User(input: {name: "Alice"}) { _docID } }"#,
        )
        .unwrap();
        let result = unsafe { exec_request(node, mutation.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let mutation =
            CString::new(r#"mutation { create_Address(input: {city: "NYC"}) { _docID } }"#)
                .unwrap();
        let result = unsafe { exec_request(node, mutation.as_ptr(), ptr::null(), ptr::null()) };
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
