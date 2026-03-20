use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value as JsonValue;

use storage::corekv::Store;

use super::{classify_schema_fields, json_to_graphql_input};
use crate::database::DB;

/// Statistics from a backup import operation.
#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    pub documents_imported: u64,
    pub collections_affected: Vec<String>,
}

/// Import documents from a JSON string into the database.
///
/// The data must be a JSON object mapping collection names to arrays of documents:
/// ```json
/// {
///     "User": [{"_docID": "...", "_docIDNew": "...", "name": "John", "age": 30}],
///     "Address": [{"_docID": "...", "_docIDNew": "...", "street": "...", "city": "..."}]
/// }
/// ```
///
/// Self-referencing FK fields are stripped before creation and applied
/// via update afterward, matching Go DefraDB behavior.
pub async fn import_database<S: Store>(
    database: &Arc<DB<S>>,
    runner: &Arc<dyn query::QueryExecutor>,
    data: &str,
) -> Result<ImportStats, String> {
    let parsed: JsonValue =
        serde_json::from_str(data).map_err(|e| format!("failed to parse JSON: {}", e))?;

    let root = match parsed.as_object() {
        Some(obj) => obj,
        None => {
            return Err(
                "invalid JSON: expected JSON object at root, got array or primitive".to_string(),
            )
        }
    };

    let mut documents_imported: u64 = 0;
    let mut collections_affected: HashSet<String> = HashSet::new();

    for (collection_name, docs_value) in root {
        let collection = database
            .get_collection(collection_name)
            .map_err(|e| format!("failed to get collection: {}", e))?
            .ok_or_else(|| {
                format!(
                    "failed to get collection: collection not found. Name: {}",
                    collection_name
                )
            })?;

        let schema = collection.schema();
        let fields = classify_schema_fields(schema);

        let self_ref_fk_names: Vec<String> = fields
            .iter()
            .filter(|f| f.is_self_ref && !f.is_array)
            .map(|f| format!("_{}ID", f.name))
            .collect();

        let relation_to_fk: Vec<(String, String)> = fields
            .iter()
            .filter(|f| f.is_relation && !f.is_array)
            .map(|f| (f.name.clone(), format!("_{}ID", f.name)))
            .collect();

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
        let valid_field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
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

        for doc in docs {
            let mut doc_map = match doc.as_object() {
                Some(m) => m.clone(),
                None => continue,
            };

            doc_map.remove("_docID");
            doc_map.remove("_docIDNew");

            for (rel_name, fk_name) in &relation_to_fk {
                if let Some(value) = doc_map.remove(rel_name) {
                    if !value.is_null() {
                        doc_map.insert(fk_name.clone(), value);
                    }
                }
            }

            let mut self_ref_values: Vec<(String, JsonValue)> = Vec::new();
            for fk_name in &self_ref_fk_names {
                if let Some(value) = doc_map.remove(fk_name) {
                    if !value.is_null() {
                        self_ref_values.push((fk_name.clone(), value));
                    }
                }
            }

            let input = json_to_graphql_input(&JsonValue::Object(doc_map));
            let mutation = format!(
                "mutation {{ add_{}(input: {}) {{ _docID }} }}",
                collection_name, input
            );

            let request = query::QueryRequest::new(mutation);
            let response = runner.execute(request).await;

            if !response.errors.is_empty() {
                let errs: Vec<String> = response.errors.iter().map(|e| e.message.clone()).collect();
                let err_msg = errs.join("; ");
                if err_msg.contains("already exists") {
                    return Err("a document with the given ID already exists".to_string());
                }
                return Err(format!(
                    "failed to create document in '{}': {}",
                    collection_name, err_msg
                ));
            }

            documents_imported += 1;
            collections_affected.insert(collection_name.clone());

            if !self_ref_values.is_empty() {
                let response_json = serde_json::to_value(&response.data)
                    .map_err(|e| format!("failed to serialize response: {}", e))?;

                let create_key = format!("add_{}", collection_name);
                let new_doc_id = response_json
                    .get(&create_key)
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|v| v.get("_docID"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "failed to get _docID from create response".to_string())?;

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

    Ok(ImportStats {
        documents_imported,
        collections_affected: collections_affected.into_iter().collect(),
    })
}
