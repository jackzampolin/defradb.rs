use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use document::{DocID, Document};
use serde_json::Value as JsonValue;

use storage::corekv::Store;

use super::classify_schema_fields;
use db::{AutoCommitMutator, DB};
use query::mutator::DocMutator;

/// Statistics from a backup import operation.
#[derive(Debug, Clone, Default)]
pub struct ImportStats {
    pub documents_imported: u64,
    pub collections_affected: Vec<String>,
}

/// A relation field whose target could not be resolved when its document
/// was imported (forward reference); applied after all imports.
struct PendingRelation {
    collection_name: String,
    doc_id: DocID,
    fk_name: String,
    imported_doc_id: String,
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
/// Imported documents receive fresh genesis-CID-derived DocIDs. The file's
/// `_docID`/`_docIDNew` values are registered as aliases of the new
/// identity and relation fields are remapped through them, matching Go
/// v1.0.0's import behavior.
pub async fn import_database<S: Store + 'static>(
    database: &Arc<DB<S>>,
    _runner: &Arc<dyn query::QueryExecutor>,
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
    let mut imported_doc_ids: HashMap<String, String> = HashMap::new();
    let mut pending_relations: Vec<PendingRelation> = Vec::new();
    let mutator = AutoCommitMutator::new(database.clone());

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

        let fk_names: Vec<String> = fields
            .iter()
            .filter(|f| f.is_relation && !f.is_array)
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

            // The file's identities become aliases of the fresh
            // genesis-derived identity.
            let mut aliases: Vec<String> = Vec::new();
            for key in ["_docID", "_docIDNew"] {
                if let Some(JsonValue::String(alias)) = doc_map.remove(key) {
                    if !alias.is_empty() && !aliases.contains(&alias) {
                        aliases.push(alias);
                    }
                }
            }

            for (rel_name, fk_name) in &relation_to_fk {
                if let Some(value) = doc_map.remove(rel_name) {
                    if !value.is_null() {
                        doc_map.insert(fk_name.clone(), value);
                    }
                }
            }

            ensure_doc_id_aliases_available(database, &aliases).await?;

            // Resolve relation targets through already-imported identities;
            // unresolved targets are stripped and applied after the import
            // completes (forward references).
            let mut deferred_relations: Vec<(String, String)> = Vec::new();
            for fk_name in &fk_names {
                let Some(value) = doc_map.get(fk_name) else {
                    continue;
                };
                let Some(target) = value.as_str().map(str::to_string) else {
                    continue;
                };
                match imported_doc_ids.get(&target) {
                    Some(new_id) => {
                        doc_map.insert(fk_name.clone(), JsonValue::String(new_id.clone()));
                    }
                    None => {
                        doc_map.remove(fk_name);
                        deferred_relations.push((fk_name.clone(), target));
                    }
                }
            }

            let mut doc =
                Document::from_map(doc_map.clone().into_iter().collect()).map_err(|e| {
                    format!("failed to create document in '{}': {}", collection_name, e)
                })?;
            doc.set_collection(schema.clone());

            let create_result = mutator.create(collection_name, doc).await;
            let created_doc = match create_result {
                Ok(result) => result.document,
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("already exists") {
                        return Err("a document with the given ID already exists".to_string());
                    }
                    return Err(format!(
                        "failed to create document in '{}': {}",
                        collection_name, err_msg
                    ));
                }
            };

            let new_doc_id = created_doc.id().cloned().ok_or_else(|| {
                format!(
                    "created document in '{}' is missing its derived DocID",
                    collection_name
                )
            })?;
            let new_doc_id_string = new_doc_id.to_string();

            register_doc_id_aliases(
                database,
                collection.resolved_root_id(),
                &new_doc_id_string,
                &aliases,
            )
            .await?;
            for alias in aliases {
                imported_doc_ids.insert(alias, new_doc_id_string.clone());
            }

            for (fk_name, imported_doc_id) in deferred_relations {
                pending_relations.push(PendingRelation {
                    collection_name: collection_name.clone(),
                    doc_id: new_doc_id.clone(),
                    fk_name,
                    imported_doc_id,
                });
            }

            documents_imported += 1;
            collections_affected.insert(collection_name.clone());
        }
    }

    for pending in pending_relations {
        let target = imported_doc_ids
            .get(&pending.imported_doc_id)
            .cloned()
            .unwrap_or(pending.imported_doc_id);

        let mut doc = mutator
            .get_for_update(&pending.collection_name, &pending.doc_id)
            .await
            .map_err(|e| {
                format!(
                    "failed to reload relation document in '{}': {}",
                    pending.collection_name, e
                )
            })?
            .ok_or_else(|| {
                format!(
                    "imported document '{}' no longer exists in '{}'",
                    pending.doc_id, pending.collection_name
                )
            })?;
        doc.set(pending.fk_name.clone(), target);
        let mut modified_fields = HashSet::new();
        modified_fields.insert(pending.fk_name.clone());

        mutator
            .update(&pending.collection_name, doc, modified_fields)
            .await
            .map_err(|e| {
                format!(
                    "failed to update relation fields in '{}': {}",
                    pending.collection_name, e
                )
            })?;
    }

    Ok(ImportStats {
        documents_imported,
        collections_affected: collections_affected.into_iter().collect(),
    })
}

async fn ensure_doc_id_aliases_available<S: Store + 'static>(
    database: &Arc<DB<S>>,
    aliases: &[String],
) -> Result<(), String> {
    if aliases.is_empty() {
        return Ok(());
    }

    let txn = database
        .new_txn(true)
        .await
        .map_err(|e| format!("failed to create alias lookup transaction: {e}"))?;
    let result = {
        let systemstore = txn
            .systemstore()
            .map_err(|e| format!("failed to get systemstore: {e}"))?;
        let mut collision = false;
        for alias in aliases {
            if db::doc_id_map::get_doc_ref(&systemstore, alias)
                .await
                .map_err(|e| format!("doc-ID alias lookup failed: {e}"))?
                .is_some()
            {
                collision = true;
                break;
            }
        }
        collision
    };
    txn.discard()
        .map_err(|e| format!("failed to discard alias lookup transaction: {e}"))?;

    if result {
        Err("a document with the given ID already exists".to_string())
    } else {
        Ok(())
    }
}

/// Register the file's original DocIDs as aliases of the imported
/// document's new identity, so existing references stay addressable.
async fn register_doc_id_aliases<S: Store + 'static>(
    database: &Arc<DB<S>>,
    collection_short_id: u32,
    new_doc_id: &str,
    aliases: &[String],
) -> Result<(), String> {
    if aliases.iter().all(|alias| alias == new_doc_id) {
        return Ok(());
    }

    let txn = database
        .new_txn(false)
        .await
        .map_err(|e| format!("failed to create alias transaction: {}", e))?;
    {
        let systemstore = txn
            .systemstore()
            .map_err(|e| format!("failed to get systemstore: {}", e))?;
        let doc_ref = db::doc_id_map::get_doc_ref(&systemstore, new_doc_id)
            .await
            .map_err(|e| format!("doc-ID mapping lookup failed: {}", e))?
            .ok_or_else(|| format!("imported document '{}' has no identity mapping", new_doc_id))?;

        for alias in aliases {
            if alias == new_doc_id {
                continue;
            }
            db::doc_id_map::set_doc_id_alias(
                &systemstore,
                collection_short_id,
                doc_ref.doc_short_id,
                alias,
            )
            .await
            .map_err(|e| format!("failed to register doc-ID alias: {}", e))?;
        }
    }
    txn.commit()
        .await
        .map_err(|e| format!("failed to commit alias transaction: {}", e))?;
    Ok(())
}
