use std::ffi::c_char;

use acp::nac::NodePermission;
use defra_core::{ActionExecution, ActionStatus};
use serde::Serialize;
use storage::corekv::Key;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::types::FfiResult;
use crate::{ffi_async, ffi_entry, try_ffi};

fn visible_indexes(collection: &db::Collection) -> Vec<schema::IndexDescription> {
    collection
        .get_indexes()
        .iter()
        .filter(|index| !is_hidden_auto_relation_index(collection, index))
        .cloned()
        .collect()
}

#[derive(Serialize)]
struct IndexResult {
    #[serde(flatten)]
    description: schema::IndexDescription,
    #[serde(rename = "CollectionName")]
    collection_name: String,
    #[serde(rename = "Execution")]
    execution: ActionExecution,
}

fn index_results(
    collection: &db::Collection,
    actions: &std::collections::HashMap<u32, ActionExecution>,
) -> Vec<IndexResult> {
    visible_indexes(collection)
        .into_iter()
        .map(|description| {
            let execution =
                actions
                    .get(&description.id)
                    .cloned()
                    .unwrap_or_else(|| ActionExecution {
                        collection_id: collection.collection_id().to_string(),
                        subject: description.id.to_string(),
                        status: ActionStatus::COMPLETED,
                        ..Default::default()
                    });
            IndexResult {
                description,
                collection_name: collection.name().to_string(),
                execution,
            }
        })
        .collect()
}

fn is_hidden_auto_relation_index(
    collection: &db::Collection,
    index: &schema::IndexDescription,
) -> bool {
    if index.unique || index.fields.len() != 1 {
        return false;
    }

    let field_name = &index.fields[0].name;
    let Some(stripped_name) = field_name
        .strip_prefix('_')
        .and_then(|name| name.strip_suffix("ID"))
    else {
        return false;
    };

    let expected_name = format!("{}__{}ID_ASC", collection.name(), stripped_name);
    if index.name != expected_name {
        return false;
    }

    collection.schema().fields.iter().any(|field| {
        field.name == *field_name
            && field.relation_name.is_some()
            && field.is_primary
            && matches!(
                field.kind,
                schema::FieldKind::Scalar(schema::ScalarKind::DocID)
            )
    })
}

/// Delete an index from a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_name` - Name of the collection
/// * `index_name` - Name of the index to delete
///
/// # Returns
///
/// Empty JSON object on success, error on failure.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_index(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    index_name: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::IndexDelete
        ));
        let collection_name_str = try_ffi!(require_c_str(collection_name, "collection_name"));
        let index_name_str = try_ffi!(require_c_str(index_name, "index_name"));
        let database = try_ffi!(get_node_database(node_ptr));

        // Bind the caller's identity so any DB-layer NAC gate reached by the body
        // resolves the actual caller instead of the wildcard.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        ffi_async!(rt, {
            // Get the collection
            let collection = database
                .get_collection(&collection_name_str)
                .map_err(|e| format!("failed to get collection: {}", e))?
                .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;
            let collection_id = collection.collection_id().to_string();
            let index_id = collection
                .get_index(&index_name_str)
                .map(|index| index.id)
                .ok_or_else(|| {
                    format!(
                        "index with name doesn't exists. Name: {}",
                        index_name_str
                    )
                })?;

            // Create a transaction
            let txn = database
                .new_txn(false)
                .await
                .map_err(|e| format!("failed to create transaction: {}", e))?;

            // Do all datastore operations in a scope to delete references before commit
            {
                let datastore = txn
                    .datastore()
                    .map_err(|e| format!("failed to get datastore: {}", e))?;

                // Create the index manager
                let mut index_manager = db::index::IndexManager::from_collection(
                    collection.schema().resolved_root_id(),
                    collection.schema(),
                )
                .map_err(|e| format!("failed to create index manager: {}", e))?;

                // Delete the index
                let dropped = index_manager
                    .delete_index(&datastore, &index_name_str)
                    .await
                    .map_err(|e| format!("failed to delete index: {}", e))?;

                if !dropped {
                    return Err(format!(
                        "index with name doesn't exists. Name: {}",
                        index_name_str
                    ));
                }

                // Update the collection schema to remove the index
                let mut updated_schema = collection.schema().clone();
                updated_schema
                    .indexes
                    .retain(|idx| idx.name != index_name_str);

                // Save the updated schema at /collection/id/{version_id}
                let collection_key =
                    storage::keys::systemstore::CollectionKey::new(&updated_schema.version_id);
                let schema_data = serde_json::to_vec(&updated_schema)
                    .map_err(|e| format!("failed to serialize schema: {}", e))?;

                let systemstore = txn
                    .systemstore()
                    .map_err(|e| format!("failed to get systemstore: {}", e))?;

                systemstore
                    .set(&collection_key.bytes(), &schema_data)
                    .await
                    .map_err(|e| format!("failed to save schema: {}", e))?;

                // Update the name → version_id mapping at /collection/name/{name}
                let name_key = storage::keys::systemstore::CollectionNameKey::new(&collection_name_str);
                systemstore
                    .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                    .await
                    .map_err(|e| format!("failed to save name mapping: {}", e))?;
            }

            // Commit the transaction (datastore reference is now dropped)
            txn.commit()
                .await
                .map_err(|e| format!("failed to commit: {}", e))?;

            database
                .clear_action(
                    &collection_id,
                    defra_core::Action::BACKFILL_INDEX,
                    &index_id.to_string(),
                )
                .await
                .map_err(|e| format!("failed to clear index backfill state: {}", e))?;

            // Reload the collection cache
            database
                .reload_cache()
                .await
                .map_err(|e| format!("failed to reload cache: {}", e))?;

            Ok("{}".to_string())
        })
    }
}

/// Get all indexes for a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_name` - Name of the collection
///
/// # Returns
///
/// JSON array of index descriptions.
///
/// # Safety
///
/// `collection_name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn get_indexes(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::IndexList
        ));
        let collection_name_str = try_ffi!(require_c_str(collection_name, "collection_name"));
        let database = try_ffi!(get_node_database(node_ptr));

        // Bind the caller's identity so any DB-layer NAC gate reached by the body
        // resolves the actual caller instead of the wildcard.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        ffi_async!(rt, {
            // Get the collection
            let collection = database
                .get_collection(&collection_name_str)
                .map_err(|e| format!("failed to get collection: {}", e))?
                .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;

            let actions = database
                .list_index_actions(collection.collection_id())
                .await
                .map_err(|e| format!("failed to list index actions: {}", e))?;
            let indexes = index_results(&collection, &actions);

            // Return JSON array
            let json = serde_json::to_string(&indexes)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
        })
    }
}

/// Get all indexes across all collections.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
///
/// # Returns
///
/// JSON object mapping collection names to their index arrays.
///
/// # Safety
///
/// Caller must ensure all pointer arguments are valid, non-null, and point to valid C strings.
#[no_mangle]
pub unsafe extern "C" fn list_all_indexes(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::IndexList
        ));
        let database = try_ffi!(get_node_database(node_ptr));

        // Bind the caller's identity so any DB-layer NAC gate reached by the body
        // resolves the actual caller instead of the wildcard.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        ffi_async!(rt, {
            // Get all collection names
            let names = database
                .list_collections()
                .map_err(|e| format!("failed to list collections: {}", e))?;

            // Build a map of collection name -> indexes
            let mut all_indexes: std::collections::HashMap<String, Vec<IndexResult>> =
                std::collections::HashMap::new();

            for name in names {
                match database.get_collection(&name) {
                    Ok(Some(collection)) => {
                        let actions = database
                            .list_index_actions(collection.collection_id())
                            .await
                            .map_err(|e| format!("failed to list index actions: {}", e))?;
                        let indexes = index_results(&collection, &actions);
                        if !indexes.is_empty() {
                            all_indexes.insert(name, indexes);
                        }
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        return Err(format!("failed to get collection '{}': {}", name, e));
                    }
                }
            }

            // Return JSON object
            let json = serde_json::to_string(&all_indexes)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::create::create_index;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_delete_index() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Post { title: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Create index
        let collection_name = CString::new("Post").unwrap();
        let index_json =
            CString::new(r#"{"Name": "idx_title", "Fields": [{"Name": "title"}]}"#).unwrap();
        let result = unsafe {
            create_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                index_json.as_ptr(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Drop index
        let index_name = CString::new("idx_title").unwrap();
        let result = unsafe {
            delete_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                index_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 0, "delete_index should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Verify index is gone
        let result = unsafe { get_indexes(node, std::ptr::null(), collection_name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(!value.contains("idx_title"), "index should be removed");
        unsafe { crate::types::defra_free_string(result.value) };

        // Cleanup
        node_close(node);
    }

    #[test]
    fn test_list_all_indexes() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schemas
        let sdl = CString::new("type Author { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let sdl = CString::new("type Book { title: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Create indexes on both collections
        let author_coll = CString::new("Author").unwrap();
        let book_coll = CString::new("Book").unwrap();

        let idx1 =
            CString::new(r#"{"Name": "idx_author_name", "Fields": [{"Name": "name"}]}"#).unwrap();
        let result =
            unsafe { create_index(node, std::ptr::null(), author_coll.as_ptr(), idx1.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let idx2 =
            CString::new(r#"{"Name": "idx_book_title", "Fields": [{"Name": "title"}]}"#).unwrap();
        let result =
            unsafe { create_index(node, std::ptr::null(), book_coll.as_ptr(), idx2.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Get all indexes
        let result = unsafe { list_all_indexes(node, std::ptr::null()) };
        assert_eq!(result.status, 0, "list_all_indexes should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Author"), "should contain Author collection");
        assert!(value.contains("Book"), "should contain Book collection");
        assert!(
            value.contains("idx_author_name"),
            "should contain author index"
        );
        assert!(
            value.contains("idx_book_title"),
            "should contain book index"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        // Cleanup
        node_close(node);
    }
}
