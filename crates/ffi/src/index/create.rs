use std::ffi::c_char;

use acp::nac::NodePermission;
use storage::corekv::Key;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::types::FfiResult;
use crate::{ffi_async, ffi_entry, try_ffi};

use super::IndexCreateInput;

/// Create a new index on a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_name` - Name of the collection to create the index on
/// * `index_json` - JSON object describing the index to create
///
/// # Index JSON Format
///
/// ```json
/// {
///     "Name": "my_index",
///     "Fields": [
///         {"Name": "field1", "Descending": false},
///         {"Name": "field2", "Descending": true}
///     ],
///     "Unique": false
/// }
/// ```
///
/// # Returns
///
/// JSON object containing the created index description with assigned ID.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn create_index(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    index_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::IndexCreate
        ));
        let collection_name_str = try_ffi!(require_c_str(collection_name, "collection_name"));
        let index_json_str = try_ffi!(require_c_str(index_json, "index_json"));

        // Parse the index JSON
        let index_input: IndexCreateInput = match serde_json::from_str(&index_json_str) {
            Ok(idx) => idx,
            Err(e) => return FfiResult::error(format!("failed to parse index JSON: {}", e)),
        };

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

            // Build the fields list
            let fields: Vec<schema::IndexedFieldDescription> = index_input
                .fields
                .into_iter()
                .map(|f| schema::IndexedFieldDescription {
                    name: f.name,
                    descending: f.descending,
                })
                .collect();

            // Create a transaction for the index creation
            let txn = database
                .new_txn(false)
                .await
                .map_err(|e| format!("failed to create transaction: {}", e))?;

            // Do all datastore operations in a scope to drop references before commit
            let (index_desc, _updated_schema) = {
                let datastore = txn
                    .datastore()
                    .map_err(|e| format!("failed to get datastore: {}", e))?;

                // Create the index manager
                let mut index_manager = db::index_manager::IndexManager::from_collection(
                    collection.schema().resolved_root_id(),
                    collection.schema(),
                )
                .map_err(|e| format!("failed to create index manager: {}", e))?;

                // Create the index
                let index_desc = index_manager
                    .create_index(
                        &datastore,
                        &collection_name_str,
                        index_input.name,
                        fields,
                        index_input.unique,
                        &collection.schema().fields,
                    )
                    .await
                    .map_err(|e| format!("{}", e))?;

                // Update the collection schema with the new index
                let mut updated_schema = collection.schema().clone();
                updated_schema.indexes.push(index_desc.clone());

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

                // Bulk index existing documents
                let documents: Vec<(u64, document::Document)> = collection
                    .get_all_with_datastore_short_ids(&datastore, &systemstore, false)
                    .await
                    .map_err(|e| format!("failed to get documents: {}", e))?
                    .into_iter()
                    .map(|(doc_short_id, doc, _)| (doc_short_id, doc))
                    .collect();

                if !documents.is_empty() {
                    index_manager
                        .bulk_index(
                            &datastore,
                            &index_desc.name,
                            &documents,
                            collection.schema(),
                        )
                        .await
                        .map_err(|e| format!("failed to bulk index: {}", e))?;
                }

                (index_desc, updated_schema)
            };

            // Commit the transaction (datastore reference is now dropped)
            txn.commit()
                .await
                .map_err(|e| format!("failed to commit: {}", e))?;

            // Reload the collection cache to pick up the new index
            database
                .reload_cache()
                .await
                .map_err(|e| format!("failed to reload cache: {}", e))?;

            // If the collection has migrations, rebuild the index with migrated values.
            // The initial bulk_index above uses raw document values, but if a lens migration
            // exists, the index should contain migrated values for the active version.
            database
                .reindex_collection_with_migrations(&collection_name_str)
                .await
                .map_err(|e| format!("failed to reindex after migration: {}", e))?;

            // Return the created index description
            let json = serde_json::to_string(&index_desc)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::manage::get_indexes;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_create_and_get_index() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String, email: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Create index
        let collection_name = CString::new("User").unwrap();
        let index_json =
            CString::new(r#"{"Name": "idx_email", "Fields": [{"Name": "email"}], "Unique": true}"#)
                .unwrap();
        let result = unsafe {
            create_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                index_json.as_ptr(),
            )
        };
        assert_eq!(result.status, 0, "create_index should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("idx_email"), "should contain index name");
        unsafe { crate::types::defra_free_string(result.value) };

        // Get indexes
        let result = unsafe { get_indexes(node, std::ptr::null(), collection_name.as_ptr()) };
        assert_eq!(result.status, 0, "get_indexes should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("idx_email"), "should contain index name");
        unsafe { crate::types::defra_free_string(result.value) };

        // Cleanup
        node_close(node);
    }
}
