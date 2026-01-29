//! Index operations for FFI.
//!
//! This module exposes index management functions that allow creating,
//! dropping, and listing indexes on collections via FFI.
//! Uses CollectionOptions + identity_ptr pattern.

use std::ffi::{c_char, c_int};

use db::collection_short_id;
use storage::corekv::Key;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, resolve_collection, CollectionOptions, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Create a new index on a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `index_name` - Name for the new index
/// * `fields_str` - Comma-separated list of field names to index
/// * `is_unique` - Whether the index should enforce uniqueness (1=true, 0=false)
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "IndexCreate"]
pub unsafe extern "C" fn index_create(
    node_ptr: usize,
    index_name: *const c_char,
    fields_str: *const c_char,
    is_unique: c_int,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let index_name_str = match c_str_to_string(index_name) {
        Some(s) => s,
        None => return FfiResult::error("index_name is null"),
    };

    let fields_string = match c_str_to_string(fields_str) {
        Some(s) => s,
        None => return FfiResult::error("fields_str is null"),
    };

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let collection_name_str = collection.schema().name.clone();

    // Parse fields from comma-separated or JSON array
    let fields: Vec<schema::IndexedFieldDescription> = if fields_string.starts_with('[') {
        // JSON array format
        let parsed: Vec<String> = match serde_json::from_str(&fields_string) {
            Ok(v) => v,
            Err(e) => return FfiResult::error(format!("invalid fields JSON: {}", e)),
        };
        parsed
            .into_iter()
            .map(|name| schema::IndexedFieldDescription {
                name,
                descending: false,
            })
            .collect()
    } else {
        // Comma-separated format
        fields_string
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|name| schema::IndexedFieldDescription {
                name: name.to_string(),
                descending: false,
            })
            .collect()
    };

    if fields.is_empty() {
        return FfiResult::error("at least one field is required");
    }

    let result = rt.block_on(async {
        let txn = database
            .new_txn(false)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        let (index_desc, _updated_schema) = {
            let datastore = txn
                .datastore()
                .map_err(|e| format!("failed to get datastore: {}", e))?;

            let mut index_manager = db::index_manager::IndexManager::from_collection(
                collection_short_id(collection.schema().collection_id.as_str()),
                collection.schema(),
            )
            .map_err(|e| format!("failed to create index manager: {}", e))?;

            let index_desc = index_manager
                .create_index(&datastore, index_name_str, fields, is_unique != 0)
                .await
                .map_err(|e| format!("failed to create index: {}", e))?;

            let mut updated_schema = collection.schema().clone();
            updated_schema.indexes.push(index_desc.clone());

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

            let name_key = storage::keys::systemstore::CollectionNameKey::new(&collection_name_str);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|e| format!("failed to save name mapping: {}", e))?;

            let documents = collection
                .get_all_with_datastore(&datastore)
                .await
                .map_err(|e| format!("failed to get documents: {}", e))?;

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

        txn.commit()
            .await
            .map_err(|e| format!("failed to commit: {}", e))?;

        database
            .reload_cache()
            .await
            .map_err(|e| format!("failed to reload cache: {}", e))?;

        let json = serde_json::to_string(&index_desc)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// List indexes on a collection (or all collections).
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `opts` - Collection options (name to list specific, empty for all)
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// String pointers in `opts` must be null or valid null-terminated UTF-8 strings.
#[export_name = "IndexList"]
pub unsafe extern "C" fn index_list(
    node_ptr: usize,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let name_opt = opts.name_str().filter(|s| !s.is_empty());

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        match name_opt {
            Some(name) => {
                let collection = database
                    .get_collection(&name)
                    .map_err(|e| format!("failed to get collection: {}", e))?
                    .ok_or_else(|| format!("collection '{}' not found", name))?;

                let indexes = collection.get_indexes();
                serde_json::to_string(&indexes)
                    .map_err(|e| format!("failed to serialize result: {}", e))
            }
            None => {
                let names = database
                    .list_collections()
                    .map_err(|e| format!("failed to list collections: {}", e))?;

                let mut all_indexes: std::collections::HashMap<
                    String,
                    Vec<schema::IndexDescription>,
                > = std::collections::HashMap::new();

                for name in names {
                    match database.get_collection(&name) {
                        Ok(Some(collection)) => {
                            let indexes = collection.get_indexes();
                            if !indexes.is_empty() {
                                all_indexes.insert(name, indexes.to_vec());
                            }
                        }
                        Ok(None) => continue,
                        Err(e) => {
                            return Err(format!("failed to get collection '{}': {}", name, e));
                        }
                    }
                }

                serde_json::to_string(&all_indexes)
                    .map_err(|e| format!("failed to serialize result: {}", e))
            }
        }
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Drop an index from a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `index_name` - Name of the index to drop
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "IndexDrop"]
pub unsafe extern "C" fn index_drop(
    node_ptr: usize,
    index_name: *const c_char,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let index_name_str = match c_str_to_string(index_name) {
        Some(s) => s,
        None => return FfiResult::error("index_name is null"),
    };

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let collection_name_str = collection.schema().name.clone();

    let result = rt.block_on(async {
        let txn = database
            .new_txn(false)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        {
            let datastore = txn
                .datastore()
                .map_err(|e| format!("failed to get datastore: {}", e))?;

            let mut index_manager = db::index_manager::IndexManager::from_collection(
                collection_short_id(collection.schema().collection_id.as_str()),
                collection.schema(),
            )
            .map_err(|e| format!("failed to create index manager: {}", e))?;

            let dropped = index_manager
                .drop_index(&datastore, &index_name_str)
                .await
                .map_err(|e| format!("failed to drop index: {}", e))?;

            if !dropped {
                return Err(format!("index '{}' not found", index_name_str));
            }

            let mut updated_schema = collection.schema().clone();
            updated_schema
                .indexes
                .retain(|idx| idx.name != index_name_str);

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

            let name_key = storage::keys::systemstore::CollectionNameKey::new(&collection_name_str);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|e| format!("failed to save name mapping: {}", e))?;
        }

        txn.commit()
            .await
            .map_err(|e| format!("failed to commit: {}", e))?;

        database
            .reload_cache()
            .await
            .map_err(|e| format!("failed to reload cache: {}", e))?;

        Ok::<String, String>("{}".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

// =============================================================================
// Encrypted Index Operations (signature-compatible stubs)
// =============================================================================

/// Create an encrypted index on a collection field.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "EncryptedIndexCreate"]
pub unsafe extern "C" fn encrypted_index_create(
    node_ptr: usize,
    _collection_name: *const c_char,
    _field_name: *const c_char,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }
    FfiResult::error("encrypted indexes not yet implemented in Rust")
}

/// Delete an encrypted index from a collection field.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "EncryptedIndexDelete"]
pub unsafe extern "C" fn encrypted_index_delete(
    node_ptr: usize,
    _collection_name: *const c_char,
    _field_name: *const c_char,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }
    FfiResult::error("encrypted indexes not yet implemented in Rust")
}

/// List encrypted indexes on a collection.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "EncryptedIndexList"]
pub unsafe extern "C" fn encrypted_index_list(
    node_ptr: usize,
    _collection_name: *const c_char,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }
    FfiResult::success("[]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;
    use std::ptr;

    fn make_opts(name: *const c_char) -> CollectionOptions {
        CollectionOptions {
            version: ptr::null(),
            collection_id: ptr::null(),
            name,
            get_inactive: 0,
        }
    }

    #[test]
    fn test_create_and_list_index() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String, email: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0, "add_schema should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let collection_name = CString::new("User").unwrap();
        let index_name = CString::new("idx_email").unwrap();
        let fields = CString::new("email").unwrap();
        let result = unsafe {
            index_create(
                node,
                index_name.as_ptr(),
                fields.as_ptr(),
                1, // unique
                make_opts(collection_name.as_ptr()),
                0,
            )
        };
        assert_eq!(result.status, 0, "index_create should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("idx_email"), "should contain index name");
        unsafe { crate::types::defra_free_string(result.value) };

        // List indexes
        let result = unsafe { index_list(node, make_opts(collection_name.as_ptr()), 0) };
        assert_eq!(result.status, 0, "index_list should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("idx_email"), "should contain index name");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_drop_index() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Post { title: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let collection_name = CString::new("Post").unwrap();
        let index_name = CString::new("idx_title").unwrap();
        let fields = CString::new("title").unwrap();
        let result = unsafe {
            index_create(
                node,
                index_name.as_ptr(),
                fields.as_ptr(),
                0,
                make_opts(collection_name.as_ptr()),
                0,
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe {
            index_drop(
                node,
                index_name.as_ptr(),
                make_opts(collection_name.as_ptr()),
                0,
            )
        };
        assert_eq!(result.status, 0, "index_drop should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe { index_list(node, make_opts(collection_name.as_ptr()), 0) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(!value.contains("idx_title"), "index should be removed");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }
}
