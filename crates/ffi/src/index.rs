//! Index operations for FFI.
//!
//! This module exposes index management functions that allow creating,
//! dropping, and querying indexes on collections via FFI.

use std::ffi::c_char;

use acp::nac::NodePermission;
use db::collection_short_id;
use storage::corekv::Key;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::IndexCreate) {
        return e;
    }

    let collection_name_str = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    let index_json_str = match c_str_to_string(index_json) {
        Some(s) => s,
        None => return FfiResult::error("index_json is null"),
    };

    // Parse the index JSON
    let index_input: IndexCreateInput = match serde_json::from_str(&index_json_str) {
        Ok(idx) => idx,
        Err(e) => return FfiResult::error(format!("failed to parse index JSON: {}", e)),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
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
                collection_short_id(collection.schema().collection_id.as_str()),
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

        // Commit the transaction (datastore reference is now dropped)
        txn.commit()
            .await
            .map_err(|e| format!("failed to commit: {}", e))?;

        // Reload the collection cache to pick up the new index
        database
            .reload_cache()
            .await
            .map_err(|e| format!("failed to reload cache: {}", e))?;

        // Return the created index description
        let json = serde_json::to_string(&index_desc)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
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
/// * `collection_name` - Name of the collection
/// * `index_name` - Name of the index to drop
///
/// # Returns
///
/// Empty JSON object on success, error on failure.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn drop_index(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    index_name: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::IndexDrop) {
        return e;
    }

    let collection_name_str = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    let index_name_str = match c_str_to_string(index_name) {
        Some(s) => s,
        None => return FfiResult::error("index_name is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Get the collection
        let collection = database
            .get_collection(&collection_name_str)
            .map_err(|e| format!("failed to get collection: {}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;

        // Create a transaction
        let txn = database
            .new_txn(false)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        // Do all datastore operations in a scope to drop references before commit
        {
            let datastore = txn
                .datastore()
                .map_err(|e| format!("failed to get datastore: {}", e))?;

            // Create the index manager
            let mut index_manager = db::index_manager::IndexManager::from_collection(
                collection_short_id(collection.schema().collection_id.as_str()),
                collection.schema(),
            )
            .map_err(|e| format!("failed to create index manager: {}", e))?;

            // Drop the index
            let dropped = index_manager
                .drop_index(&datastore, &index_name_str)
                .await
                .map_err(|e| format!("failed to drop index: {}", e))?;

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

        // Reload the collection cache
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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::IndexList) {
        return e;
    }

    let collection_name_str = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Get the collection
        let collection = database
            .get_collection(&collection_name_str)
            .map_err(|e| format!("failed to get collection: {}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;

        // Get indexes from the collection schema
        let indexes = collection.get_indexes();

        // Return JSON array
        let json = serde_json::to_string(&indexes)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
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
/// ```json
/// {
///     "User": [{ "Name": "idx_email", ... }],
///     "Post": [{ "Name": "idx_title", ... }]
/// }
/// ```
#[no_mangle]
pub unsafe extern "C" fn get_all_indexes(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::IndexList) {
        return e;
    }

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Get all collection names
        let names = database
            .list_collections()
            .map_err(|e| format!("failed to list collections: {}", e))?;

        // Build a map of collection name -> indexes
        let mut all_indexes: std::collections::HashMap<String, Vec<schema::IndexDescription>> =
            std::collections::HashMap::new();

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

        // Return JSON object
        let json = serde_json::to_string(&all_indexes)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Input structure for creating an index via FFI.
#[derive(serde::Deserialize)]
struct IndexCreateInput {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Fields", default)]
    fields: Vec<IndexFieldInput>,
    #[serde(rename = "Unique", default)]
    unique: bool,
}

/// Input structure for an indexed field.
#[derive(serde::Deserialize)]
struct IndexFieldInput {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Descending", default)]
    descending: bool,
}

// =============================================================================
// Encrypted Index Stubs
// =============================================================================
// These are stub implementations for searchable encryption indexes.
// The full implementation is on the searchable-encryption branch and will be
// merged later. These stubs allow the FFI to compile and tests to run.

/// Create an encrypted index on a collection field.
///
/// # Stub Implementation
///
/// This is a stub that returns "not implemented" error.
/// Full implementation is on the searchable-encryption branch.
#[no_mangle]
pub unsafe extern "C" fn create_encrypted_index(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _collection_name: *const c_char,
    _index_json: *const c_char,
) -> FfiResult {
    FfiResult::error("encrypted indexes not yet implemented")
}

/// Delete an encrypted index from a collection.
///
/// # Stub Implementation
///
/// This is a stub that returns "not implemented" error.
/// Full implementation is on the searchable-encryption branch.
#[no_mangle]
pub unsafe extern "C" fn delete_encrypted_index(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _collection_name: *const c_char,
    _index_name: *const c_char,
) -> FfiResult {
    FfiResult::error("encrypted indexes not yet implemented")
}

/// List encrypted indexes for a specific collection.
///
/// # Stub Implementation
///
/// This is a stub that returns an empty array.
/// Full implementation is on the searchable-encryption branch.
#[no_mangle]
pub unsafe extern "C" fn list_encrypted_indexes(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _collection_name: *const c_char,
) -> FfiResult {
    // Return empty array - no encrypted indexes in stub
    FfiResult::success("[]")
}

/// List all encrypted indexes across all collections.
///
/// # Stub Implementation
///
/// This is a stub that returns an empty object.
/// Full implementation is on the searchable-encryption branch.
#[no_mangle]
pub unsafe extern "C" fn list_all_encrypted_indexes(
    _node_ptr: usize,
    _identity_did: *const c_char,
) -> FfiResult {
    // Return empty object - no encrypted indexes in stub
    FfiResult::success("{}")
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_drop_index() {
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
            drop_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                index_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 0, "drop_index should succeed");
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
    fn test_get_all_indexes() {
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
        let result = unsafe { get_all_indexes(node, std::ptr::null()) };
        assert_eq!(result.status, 0, "get_all_indexes should succeed");

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
