use std::ffi::c_char;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// List encrypted indexes for a collection.
///
/// # Safety
///
/// `collection_name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn list_encrypted_indexes(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let _identity = c_str_to_string(identity_did);

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

        // Get encrypted indexes from the collection schema
        let encrypted_indexes = &collection.schema().encrypted_indexes;

        // Return JSON array
        let json = serde_json::to_string(encrypted_indexes)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// List all encrypted indexes across all collections.
///
/// # Safety
///
/// Caller must ensure all pointer arguments are valid, non-null, and point to valid C strings.
#[no_mangle]
pub unsafe extern "C" fn list_all_encrypted_indexes(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let _identity = c_str_to_string(identity_did);

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

        // Build a map of collection name -> encrypted indexes
        let mut all_encrypted_indexes: std::collections::HashMap<
            String,
            Vec<schema::EncryptedIndexDescription>,
        > = std::collections::HashMap::new();

        for name in names {
            match database.get_collection(&name) {
                Ok(Some(collection)) => {
                    let encrypted_indexes = collection.schema().encrypted_indexes.clone();
                    if !encrypted_indexes.is_empty() {
                        all_encrypted_indexes.insert(name, encrypted_indexes);
                    }
                }
                Ok(None) => continue,
                Err(e) => {
                    return Err(format!("failed to get collection '{}': {}", name, e));
                }
            }
        }

        // Return JSON object
        let json = serde_json::to_string(&all_encrypted_indexes)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encrypted_index::create_encrypted_index;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_list_all_encrypted_indexes() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Person { name: String, ssn: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let sdl = CString::new("type Company { name: String, taxId: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let person_coll = CString::new("Person").unwrap();
        let company_coll = CString::new("Company").unwrap();
        let ssn_field = CString::new("ssn").unwrap();
        let tax_field = CString::new("taxId").unwrap();

        let result = unsafe {
            create_encrypted_index(
                node,
                std::ptr::null(),
                person_coll.as_ptr(),
                ssn_field.as_ptr(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe {
            create_encrypted_index(
                node,
                std::ptr::null(),
                company_coll.as_ptr(),
                tax_field.as_ptr(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe { list_all_encrypted_indexes(node, std::ptr::null()) };
        assert_eq!(
            result.status, 0,
            "list_all_encrypted_indexes should succeed"
        );

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Person"), "should contain Person collection");
        assert!(
            value.contains("Company"),
            "should contain Company collection"
        );
        assert!(value.contains("ssn"), "should contain ssn field");
        assert!(value.contains("taxId"), "should contain taxId field");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }
}
