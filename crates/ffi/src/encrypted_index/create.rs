use std::ffi::c_char;

use acp::nac::NodePermission;
use storage::corekv::Key;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::types::FfiResult;
use crate::{ffi_async, ffi_entry, try_ffi};

/// Add a new encrypted index on a collection field.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn add_encrypted_index(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    field_name: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::EncryptedIndexAdd
        ));
        let collection_name_str = try_ffi!(require_c_str(collection_name, "collection_name"));
        let field_name_str = try_ffi!(require_c_str(field_name, "field_name"));
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

            let schema = collection.schema();

            // Check if field exists in the collection
            let field_exists = schema.fields.iter().any(|f| f.name == field_name_str);
            if !field_exists {
                return Err(format!(
                    "encrypted index on non-existent field. Field: {}",
                    field_name_str
                ));
            }

            // Check if encrypted index already exists for this field
            let index_exists = schema
                .encrypted_indexes
                .iter()
                .any(|idx| idx.field_name == field_name_str);
            if index_exists {
                return Err(format!(
                    "encrypted index already exists on this field. Field: {}",
                    field_name_str
                ));
            }

            // Create the encrypted index description
            let enc_idx = schema::EncryptedIndexDescription::new(&field_name_str);

            // Create a transaction
            let txn = database
                .new_txn(false)
                .await
                .map_err(|e| format!("failed to create transaction: {}", e))?;

            // Update the collection schema with the new encrypted index
            {
                let mut updated_schema = schema.clone();
                updated_schema.encrypted_indexes.push(enc_idx.clone());

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

            // Commit the transaction
            txn.commit()
                .await
                .map_err(|e| format!("failed to commit: {}", e))?;

            // Reload the collection cache
            database
                .reload_cache()
                .await
                .map_err(|e| format!("failed to reload cache: {}", e))?;

            // Return the created encrypted index description
            let json = serde_json::to_string(&enc_idx)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
        })
    }
}

/// Delete an encrypted index from a collection.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_encrypted_index(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    field_name: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::EncryptedIndexDelete
        ));
        let collection_name_str = try_ffi!(require_c_str(collection_name, "collection_name"));
        let field_name_str = try_ffi!(require_c_str(field_name, "field_name"));
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

            let schema = collection.schema();

            // Check if encrypted index exists for this field
            let index_exists = schema
                .encrypted_indexes
                .iter()
                .any(|idx| idx.field_name == field_name_str);
            if !index_exists {
                return Err(format!(
                    "encrypted index does not exist on this field. Field: {}",
                    field_name_str
                ));
            }

            // Create a transaction
            let txn = database
                .new_txn(false)
                .await
                .map_err(|e| format!("failed to create transaction: {}", e))?;

            // Update the collection schema to remove the encrypted index
            {
                let mut updated_schema = schema.clone();
                updated_schema
                    .encrypted_indexes
                    .retain(|idx| idx.field_name != field_name_str);

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

            // Commit the transaction
            txn.commit()
                .await
                .map_err(|e| format!("failed to commit: {}", e))?;

            // Reload the collection cache
            database
                .reload_cache()
                .await
                .map_err(|e| format!("failed to reload cache: {}", e))?;

            Ok("{}".to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encrypted_index::list_encrypted_indexes;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_create_and_list_encrypted_index() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String, ssn: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let collection_name = CString::new("User").unwrap();
        let field_name = CString::new("ssn").unwrap();
        let result = unsafe {
            add_encrypted_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                field_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 0, "add_encrypted_index should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("ssn"), "should contain field name");
        assert!(value.contains("equality"), "should contain index type");
        unsafe { crate::types::defra_free_string(result.value) };

        let result =
            unsafe { list_encrypted_indexes(node, std::ptr::null(), collection_name.as_ptr()) };
        assert_eq!(result.status, 0, "list_encrypted_indexes should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("ssn"), "should contain field name");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_delete_encrypted_index() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Employee { name: String, salary: Int }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let collection_name = CString::new("Employee").unwrap();
        let field_name = CString::new("salary").unwrap();
        let result = unsafe {
            add_encrypted_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                field_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe {
            delete_encrypted_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                field_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 0, "delete_encrypted_index should succeed");
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result =
            unsafe { list_encrypted_indexes(node, std::ptr::null(), collection_name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(!value.contains("salary"), "index should be removed");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_add_encrypted_index_nonexistent_field() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Product { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let collection_name = CString::new("Product").unwrap();
        let field_name = CString::new("nonexistent").unwrap();
        let result = unsafe {
            add_encrypted_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                field_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 1, "should fail for non-existent field");
        assert!(!result.error.is_null());
        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("non-existent field"),
            "should contain error message, got: {}",
            error
        );
        unsafe { crate::types::defra_free_string(result.error) };

        node_close(node);
    }

    #[test]
    fn test_create_duplicate_encrypted_index() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Account { email: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let collection_name = CString::new("Account").unwrap();
        let field_name = CString::new("email").unwrap();
        let result = unsafe {
            add_encrypted_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                field_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe {
            add_encrypted_index(
                node,
                std::ptr::null(),
                collection_name.as_ptr(),
                field_name.as_ptr(),
            )
        };
        assert_eq!(result.status, 1, "should fail for duplicate index");
        assert!(!result.error.is_null());
        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("already exists"),
            "should contain error message"
        );
        unsafe { crate::types::defra_free_string(result.error) };

        node_close(node);
    }
}
