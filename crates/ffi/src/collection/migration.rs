use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{ffi_async, ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};

/// Set migration for collection versions.
///
/// Sets the migration for all collections using the given source-destination
/// collection version IDs.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `config` - JSON string of LensConfig containing:
///   - `source_version_id`: Source collection version ID
///   - `destination_version_id`: Destination collection version ID
///   - `lens`: Lens transform configuration
///
/// # Returns
///
/// - Status 0: Success (value contains the Lens transform ID)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `config` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn set_migration(
    node_ptr: usize,
    identity_did: *const c_char,
    config: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::MigrationSet
        ));
        let config_str = try_ffi!(require_c_str(config, "config"));
        let database = try_ffi!(get_node_database(node_ptr));

        ffi_async!(rt, {
            // Parse the LensConfig from JSON
            let lens_config: lens::LensConfig = serde_json::from_str(&config_str)
                .map_err(|e| format!("failed to parse lens config: {}", e))?;

            // Register the migration with the lens store
            let transform_id = database
                .set_migration(lens_config, None)
                .await
                .map_err(|e| format!("failed to set migration: {}", e))?;

            Ok(transform_id.to_string())
        })
    }
}

/// Set a migration within an existing transaction.
///
/// This registers a lens migration configuration within the specified transaction.
/// The migration will only be visible after the transaction is committed.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `txn_id` - Transaction ID from `begin_txn`
/// * `identity_did` - Optional DID for permission checks (null for anonymous)
/// * `config` - JSON string containing the lens configuration
///
/// # Returns
///
/// - Status 0: Success (value contains the transform ID)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `txn_id` and `config` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn set_migration_in_txn(
    node_ptr: usize,
    txn_id: *const c_char,
    identity_did: *const c_char,
    config: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::MigrationSet
        ));
        let txn_str = try_ffi!(require_c_str(txn_id, "txn_id"));
        let config_str = try_ffi!(require_c_str(config, "config"));

        // Get the transaction registry
        let registry = match NODES.get(node_ptr, |state| state.txn_registry.clone()) {
            Some(r) => r,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async!(rt, {
            // Parse the LensConfig from JSON
            let lens_config: lens::LensConfig = serde_json::from_str(&config_str)
                .map_err(|e| format!("failed to parse lens config: {}", e))?;

            // Register the migration within the transaction
            let transform_id = registry
                .set_migration_in_txn(&txn_str, lens_config, None)
                .await
                .map_err(|e| format!("failed to set migration in txn: {}", e))?;

            Ok(transform_id.to_string())
        })
    }
}

/// Delete multiple collection versions by their version IDs.
///
/// Takes a JSON array of version ID strings. Versions are deleted in
/// topological order (children before parents).
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `version_ids_json` - JSON array of version ID strings
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `version_ids_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn delete_collection_versions(
    node_ptr: usize,
    identity_did: *const c_char,
    version_ids_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::CollectionPatch
        ));
        let ids_str = try_ffi!(require_c_str(version_ids_json, "version_ids_json"));
        let database = try_ffi!(get_node_database(node_ptr));

        ffi_async!(rt, {
            let version_ids: Vec<String> = serde_json::from_str(&ids_str)
                .map_err(|e| format!("failed to parse version IDs JSON: {}", e))?;

            database
                .delete_collection_versions_batch(version_ids)
                .await
                .map_err(|e| format!("failed to delete collection versions: {}", e))?;

            Ok("{}".to_string())
        })
    }
}
