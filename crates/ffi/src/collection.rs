//! Collection management operations for FFI.
//!
//! This module exposes collection lifecycle and management functions
//! that match Go's collection management behavior.

use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Convert a Rust `Select` into a Go-compatible `request.Select` JSON object.
///
/// Go's `request.Select` uses PascalCase keys and `immutable.Option[T]` which
/// serializes as `null` when empty or the bare value when present.
fn select_to_go_json(select: &query::Select) -> serde_json::Value {
    let fields: Vec<serde_json::Value> = select
        .fields
        .iter()
        .map(|f| match f {
            query::mapper::Requestable::Field(field) => {
                let mut m = serde_json::Map::new();
                m.insert("Name".into(), serde_json::Value::String(field.name.clone()));
                m.insert(
                    "Alias".into(),
                    field
                        .alias
                        .as_ref()
                        .map(|a| serde_json::Value::String(a.clone()))
                        .unwrap_or(serde_json::Value::Null),
                );
                serde_json::Value::Object(m)
            }
            query::mapper::Requestable::Select(sub) => select_to_go_json(sub),
            query::mapper::Requestable::Similarity(_) => {
                // Similarity fields are not used in view query serialization
                serde_json::Value::Null
            }
            query::mapper::Requestable::Aggregate(agg) => {
                let mut m = serde_json::Map::new();
                m.insert(
                    "Name".into(),
                    serde_json::Value::String(agg.aggregate_type.as_str().to_string()),
                );
                m.insert(
                    "Alias".into(),
                    agg.alias
                        .as_ref()
                        .map(|a| serde_json::Value::String(a.clone()))
                        .unwrap_or(serde_json::Value::Null),
                );
                let targets: Vec<serde_json::Value> = agg
                    .targets
                    .iter()
                    .map(|t| {
                        let mut tm = serde_json::Map::new();
                        tm.insert(
                            "HostName".into(),
                            serde_json::Value::String(t.host_name.clone()),
                        );
                        tm.insert(
                            "ChildName".into(),
                            t.field_name
                                .as_ref()
                                .map(|n| serde_json::Value::String(n.clone()))
                                .unwrap_or(serde_json::Value::Null),
                        );
                        tm.insert("Filter".into(), serde_json::Value::Null);
                        tm.insert("Limit".into(), serde_json::Value::Null);
                        tm.insert("Offset".into(), serde_json::Value::Null);
                        tm.insert("OrderBy".into(), serde_json::Value::Null);
                        serde_json::Value::Object(tm)
                    })
                    .collect();
                m.insert("Targets".into(), serde_json::Value::Array(targets));
                serde_json::Value::Object(m)
            }
        })
        .collect();

    let mut m = serde_json::Map::new();
    m.insert(
        "Name".into(),
        serde_json::Value::String(select.collection_name.clone()),
    );
    m.insert(
        "Alias".into(),
        select
            .field
            .alias
            .as_ref()
            .map(|a| serde_json::Value::String(a.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert("Fields".into(), serde_json::Value::Array(fields));
    m.insert(
        "Limit".into(),
        select
            .limit
            .as_ref()
            .and_then(|l| l.limit)
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert(
        "Offset".into(),
        select
            .limit
            .as_ref()
            .map(|l| {
                if l.offset > 0 {
                    serde_json::Value::Number(l.offset.into())
                } else {
                    serde_json::Value::Null
                }
            })
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert("OrderBy".into(), serde_json::Value::Null);
    m.insert(
        "Filter".into(),
        select
            .filter
            .as_ref()
            .map(|f| {
                let conditions: serde_json::Map<String, serde_json::Value> = f
                    .conditions()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let mut filter_obj = serde_json::Map::new();
                filter_obj.insert(
                    "Conditions".into(),
                    serde_json::Value::Object(conditions),
                );
                serde_json::Value::Object(filter_obj)
            })
            .unwrap_or(serde_json::Value::Null),
    );
    m.insert("DocIDs".into(), serde_json::Value::Null);
    m.insert("CID".into(), serde_json::Value::Null);
    m.insert("GroupBy".into(), serde_json::Value::Null);
    m.insert(
        "ShowDeleted".into(),
        serde_json::Value::Bool(select.show_deleted),
    );
    m.insert(
        "IsEncrypted".into(),
        serde_json::Value::Bool(select.is_encrypted),
    );

    serde_json::Value::Object(m)
}

/// Get a collection by name.
///
/// Returns a JSON object containing the collection's schema (CollectionVersion)
/// if found, or an error if the collection doesn't exist.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name
///
/// # Returns
///
/// - Status 0: Success (value contains JSON CollectionVersion)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn get_collection_by_name(
    node_ptr: usize,
    identity_did: *const c_char,
    name: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionGet) {
        return e;
    }

    let name_str = match c_str_to_string(name) {
        Some(s) => s,
        None => return FfiResult::error("name is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let collection = database
            .get_collection(&name_str)
            .map_err(|e| format!("failed to get collection: {}", e))?
            .ok_or_else(|| format!("collection '{}' not found", name_str))?;

        let json = serde_json::to_string(collection.schema())
            .map_err(|e| format!("failed to serialize collection: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Check if a collection exists by name.
///
/// Returns a JSON boolean: `true` if the collection exists, `false` otherwise.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name to check
///
/// # Returns
///
/// - Status 0: Success (value contains "true" or "false")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn has_collection(
    node_ptr: usize,
    identity_did: *const c_char,
    name: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionGet) {
        return e;
    }

    let name_str = match c_str_to_string(name) {
        Some(s) => s,
        None => return FfiResult::error("name is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let exists = database
            .has_collection(&name_str)
            .map_err(|e| format!("failed to check collection: {}", e))?;

        Ok::<String, String>(exists.to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Delete a collection by name.
///
/// Deletes the collection and all its documents.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name to delete
///
/// # Returns
///
/// - Status 0: Success (value is empty)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn delete_collection(
    node_ptr: usize,
    identity_did: *const c_char,
    name: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionPatch) {
        return e;
    }

    let name_str = match c_str_to_string(name) {
        Some(s) => s,
        None => return FfiResult::error("name is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        database
            .delete_collection(&name_str)
            .await
            .map_err(|e| format!("failed to delete collection: {}", e))?;

        Ok::<String, String>("{}".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Find a collection by its collection ID (schema version ID).
///
/// This is useful for P2P sync where we receive blocks with schema_version_id
/// and need to find the corresponding collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_id` - The collection ID (schema version ID)
///
/// # Returns
///
/// - Status 0: Success (value contains JSON CollectionVersion or "null" if not found)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `collection_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn find_collection_by_id(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_id: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionGet) {
        return e;
    }

    let id_str = match c_str_to_string(collection_id) {
        Some(s) => s,
        None => return FfiResult::error("collection_id is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let collection = database
            .find_collection_by_id(&id_str)
            .map_err(|e| format!("failed to find collection: {}", e))?;

        let json = match collection {
            Some(c) => serde_json::to_string(c.schema())
                .map_err(|e| format!("failed to serialize collection: {}", e))?,
            None => "null".to_string(),
        };

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Set the active collection version.
///
/// This activates the collection with the given version ID and deactivates
/// any other versions of the same collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `version_id` - The version ID of the collection to activate
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `version_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn set_active_collection_version(
    node_ptr: usize,
    identity_did: *const c_char,
    version_id: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionPatch) {
        return e;
    }

    let version_str = match c_str_to_string(version_id) {
        Some(s) => s,
        None => return FfiResult::error("version_id is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        database
            .set_active_collection_version(&version_str)
            .await
            .map_err(|e| format!("failed to set active collection version: {}", e))?;

        Ok::<String, String>("{}".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Patch a collection's schema using JSON patch operations.
///
/// This applies the given JSON patch to the collection's schema,
/// validates the result, and updates the collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_name` - The name of the collection to patch
/// * `patch` - A JSON patch string (RFC 6902 format)
///
/// # Returns
///
/// - Status 0: Success (value contains the updated CollectionVersion as JSON)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `collection_name` and `patch` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn patch_collection(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    patch: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionPatch) {
        return e;
    }

    let name_str = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    let patch_str = match c_str_to_string(patch) {
        Some(s) => s,
        None => return FfiResult::error("patch is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let updated_schema = database
            .patch_collection(&name_str, &patch_str)
            .await
            .map_err(|e| format!("failed to patch collection: {}", e))?;

        let json = serde_json::to_string(&updated_schema)
            .map_err(|e| format!("failed to serialize updated schema: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Get a collection by its version ID.
///
/// This searches all collections for one matching the given version ID.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `version_id` - The version ID to search for
///
/// # Returns
///
/// - Status 0: Success (value contains JSON CollectionVersion or "null" if not found)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `version_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn get_collection_by_version_id(
    node_ptr: usize,
    identity_did: *const c_char,
    version_id: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionGet) {
        return e;
    }

    let version_str = match c_str_to_string(version_id) {
        Some(s) => s,
        None => return FfiResult::error("version_id is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let collection = database
            .get_collection_by_version_id(&version_str)
            .map_err(|e| format!("failed to get collection: {}", e))?;

        let json = match collection {
            Some(c) => serde_json::to_string(c.schema())
                .map_err(|e| format!("failed to serialize collection: {}", e))?,
            None => "null".to_string(),
        };

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

// =============================================================================
// View and Migration APIs
// =============================================================================

/// Add a view to the database.
///
/// Creates a new Defra View from a GQL query and SDL schema.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `gql_query` - The GraphQL query defining the view
/// * `sdl` - The SDL schema for the view output type
/// * `transform` - Optional Lens transform configuration (JSON, null for none)
///
/// # Returns
///
/// - Status 0: Success (value contains JSON array of CollectionVersions)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[no_mangle]
pub unsafe extern "C" fn add_view(
    node_ptr: usize,
    identity_did: *const c_char,
    gql_query: *const c_char,
    sdl: *const c_char,
    transform: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionPatch) {
        return e;
    }

    let query_str = match c_str_to_string(gql_query) {
        Some(s) => s,
        None => return FfiResult::error("gql_query is null"),
    };

    let sdl_str = match c_str_to_string(sdl) {
        Some(s) => s,
        None => return FfiResult::error("sdl is null"),
    };

    let transform_opt = c_str_to_string(transform);

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Get existing collection names so the SDL parser can resolve external type references
        let known_types: std::collections::HashSet<String> = database
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // Parse the SDL into collection versions, passing known types for resolution
        let collections = query::parse_sdl_with_known_types(&sdl_str, known_types)
            .map_err(|e| format!("failed to parse view SDL: {}", e))?;

        // Parse the GQL query into a Select, matching Go's `addView` behavior:
        // Go wraps query as `query { <input> }` then parses to request.Select
        let wrapped_query = format!("query {{ {} }}", &query_str);
        let selects = query::parse_query(&wrapped_query)
            .map_err(|e| format!("failed to parse view query: {}", e))?;
        if selects.is_empty() {
            return Err("invalid view query: no selections found".to_string());
        }
        let select_json = select_to_go_json(&selects[0]);

        // Validate transform CID exists in the lens store if provided
        if let Some(ref t) = transform_opt {
            let lens_store = database.lens_store();
            // Check each transform ID (may be comma-separated for chained transforms)
            for cid in t.split(',') {
                let cid = cid.trim();
                if !cid.is_empty() {
                    let tid = lens::TransformId::new(cid);
                    if !lens_store.has_transform(&tid) {
                        return Err("lens CID not found".to_string());
                    }
                }
            }
        }

        // Build the query source with Go-compatible Select JSON
        let mut query_source = schema::QuerySource::new(select_json);
        if let Some(ref t) = transform_opt {
            query_source = query_source.with_transform(t);
        }

        // Attach query source to all collections
        let view_collections: Vec<_> = collections
            .into_iter()
            .map(|mut col_version| {
                col_version.query = Some(query_source.clone());
                col_version
            })
            .collect();

        // Create all view collections atomically (all-or-nothing)
        let created_versions = database
            .create_collections_atomic(view_collections)
            .await
            .map_err(|e| format!("failed to create view collection: {}", e))?;

        let json = serde_json::to_string(&created_versions)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Refresh view caches.
///
/// Refreshes the caches of all views matching the given options.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `options` - JSON string of CollectionFetchOptions (null for all views)
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `options` must be null or a valid null-terminated UTF-8 string.
///
/// # Note
///
/// Not yet implemented. See issue #178.
#[no_mangle]
pub unsafe extern "C" fn refresh_views(_node_ptr: usize, _options: *const c_char) -> FfiResult {
    FfiResult::error("refresh_views is not yet implemented - see issue #178")
}

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
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionPatch) {
        return e;
    }

    let config_str = match c_str_to_string(config) {
        Some(s) => s,
        None => return FfiResult::error("config is null"),
    };

    // Validate node handle before entering async block
    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Parse the LensConfig from JSON
        let lens_config: lens::LensConfig = serde_json::from_str(&config_str)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;

        // Register the migration with the lens store
        let transform_id = database
            .set_migration(lens_config)
            .await
            .map_err(|e| format!("failed to set migration: {}", e))?;

        Ok::<String, String>(transform_id.to_string())
    });

    match result {
        Ok(transform_id) => FfiResult::success(&transform_id),
        Err(e) => FfiResult::error(&e),
    }
}

/// Truncate a collection (delete all documents, preserve schema).
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name to truncate
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn truncate_collection(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _name: *const c_char,
) -> FfiResult {
    FfiResult::error("truncate_collection is not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_get_collection_by_name() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Get collection by name
        let name = CString::new("User").unwrap();
        let result = unsafe { get_collection_by_name(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0, "get_collection_by_name should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("User"), "should contain User collection");
        assert!(value.contains("name"), "should contain name field");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_get_collection_by_name_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let name = CString::new("NonExistent").unwrap();
        let result = unsafe { get_collection_by_name(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(
            result.status, 1,
            "should return error for non-existent collection"
        );
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("not found"),
            "error should mention not found"
        );

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_get_collection_by_name_null_pointer() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { get_collection_by_name(node, std::ptr::null(), std::ptr::null()) };
        assert_eq!(result.status, 1, "should return error for null name");
        assert!(!result.error.is_null());

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_has_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Person { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Check existing collection
        let name = CString::new("Person").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Check non-existing collection
        let name = CString::new("NonExistent").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_delete_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type ToDelete { field: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Verify it exists
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete it
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { delete_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0, "delete_collection should succeed");
        unsafe { crate::types::defra_free_string(result.value) };

        // Verify it's gone
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_find_collection_by_id() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type FindMe { data: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract collection ID from add_schema result
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let collection_id = collections[0]["CollectionID"].as_str().unwrap();

        unsafe { crate::types::defra_free_string(result.value) };

        // Find by collection ID
        let id_cstr = CString::new(collection_id).unwrap();
        let result = unsafe { find_collection_by_id(node, std::ptr::null(), id_cstr.as_ptr()) };
        assert_eq!(result.status, 0, "find_collection_by_id should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("FindMe"), "should contain FindMe collection");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_find_collection_by_id_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let id = CString::new("bafkreibnonexistent").unwrap();
        let result = unsafe { find_collection_by_id(node, std::ptr::null(), id.as_ptr()) };
        assert_eq!(result.status, 0, "should succeed with null value");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "null", "should return null for non-existent ID");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_set_active_collection_version() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Active { data: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract version ID from result
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let version_id = collections[0]["VersionID"].as_str().unwrap();
        unsafe { crate::types::defra_free_string(result.value) };

        // Set active version (should succeed)
        let version_cstr = CString::new(version_id).unwrap();
        let result = unsafe { set_active_collection_version(node, std::ptr::null(), version_cstr.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "set_active_collection_version should succeed"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_set_active_collection_version_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let version_id = CString::new("nonexistent-version-id").unwrap();
        let result = unsafe { set_active_collection_version(node, std::ptr::null(), version_id.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for non-existent version");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_get_collection_by_version_id() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type VersionTest { field: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract version ID
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let version_id = collections[0]["VersionID"].as_str().unwrap();
        unsafe { crate::types::defra_free_string(result.value) };

        // Get by version ID
        let version_cstr = CString::new(version_id).unwrap();
        let result = unsafe { get_collection_by_version_id(node, std::ptr::null(), version_cstr.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "get_collection_by_version_id should succeed"
        );

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("VersionTest"), "should contain VersionTest");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_patch_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Patchable { original: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Patch the collection - change is_active to false
        let patch = CString::new(r#"[{"op":"replace","path":"/IsActive","value":false}]"#).unwrap();
        let name = CString::new("Patchable").unwrap();
        let result = unsafe { patch_collection(node, std::ptr::null(), name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 0, "patch_collection should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Patchable"), "should contain Patchable");
        assert!(
            value.contains("\"IsActive\":false"),
            "should have IsActive:false"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_patch_collection_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let patch = CString::new(r#"[{"op":"replace","path":"/IsActive","value":false}]"#).unwrap();
        let name = CString::new("NonExistent").unwrap();
        let result = unsafe { patch_collection(node, std::ptr::null(), name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for non-existent collection");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_patch_collection_invalid_patch() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type PatchTest { field: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Invalid patch - not valid JSON
        let patch = CString::new("not valid json").unwrap();
        let name = CString::new("PatchTest").unwrap();
        let result = unsafe { patch_collection(node, std::ptr::null(), name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for invalid patch");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_add_view() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add base schema first
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Add a view
        let gql_query = CString::new("{ User { name } }").unwrap();
        let view_sdl = CString::new("type UserView { name: String }").unwrap();
        let result = unsafe {
            add_view(
                node,
                std::ptr::null(),
                gql_query.as_ptr(),
                view_sdl.as_ptr(),
                std::ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "add_view should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("UserView"), "should contain view name");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_add_view_null_query() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let view_sdl = CString::new("type V { name: String }").unwrap();
        let result =
            unsafe { add_view(node, std::ptr::null(), std::ptr::null(), view_sdl.as_ptr(), std::ptr::null()) };
        assert_eq!(result.status, 1, "should fail with null query");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }
}
