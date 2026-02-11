use std::ffi::c_char;

use acp::nac::NodePermission;

use super::select_to_go_json;
use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

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

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::CollectionPatch)
    {
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

        // Auto-refresh materialized views after creation
        // Exclude embedded-only types (interfaces) - they can't be queried
        let materialized_names: Vec<String> = created_versions
            .iter()
            .filter(|col| col.is_materialized && !col.is_embedded_only)
            .map(|col| col.name.clone())
            .collect();

        if !materialized_names.is_empty() {
            database
                .refresh_views(Some(db::RefreshViewsOptions::with_names(
                    materialized_names,
                )))
                .await
                .map_err(|e| format!("failed to refresh materialized views: {}", e))?;
        }

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
/// Refreshes the caches of all materialized views matching the given options.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `options` - JSON string with optional "Names" field (null for all views)
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `options` must be null or a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn refresh_views(node_ptr: usize, options: *const c_char) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    // Parse options if provided
    let refresh_options = if let Some(opts_str) = c_str_to_string(options) {
        // Parse JSON options
        match serde_json::from_str::<serde_json::Value>(&opts_str) {
            Ok(json) => {
                let names = json.get("Names").and_then(|n| n.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                });
                names.map(db::RefreshViewsOptions::with_names)
            }
            Err(_) => None,
        }
    } else {
        None
    };

    let result = rt.block_on(async {
        database
            .refresh_views(refresh_options)
            .await
            .map_err(|e| format!("failed to refresh views: {}", e))?;

        Ok::<String, String>("{}".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

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
        let result = unsafe {
            add_view(
                node,
                std::ptr::null(),
                std::ptr::null(),
                view_sdl.as_ptr(),
                std::ptr::null(),
            )
        };
        assert_eq!(result.status, 1, "should fail with null query");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }
}
