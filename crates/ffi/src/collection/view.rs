use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::schema::parse_optional_identity_did;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{ffi_async, ffi_entry, try_ffi};
use query::select_to_go_json;

fn parse_view_names(value: &serde_json::Value) -> Option<Vec<String>> {
    value.get("Names").and_then(|n| n.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>()
    })
}

fn parse_view_names_options(options: Option<String>) -> Option<Vec<String>> {
    options.and_then(|opts_str| {
        serde_json::from_str::<serde_json::Value>(&opts_str)
            .ok()
            .and_then(|json| parse_view_names(&json))
    })
}

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
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::ViewAdd
        ));
        let query_str = try_ffi!(require_c_str(gql_query, "gql_query"));
        let sdl_str = try_ffi!(require_c_str(sdl, "sdl"));
        let transform_opt = c_str_to_string(transform);
        let (database, query_limits, document_acp) = match NODES.get(node_ptr, |state| {
            (
                state.database.clone(),
                state.query_limits,
                state.document_acp.clone(),
            )
        }) {
            Some(state) => state,
            None => return FfiResult::error(crate::ERR_INVALID_NODE_HANDLE),
        };
        let (identity_str, creator) = try_ffi!(parse_optional_identity_did(identity_did));

        // Bind the caller's identity into the ambient context so the DB-layer NAC
        // gate on create_collections_atomic resolves the actual caller instead of
        // the wildcard. The body runs on this thread via `block_on`, so the
        // thread-local is visible throughout it; the guard restores on drop.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(identity_str);

        ffi_async!(rt, {
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
            let wrapped_query = format!("query {{ {} }}", query_str);
            let selects = query::parse_query_with_limits(&wrapped_query, None, query_limits)
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
                    if col_version.downsample_interval.is_some() {
                        col_version.downsample_source = Some(query_source.clone());
                    } else {
                        col_version.query = Some(query_source.clone());
                    }
                    col_version
                })
                .collect();

            for collection in &view_collections {
                if collection.downsample_interval.is_some() {
                    database
                        .validate_downsample_collection(collection)
                        .map_err(|e| format!("invalid downsample definition: {}", e))?;
                }
            }

            // Create all view collections atomically (all-or-nothing)
            let created_versions = database
                .create_collections_atomic_with_acp_registration(
                    view_collections,
                    document_acp.clone(),
                    creator,
                )
                .await
                .map_err(|e| format!("failed to create view collection: {}", e))?;

            // Auto-refresh materialized views after creation
            // Exclude embedded-only types (interfaces) - they can't be queried
            let materialized_names: Vec<String> = created_versions
                .iter()
                .filter(|col| col.query.is_some() && col.is_materialized && !col.is_embedded_only)
                .map(|col| col.name.clone())
                .collect();
            let downsample_names: Vec<String> = created_versions
                .iter()
                .filter(|col| col.downsample_interval.is_some())
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

            if !downsample_names.is_empty() {
                database
                    .bootstrap_downsamples(Some(&downsample_names))
                    .await
                    .map_err(|e| format!("failed to bootstrap downsample collections: {}", e))?;
            }

            let json = serde_json::to_string(&created_versions)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
        })
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
pub unsafe extern "C" fn refresh_views(
    node_ptr: usize,
    identity_did: *const c_char,
    options: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::ViewRefresh
        ));
        let database = try_ffi!(get_node_database(node_ptr));

        // Bind the caller's identity so any DB-layer NAC gate reached by the body
        // resolves the actual caller instead of the wildcard.
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let refresh_options =
            parse_view_names_options(c_str_to_string(options)).map(db::RefreshViewsOptions::with_names);

        ffi_async!(rt, {
            database
                .refresh_views(refresh_options)
                .await
                .map_err(|e| format!("failed to refresh views: {}", e))?;

            Ok("{}".to_string())
        })
    }
}

/// Run explicit downsample history GC.
///
/// Applies retention policies to all downsample views matching the given options.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `options` - JSON string with optional "Names" field (null for all downsample views)
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
pub unsafe extern "C" fn gc_downsample_histories(
    node_ptr: usize,
    options: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let database = try_ffi!(get_node_database(node_ptr));
        let gc_options = parse_view_names_options(c_str_to_string(options))
            .map(db::downsample::GcDownsampleHistoriesOptions::with_names);

        ffi_async!(rt, {
            database
                .gc_downsample_histories(gc_options)
                .await
                .map_err(|e| format!("failed to GC downsample histories: {}", e))?;

            Ok("{}".to_string())
        })
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
        let gql_query = CString::new("User { name }").unwrap();
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

    #[test]
    fn test_gc_downsample_histories_empty_ok() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { gc_downsample_histories(node, std::ptr::null()) };
        assert_eq!(result.status, 0, "gc_downsample_histories should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value.as_ref(), "{}");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }
}
