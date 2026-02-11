use std::ffi::c_char;
use std::sync::Arc;

use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Get the global Tokio runtime or return an FfiResult error.
pub fn get_rt() -> Result<&'static tokio::runtime::Runtime, FfiResult> {
    crate::runtime::RUNTIME
        .get()
        .ok_or_else(|| FfiResult::error("runtime not initialized - call defra_init() first"))
}

/// Parse a required C string, returning FfiResult error if null.
///
/// # Safety
/// `ptr` must be null or a valid null-terminated UTF-8 string.
pub unsafe fn require_c_str(ptr: *const c_char, name: &str) -> Result<String, FfiResult> {
    c_str_to_string(ptr).ok_or_else(|| FfiResult::error(format!("{} is null", name)))
}

/// Extract the database handle from node state.
pub fn get_node_database(node_ptr: usize) -> Result<Arc<crate::state::FfiDatabase>, FfiResult> {
    NODES
        .get(node_ptr, |state| state.database.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))
}

/// Extract the query runner from node state.
pub fn get_node_runner(node_ptr: usize) -> Result<Arc<dyn query::QueryExecutor>, FfiResult> {
    NODES
        .get(node_ptr, |state| state.query_runner.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))
}
