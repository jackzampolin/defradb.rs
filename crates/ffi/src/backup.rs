//! Backup (import/export) operations for FFI.

use std::ffi::c_char;

use crate::types::FfiResult;

/// Export the database to a JSON backup file.
///
/// # Safety
///
/// `config_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn basic_export(_node_ptr: usize, _config_json: *const c_char) -> FfiResult {
    FfiResult::error("basic_export not yet implemented")
}

/// Import documents from a JSON backup file.
///
/// # Safety
///
/// `filepath` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn basic_import(_node_ptr: usize, _filepath: *const c_char) -> FfiResult {
    FfiResult::error("basic_import not yet implemented")
}
