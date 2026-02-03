//! Encrypted index operations for FFI.
//!
//! This module provides stub implementations for encrypted index operations.
//! Full implementation is pending.

use std::ffi::c_char;

use crate::types::FfiResult;

/// Create an encrypted index on a collection.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn create_encrypted_index(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _collection_name: *const c_char,
    _field_name: *const c_char,
) -> FfiResult {
    FfiResult::error("encrypted indexes not yet implemented in Rust FFI")
}

/// Delete an encrypted index from a collection.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_encrypted_index(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _collection_name: *const c_char,
    _field_name: *const c_char,
) -> FfiResult {
    FfiResult::error("encrypted indexes not yet implemented in Rust FFI")
}

/// List encrypted indexes for a collection.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn list_encrypted_indexes(
    _node_ptr: usize,
    _identity_did: *const c_char,
    _collection_name: *const c_char,
) -> FfiResult {
    FfiResult::error("encrypted indexes not yet implemented in Rust FFI")
}

/// List all encrypted indexes across all collections.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn list_all_encrypted_indexes(
    _node_ptr: usize,
    _identity_did: *const c_char,
) -> FfiResult {
    FfiResult::error("encrypted indexes not yet implemented in Rust FFI")
}
