//! FFI type definitions matching Go's cbindings/defra_structs.h

use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;

use identity::Identity;

/// FFI result type matching Go's Result struct.
///
/// Status codes:
/// - 0: Success
/// - 1: Error (message in error field)
/// - 2: Subscription (ID in value field)
#[repr(C)]
pub struct FfiResult {
    /// Status code: 0=success, 1=error, 2=subscription
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// JSON value (null on error). Caller must free with `defra_free_string`.
    pub value: *mut c_char,
}

impl FfiResult {
    /// Create a success result with a JSON value.
    ///
    /// If the value contains null bytes, they are replaced with the Unicode
    /// replacement character to avoid panicking at the FFI boundary.
    pub fn success(value: impl Into<String>) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            value: sanitize_to_cstring(value, "{}").into_raw(),
        }
    }

    /// Create an error result.
    ///
    /// If the message contains null bytes, they are replaced with the Unicode
    /// replacement character to avoid panicking at the FFI boundary.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            value: ptr::null_mut(),
        }
    }

    /// Create a success result with no value.
    pub fn ok() -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            value: ptr::null_mut(),
        }
    }
}

/// FFI result for node creation, containing a node handle.
///
/// Matches Go's NewNodeResult struct.
#[repr(C)]
pub struct NewNodeResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Handle to the node (0 on error).
    pub node_ptr: usize,
}

impl NewNodeResult {
    /// Create a success result with a node handle.
    pub fn success(handle: usize) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            node_ptr: handle,
        }
    }

    /// Create an error result.
    ///
    /// If the message contains null bytes, they are replaced with the Unicode
    /// replacement character to avoid panicking at the FFI boundary.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            node_ptr: 0,
        }
    }
}

/// FFI result for transaction creation, containing a transaction handle.
///
/// Matches Go's NewTxnResult struct. The txn_ptr is an opaque handle
/// into the TxnRegistry, not a string ID.
#[repr(C)]
pub struct NewTxnResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Transaction handle (0 on error).
    pub txn_ptr: usize,
}

impl NewTxnResult {
    /// Create a success result with a transaction handle.
    pub fn success(handle: usize) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            txn_ptr: handle,
        }
    }

    /// Create an error result.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            txn_ptr: 0,
        }
    }
}

/// FFI result for identity creation, containing an identity handle.
///
/// Matches Go's NewIdentityResult struct.
#[repr(C)]
pub struct NewIdentityResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Handle to the identity (0 on error).
    pub identity_ptr: usize,
}

impl NewIdentityResult {
    /// Create a success result with an identity handle.
    pub fn success(handle: usize) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            identity_ptr: handle,
        }
    }

    /// Create an error result.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            identity_ptr: 0,
        }
    }
}

/// Options for node initialization.
///
/// Matches Go's NodeInitOptions struct (10 fields).
#[repr(C)]
pub struct NodeInitOptions {
    /// Path to store directory (null for in-memory).
    pub db_path: *const c_char,
    /// Comma-separated listening addresses for P2P (null to disable).
    pub listening_addresses: *const c_char,
    /// Comma-separated replicator retry intervals (null for defaults).
    pub replicator_retry_intervals: *const c_char,
    /// Comma-separated peer addresses to connect to on startup.
    pub peers: *const c_char,
    /// Identity handle (0 for no identity).
    pub identity_ptr: usize,
    /// Use in-memory storage (1=true, 0=false).
    pub in_memory: c_int,
    /// Disable P2P networking (1=true, 0=false).
    pub disable_p2p: c_int,
    /// Disable the HTTP API server (1=true, 0=false).
    pub disable_api: c_int,
    /// Enable node-level access control (1=true, 0=false).
    pub enable_node_acp: c_int,
    /// Maximum number of transaction retries (0 for default).
    pub max_transaction_retries: c_int,
}

impl Default for NodeInitOptions {
    fn default() -> Self {
        Self {
            db_path: ptr::null(),
            listening_addresses: ptr::null(),
            replicator_retry_intervals: ptr::null(),
            peers: ptr::null(),
            identity_ptr: 0,
            in_memory: 1, // Default to in-memory
            disable_p2p: 1,
            disable_api: 1,
            enable_node_acp: 0,
            max_transaction_retries: 0,
        }
    }
}

/// Options for resolving a collection.
///
/// Matches Go's CollectionOptions struct. Provides multiple ways to
/// identify a collection: by name, version, or collection_id.
#[repr(C)]
pub struct CollectionOptions {
    /// Collection version string (null if not specified).
    pub version: *const c_char,
    /// Collection ID string (null if not specified).
    pub collection_id: *const c_char,
    /// Collection name (null if not specified).
    pub name: *const c_char,
    /// Whether to include inactive collections (1=true, 0=false).
    pub get_inactive: c_int,
}

impl CollectionOptions {
    /// Extract the collection name as a Rust String, if present.
    ///
    /// # Safety
    ///
    /// The `name` pointer must be null or point to a valid null-terminated string.
    pub unsafe fn name_str(&self) -> Option<String> {
        c_str_to_string(self.name)
    }

    /// Extract the version as a Rust String, if present.
    ///
    /// # Safety
    ///
    /// The `version` pointer must be null or point to a valid null-terminated string.
    pub unsafe fn version_str(&self) -> Option<String> {
        c_str_to_string(self.version)
    }

    /// Extract the collection_id as a Rust String, if present.
    ///
    /// # Safety
    ///
    /// The `collection_id` pointer must be null or point to a valid null-terminated string.
    pub unsafe fn collection_id_str(&self) -> Option<String> {
        c_str_to_string(self.collection_id)
    }
}

/// Convert a string to a CString, sanitizing null bytes.
///
/// If the string contains embedded null bytes, they are replaced with the
/// Unicode replacement character (`\u{FFFD}`) to avoid panicking at the FFI
/// boundary. If sanitization fails, the fallback string is used.
pub fn sanitize_to_cstring(value: impl Into<String>, fallback: &str) -> CString {
    let s = value.into();
    match CString::new(s.clone()) {
        Ok(cstring) => cstring,
        Err(_) => {
            let sanitized = s.replace('\0', "\u{FFFD}");
            CString::new(sanitized).unwrap_or_else(|_| {
                CString::new(fallback).unwrap_or_else(|_| CString::new("error").unwrap())
            })
        }
    }
}

/// Convert a C string pointer to a Rust String (or None if null).
///
/// # Safety
///
/// The pointer must be null or point to a valid null-terminated string.
pub unsafe fn c_str_to_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

/// Free a string allocated by FFI functions.
///
/// # Safety
///
/// The pointer must have been allocated by an FFI function in this crate.
#[no_mangle]
pub unsafe extern "C" fn defra_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

/// Resolve a collection from CollectionOptions.
///
/// Tries name first, then version_id, then collection_id. Returns an error
/// if no collection can be found.
///
/// # Safety
///
/// String pointers in `opts` must be null or valid null-terminated UTF-8 strings.
pub unsafe fn resolve_collection(
    database: &std::sync::Arc<crate::state::FfiDatabase>,
    opts: &CollectionOptions,
) -> Result<db::Collection, String> {
    // Try by name first
    if let Some(name) = opts.name_str() {
        if !name.is_empty() {
            return database
                .get_collection(&name)
                .map_err(|e| format!("failed to get collection: {}", e))?
                .ok_or_else(|| format!("collection '{}' not found", name));
        }
    }

    // Try by version_id
    if let Some(version) = opts.version_str() {
        if !version.is_empty() {
            return database
                .get_collection_by_version_id(&version)
                .map_err(|e| format!("failed to get collection by version: {}", e))?
                .ok_or_else(|| format!("collection with version '{}' not found", version));
        }
    }

    // Try by collection_id
    if let Some(col_id) = opts.collection_id_str() {
        if !col_id.is_empty() {
            return database
                .find_collection_by_id(&col_id)
                .map_err(|e| format!("failed to find collection: {}", e))?
                .ok_or_else(|| format!("collection with ID '{}' not found", col_id));
        }
    }

    Err("no collection identifier provided in options".to_string())
}

/// Resolve an identity DID string from an identity handle.
///
/// Returns the DID string for the identity, or an error if the handle is invalid.
pub fn resolve_identity_did(identity_ptr: usize) -> Result<String, String> {
    if identity_ptr == 0 {
        return Err("identity_ptr is 0 (no identity)".to_string());
    }

    let identity = crate::state::IDENTITIES
        .get(identity_ptr)
        .ok_or_else(|| format!("invalid identity handle: {}", identity_ptr))?;

    let did = identity
        .did()
        .map_err(|e| format!("failed to get DID: {}", e))?;

    Ok(did.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ffi_result_success() {
        let result = FfiResult::success(r#"{"data": "test"}"#);
        assert_eq!(result.status, 0);
        assert!(result.error.is_null());
        assert!(!result.value.is_null());

        // Clean up
        unsafe { defra_free_string(result.value) };
    }

    #[test]
    fn test_ffi_result_error() {
        let result = FfiResult::error("something went wrong");
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        assert!(result.value.is_null());

        // Clean up
        unsafe { defra_free_string(result.error) };
    }

    #[test]
    fn test_new_node_result() {
        let result = NewNodeResult::success(42);
        assert_eq!(result.status, 0);
        assert_eq!(result.node_ptr, 42);
        assert!(result.error.is_null());
    }

    #[test]
    fn test_ffi_result_success_with_null_bytes() {
        let value_with_null = "hello\0world";
        let result = FfiResult::success(value_with_null);
        assert_eq!(result.status, 0);
        assert!(!result.value.is_null());

        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains('\u{FFFD}'), "null byte should be replaced");
        assert!(!value.contains('\0'), "should not contain null byte");

        unsafe { defra_free_string(result.value) };
    }

    #[test]
    fn test_ffi_result_error_with_null_bytes() {
        let error_with_null = "error\0message";
        let result = FfiResult::error(error_with_null);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());

        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains('\u{FFFD}'), "null byte should be replaced");
        assert!(!error.contains('\0'), "should not contain null byte");

        unsafe { defra_free_string(result.error) };
    }

    #[test]
    fn test_new_node_result_error_with_null_bytes() {
        let error_with_null = "node\0error";
        let result = NewNodeResult::error(error_with_null);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        assert_eq!(result.node_ptr, 0);

        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains('\u{FFFD}'), "null byte should be replaced");

        unsafe { defra_free_string(result.error) };
    }

    #[test]
    fn test_new_txn_result() {
        let result = NewTxnResult::success(42);
        assert_eq!(result.status, 0);
        assert_eq!(result.txn_ptr, 42);
        assert!(result.error.is_null());
    }

    #[test]
    fn test_c_str_to_string_null_ptr() {
        let result = unsafe { c_str_to_string(ptr::null()) };
        assert!(result.is_none());
    }

    #[test]
    fn test_defra_free_string_null_ptr() {
        unsafe { defra_free_string(ptr::null_mut()) };
    }

    #[test]
    fn test_ffi_result_ok() {
        let result = FfiResult::ok();
        assert_eq!(result.status, 0);
        assert!(result.error.is_null());
        assert!(result.value.is_null());
    }
}
