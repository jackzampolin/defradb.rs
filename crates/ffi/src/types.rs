//! FFI type definitions matching Go's cbindings/defra_structs.h

use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;

/// FFI result type matching Go's Result struct.
///
/// Status codes:
/// - 0: Success
/// - 1: Error (message in error field)
/// - 2: Subscription (ID in value field, not yet implemented)
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

    /// Create a subscription result (status=2) with the subscription ID in value.
    pub fn subscription(id: impl Into<String>) -> Self {
        Self {
            status: 2,
            error: ptr::null_mut(),
            value: sanitize_to_cstring(id, "0").into_raw(),
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

/// FFI result for transaction creation, containing a transaction ID.
#[repr(C)]
pub struct NewTxnResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Transaction ID (null on error). Caller must free with `defra_free_string`.
    pub txn_id: *mut c_char,
}

impl NewTxnResult {
    /// Create a success result with a transaction ID.
    pub fn success(txn_id: impl Into<String>) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            txn_id: sanitize_to_cstring(txn_id, "unknown").into_raw(),
        }
    }

    /// Create an error result.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            txn_id: ptr::null_mut(),
        }
    }
}

/// Options for node initialization.
///
/// Matches Go's NodeInitOptions struct.
#[repr(C)]
pub struct NodeInitOptions {
    /// Path to store directory (null for in-memory).
    pub db_path: *const c_char,
    /// Use in-memory storage (1=true, 0=false).
    pub in_memory: c_int,
    /// Enable block signing (1=true, 0=false).
    /// When enabled, the node uses a signing key for block signatures.
    /// If signing_private_key is provided, that key is used.
    /// Otherwise, a random secp256k1 key pair is generated.
    pub enable_signing: c_int,
    /// Optional: signing key type string (e.g. "secp256k1", "ed25519").
    /// Null to auto-generate secp256k1.
    pub signing_key_type: *const c_char,
    /// Optional: raw private key bytes for signing.
    /// Null to auto-generate.
    pub signing_private_key: *const u8,
    /// Length of signing_private_key in bytes. 0 if null.
    pub signing_private_key_len: usize,
    /// SourceHub gRPC/LCD address (null = use local ACP).
    pub sourcehub_grpc_address: *const c_char,
    /// SourceHub CometBFT RPC address.
    pub sourcehub_comet_rpc_address: *const c_char,
    /// SourceHub chain ID (e.g., "sourcehub-test").
    pub sourcehub_chain_id: *const c_char,
    /// SourceHub secp256k1 signer key bytes (raw 32-byte private key).
    pub sourcehub_signer_key: *const u8,
    /// Length of sourcehub_signer_key. 0 if null.
    pub sourcehub_signer_key_len: usize,
}

impl Default for NodeInitOptions {
    fn default() -> Self {
        Self {
            db_path: ptr::null(),
            in_memory: 1, // Default to in-memory
            enable_signing: 0,
            signing_key_type: ptr::null(),
            signing_private_key: ptr::null(),
            signing_private_key_len: 0,
            sourcehub_grpc_address: ptr::null(),
            sourcehub_comet_rpc_address: ptr::null(),
            sourcehub_chain_id: ptr::null(),
            sourcehub_signer_key: ptr::null(),
            sourcehub_signer_key_len: 0,
        }
    }
}

/// Convert a string to a CString, sanitizing null bytes.
///
/// If the string contains embedded null bytes, they are replaced with the
/// Unicode replacement character (`\u{FFFD}`) to avoid panicking at the FFI
/// boundary. If sanitization fails, the fallback string is used.
///
/// # Arguments
///
/// * `value` - The string to convert
/// * `fallback` - Fallback string if conversion fails entirely
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

    // Edge case tests for null byte handling (H2)

    #[test]
    fn test_ffi_result_success_with_null_bytes() {
        // String with embedded null byte should not panic
        let value_with_null = "hello\0world";
        let result = FfiResult::success(value_with_null);
        assert_eq!(result.status, 0);
        assert!(!result.value.is_null());

        // Value should have null bytes replaced
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains('\u{FFFD}'), "null byte should be replaced");
        assert!(!value.contains('\0'), "should not contain null byte");

        unsafe { defra_free_string(result.value) };
    }

    #[test]
    fn test_ffi_result_error_with_null_bytes() {
        // Error message with embedded null byte should not panic
        let error_with_null = "error\0message";
        let result = FfiResult::error(error_with_null);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());

        // Error should have null bytes replaced
        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains('\u{FFFD}'), "null byte should be replaced");
        assert!(!error.contains('\0'), "should not contain null byte");

        unsafe { defra_free_string(result.error) };
    }

    #[test]
    fn test_new_node_result_error_with_null_bytes() {
        // Error message with embedded null byte should not panic
        let error_with_null = "node\0error";
        let result = NewNodeResult::error(error_with_null);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        assert_eq!(result.node_ptr, 0);

        // Error should have null bytes replaced
        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains('\u{FFFD}'), "null byte should be replaced");

        unsafe { defra_free_string(result.error) };
    }

    #[test]
    fn test_c_str_to_string_null_ptr() {
        let result = unsafe { c_str_to_string(ptr::null()) };
        assert!(result.is_none());
    }

    #[test]
    fn test_defra_free_string_null_ptr() {
        // Should not panic when freeing null pointer
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
