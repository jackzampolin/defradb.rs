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
    pub fn success(value: impl Into<String>) -> Self {
        let value_cstring = CString::new(value.into()).unwrap();
        Self {
            status: 0,
            error: ptr::null_mut(),
            value: value_cstring.into_raw(),
        }
    }

    /// Create an error result.
    pub fn error(message: impl Into<String>) -> Self {
        let error_cstring = CString::new(message.into()).unwrap();
        Self {
            status: 1,
            error: error_cstring.into_raw(),
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
    pub fn error(message: impl Into<String>) -> Self {
        let error_cstring = CString::new(message.into()).unwrap();
        Self {
            status: 1,
            error: error_cstring.into_raw(),
            node_ptr: 0,
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
}

impl Default for NodeInitOptions {
    fn default() -> Self {
        Self {
            db_path: ptr::null(),
            in_memory: 1, // Default to in-memory
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
}
