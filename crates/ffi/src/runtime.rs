//! Global Tokio runtime for FFI async bridging.
//!
//! FFI functions are synchronous, but the Rust database API is async.
//! This module provides a global Tokio runtime that bridges the gap.

use std::sync::OnceLock;

use tokio::runtime::Runtime;

/// Global Tokio runtime.
///
/// Initialized once on first use. All async operations from FFI run here.
pub static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Error from runtime initialization.
static RUNTIME_ERROR: OnceLock<String> = OnceLock::new();

/// Initialize the global runtime.
///
/// Safe to call multiple times - only the first call has an effect.
/// Returns true on success, false on failure.
/// On failure, call `runtime_init_error()` to get the error message.
pub fn init_runtime() -> bool {
    // If already initialized successfully, return true
    if RUNTIME.get().is_some() {
        return true;
    }

    // If we already had an error, don't retry
    if RUNTIME_ERROR.get().is_some() {
        return false;
    }

    // Try to initialize
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => {
            // Use get_or_init to handle race condition where another thread
            // might have initialized between our check and this call
            RUNTIME.get_or_init(|| rt);
            true
        }
        Err(e) => {
            RUNTIME_ERROR.get_or_init(|| format!("failed to create Tokio runtime: {}", e));
            false
        }
    }
}

/// Get the runtime initialization error, if any.
pub fn runtime_init_error() -> Option<&'static str> {
    RUNTIME_ERROR.get().map(|s| s.as_str())
}

/// Get a handle to the global runtime.
///
/// Returns None if the runtime hasn't been initialized.
pub fn runtime_handle() -> Option<&'static Runtime> {
    RUNTIME.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_init() {
        assert!(init_runtime(), "runtime initialization should succeed");
        assert!(RUNTIME.get().is_some());
    }

    #[test]
    fn test_runtime_handle() {
        assert!(init_runtime(), "runtime initialization should succeed");
        let handle = runtime_handle();
        assert!(handle.is_some());

        // Can run async tasks
        let result = handle.unwrap().block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_runtime_init_idempotent() {
        // Multiple calls should all succeed
        assert!(init_runtime());
        assert!(init_runtime());
        assert!(init_runtime());
        assert!(RUNTIME.get().is_some());
    }
}
