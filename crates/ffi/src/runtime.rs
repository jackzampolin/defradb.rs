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

/// Initialize the global runtime.
///
/// Safe to call multiple times - only the first call has an effect.
pub fn init_runtime() {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create Tokio runtime")
    });
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
        init_runtime();
        assert!(RUNTIME.get().is_some());
    }

    #[test]
    fn test_runtime_handle() {
        init_runtime();
        let handle = runtime_handle();
        assert!(handle.is_some());

        // Can run async tasks
        let result = handle.unwrap().block_on(async { 42 });
        assert_eq!(result, 42);
    }
}
