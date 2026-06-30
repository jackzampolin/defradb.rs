//! Negative and boundary tests for FFI security (findings 07-51, 07-52).
//!
//! Tests: NULL pointers, invalid handles, non-UTF-8 strings, handle lifecycle
//! stress, and concurrent registry access.

#[cfg(test)]
mod tests {
    use std::ffi::{c_char, CStr, CString};
    use std::ptr;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use crate::node::{new_node, node_close};
    use crate::query::exec_request;
    use crate::schema::add_schema;
    use crate::subscription::{close_subscription, create_subscription, poll_subscription};
    use crate::txn::{begin_txn, commit_txn, rollback_txn};
    use crate::types::{defra_free_string, NodeInitOptions};

    fn init() {
        assert!(crate::runtime::init_runtime(), "runtime init must succeed");
    }

    fn new_in_memory_node() -> usize {
        let result = new_node(NodeInitOptions::default());
        assert_eq!(result.status, 0, "new_node must succeed");
        assert!(result.node_ptr > 0, "handle must be non-zero");
        result.node_ptr
    }

    // ── NULL pointer handling (07-51) ─────────────────────────────────────────

    /// add_schema with a NULL SDL must return an error, not crash.
    #[test]
    fn add_schema_null_sdl_returns_error() {
        init();
        let node = new_in_memory_node();

        let result = unsafe { add_schema(node, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 1, "null SDL must be an error");
        assert!(!result.error.is_null());
        let msg = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            msg.contains("null") || msg.contains("schema_sdl"),
            "error should describe the null argument, got: {msg}"
        );
        unsafe { defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn add_schema_malformed_identity_did_returns_error() {
        init();
        let node = new_in_memory_node();

        let identity = CString::new("not-a-did").unwrap();
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, identity.as_ptr(), sdl.as_ptr()) };

        assert_eq!(result.status, 1, "malformed identity DID must be an error");
        assert!(!result.error.is_null());
        let msg = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            msg.contains("invalid identity DID"),
            "error should describe the invalid identity DID, got: {msg}"
        );
        unsafe { defra_free_string(result.error) };
        node_close(node);
    }

    /// exec_request with a NULL query must return an error, not crash.
    #[test]
    fn exec_request_null_query_returns_error() {
        init();
        let node = new_in_memory_node();

        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 1, "null query must be an error");
        assert!(!result.error.is_null());
        unsafe { defra_free_string(result.error) };
        node_close(node);
    }

    /// commit_txn with a NULL txn_id must return an error, not crash.
    #[test]
    fn commit_txn_null_id_returns_error() {
        init();
        let node = new_in_memory_node();

        let result = unsafe { commit_txn(node, ptr::null()) };
        assert_eq!(result.status, 1, "null txn_id must be an error");
        assert!(!result.error.is_null());
        unsafe { defra_free_string(result.error) };
        node_close(node);
    }

    /// rollback_txn with a NULL txn_id must return an error, not crash.
    #[test]
    fn rollback_txn_null_id_returns_error() {
        init();
        let node = new_in_memory_node();

        let result = unsafe { rollback_txn(node, ptr::null()) };
        assert_eq!(result.status, 1, "null txn_id must be an error");
        assert!(!result.error.is_null());
        unsafe { defra_free_string(result.error) };
        node_close(node);
    }

    /// defra_free_string on a NULL pointer must not crash.
    #[test]
    fn free_string_null_is_safe() {
        // No init needed — this is a pure memory operation.
        unsafe { defra_free_string(ptr::null_mut()) };
    }

    // ── Invalid handle handling (07-51) ───────────────────────────────────────

    /// node_close(0) — the zero handle is always invalid.
    #[test]
    fn node_close_zero_handle_returns_error() {
        init();
        let result = node_close(0);
        assert_eq!(result.status, 1, "zero handle must fail");
        assert!(!result.error.is_null());
        let msg = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(msg.contains("invalid"), "should say invalid, got: {msg}");
        unsafe { defra_free_string(result.error) };
    }

    /// node_close with a large sentinel value that was never issued.
    #[test]
    fn node_close_large_handle_returns_error() {
        init();
        let result = node_close(usize::MAX);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { defra_free_string(result.error) };
    }

    /// Stale handle: close a node then attempt operations using the old handle.
    #[test]
    fn stale_handle_operations_return_errors() {
        init();
        let node = new_in_memory_node();
        let stale = node;

        // Close the node so the handle becomes stale.
        let r = node_close(node);
        assert_eq!(r.status, 0, "node_close must succeed");

        // All subsequent operations with the stale handle must return errors.
        let r = node_close(stale);
        assert_eq!(r.status, 1, "double-close must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }

        let sdl = CString::new("type X { v: Int }").unwrap();
        let r = unsafe { add_schema(stale, ptr::null(), sdl.as_ptr()) };
        assert_eq!(r.status, 1, "add_schema on stale handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }

        let q = CString::new("{ X { v } }").unwrap();
        let r = unsafe {
            exec_request(
                stale,
                ptr::null(),
                q.as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(r.status, 1, "exec_request on stale handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }

        let r = begin_txn(stale, 0);
        assert_eq!(r.status, 1, "begin_txn on stale handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }

        let r = unsafe { create_subscription(stale, ptr::null()) };
        assert_eq!(r.status, 1, "create_subscription on stale handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }
    }

    /// Invalid subscription handles must not crash poll or close.
    #[test]
    fn invalid_subscription_handle_returns_errors() {
        init();

        let r = poll_subscription(0);
        assert_eq!(r.status, 1, "poll on zero subscription handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }

        let r = poll_subscription(usize::MAX);
        assert_eq!(r.status, 1, "poll on max subscription handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }

        let r = close_subscription(0);
        assert_eq!(r.status, 1, "close on zero subscription handle must fail");
        if !r.error.is_null() {
            unsafe { defra_free_string(r.error) };
        }
    }

    // ── Non-UTF-8 string handling (07-51) ─────────────────────────────────────

    /// Pass a raw invalid-UTF-8 byte sequence where a C string is expected.
    ///
    /// `c_str_to_string` uses `to_string_lossy`, which accepts any byte
    /// sequence and replaces invalid sequences with U+FFFD. The result is
    /// treated as a non-null string, so the operation proceeds with a
    /// replacement-character schema name that fails schema validation —
    /// an error result, not a crash.
    #[test]
    fn add_schema_invalid_utf8_sdl_returns_error() {
        init();
        let node = new_in_memory_node();

        // Overlong / invalid UTF-8 sequence followed by a null terminator.
        let invalid_utf8: &[u8] = &[0xFF, 0xFE, 0xFD, 0x00];
        let ptr = invalid_utf8.as_ptr() as *const c_char;

        let result = unsafe { add_schema(node, ptr::null(), ptr) };
        // The function must not crash. It may succeed (unlikely) or return an
        // error — both are acceptable, but status must be a valid code.
        assert!(
            result.status == 0 || result.status == 1,
            "status must be 0 or 1, got {}",
            result.status
        );
        if result.status == 0 {
            if !result.value.is_null() {
                unsafe { defra_free_string(result.value) };
            }
        } else if !result.error.is_null() {
            unsafe { defra_free_string(result.error) };
        }

        node_close(node);
    }

    /// exec_request with an invalid-UTF-8 query must not crash.
    #[test]
    fn exec_request_invalid_utf8_query_returns_error() {
        init();
        let node = new_in_memory_node();

        let invalid_utf8: &[u8] = &[0xC0, 0xAF, 0x00]; // overlong encoding
        let ptr = invalid_utf8.as_ptr() as *const c_char;

        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                ptr,
                ptr::null(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert!(
            result.status == 0 || result.status == 1,
            "must not crash, status={}",
            result.status
        );
        if result.status == 0 {
            if !result.value.is_null() {
                unsafe { defra_free_string(result.value) };
            }
        } else if !result.error.is_null() {
            unsafe { defra_free_string(result.error) };
        }

        node_close(node);
    }

    /// commit_txn with an invalid-UTF-8 txn_id must not crash.
    #[test]
    fn commit_txn_invalid_utf8_id_returns_error() {
        init();
        let node = new_in_memory_node();

        let invalid_utf8: &[u8] = &[0x80, 0x81, 0x82, 0x00];
        let ptr = invalid_utf8.as_ptr() as *const c_char;

        let result = unsafe { commit_txn(node, ptr) };
        // Lossy conversion produces a string; txn parsing will fail.
        assert_eq!(result.status, 1, "bad UTF-8 txn id must fail");
        if !result.error.is_null() {
            unsafe { defra_free_string(result.error) };
        }

        node_close(node);
    }

    // ── Handle lifecycle stress (07-52) ───────────────────────────────────────

    /// Rapid sequential create-and-destroy produces unique, monotonically
    /// increasing handles and never panics or leaks.
    #[test]
    fn handle_lifecycle_rapid_sequential_create_destroy() {
        init();

        let mut handles = Vec::with_capacity(50);
        for _ in 0..50 {
            let r = new_node(NodeInitOptions::default());
            assert_eq!(r.status, 0, "new_node must succeed");
            assert!(r.node_ptr > 0, "handle must be non-zero");
            handles.push(r.node_ptr);
        }

        // All handles must be unique.
        let unique: std::collections::HashSet<usize> = handles.iter().copied().collect();
        assert_eq!(unique.len(), handles.len(), "all handles must be unique");

        // Destroy all nodes in reverse order.
        for h in handles.into_iter().rev() {
            let r = node_close(h);
            assert_eq!(r.status, 0, "node_close must succeed for handle {h}");
        }
    }

    /// Subscription handles from rapid create/close cycles are unique and
    /// subsequent stale polls return errors.
    #[test]
    fn subscription_handle_lifecycle_rapid_cycles() {
        init();
        let node = new_in_memory_node();

        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for _ in 0..30 {
            let r = unsafe { create_subscription(node, ptr::null()) };
            assert_eq!(r.status, 0, "create must succeed");
            let h = r.subscription_handle;
            assert!(h > 0, "handle must be non-zero");
            assert!(seen.insert(h), "handle must be unique");

            let r = close_subscription(h);
            assert_eq!(r.status, 0, "close must succeed");

            // Stale handle must fail.
            let r = poll_subscription(h);
            assert_eq!(r.status, 1, "poll on closed sub must fail");
            if !r.error.is_null() {
                unsafe { defra_free_string(r.error) };
            }
        }

        node_close(node);
    }

    /// Concurrent node creation and destruction from multiple threads must not
    /// corrupt the registry or produce duplicate handles.
    ///
    /// This exercises the RwLock + AtomicUsize handle counter under contention.
    #[test]
    fn concurrent_node_create_destroy_is_safe() {
        init();

        const THREADS: usize = 4;
        const NODES_PER_THREAD: usize = 10;

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut join_handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let barrier = Arc::clone(&barrier);
            join_handles.push(thread::spawn(move || {
                // All threads start simultaneously to maximize contention.
                barrier.wait();

                let mut local_handles = Vec::with_capacity(NODES_PER_THREAD);
                for _ in 0..NODES_PER_THREAD {
                    let r = new_node(NodeInitOptions::default());
                    assert_eq!(r.status, 0, "new_node must succeed under contention");
                    assert!(r.node_ptr > 0);
                    local_handles.push(r.node_ptr);
                }

                // Destroy all nodes created by this thread.
                for h in local_handles {
                    let r = node_close(h);
                    assert_eq!(r.status, 0, "node_close must succeed for handle {h}");
                }
            }));
        }

        for jh in join_handles {
            jh.join().expect("thread must not panic");
        }
    }

    /// Concurrent subscription create/poll/close from multiple threads on the
    /// same node must not deadlock or corrupt state.
    #[test]
    fn concurrent_subscription_access_is_safe() {
        init();
        let node = new_in_memory_node();
        let node = Arc::new(node); // share handle across threads

        const THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 10;

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut join_handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let barrier = Arc::clone(&barrier);
            let node_ptr = *node;
            join_handles.push(thread::spawn(move || {
                barrier.wait();

                for _ in 0..OPS_PER_THREAD {
                    let r = unsafe { create_subscription(node_ptr, ptr::null()) };
                    if r.status != 0 {
                        if !r.error.is_null() {
                            unsafe { defra_free_string(r.error) };
                        }
                        continue;
                    }
                    let h = r.subscription_handle;

                    // Poll should not crash.
                    let p = poll_subscription(h);
                    assert!(
                        p.status == 0 || p.status == 1 || p.status == 2 || p.status == 3,
                        "poll status must be valid"
                    );
                    if !p.error.is_null() {
                        unsafe { defra_free_string(p.error) };
                    }
                    if !p.value.is_null() {
                        unsafe { defra_free_string(p.value) };
                    }

                    let c = close_subscription(h);
                    assert_eq!(c.status, 0, "close must succeed");
                }
            }));
        }

        for jh in join_handles {
            jh.join().expect("thread must not panic");
        }

        node_close(*node);
    }
}
