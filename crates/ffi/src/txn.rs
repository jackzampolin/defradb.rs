//! Transaction operations for FFI.
//!
//! This module exposes transaction management functions that allow
//! explicit transaction control from Go/C callers.
//! Transactions use opaque usize handles via the TxnRegistry.

use std::ffi::c_int;

use crate::get_runtime;
use crate::state::{NODES, TXNS};
use crate::types::{FfiResult, NewTxnResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Create a new transaction.
///
/// Returns a transaction handle that can be used with `transaction_commit`
/// and `transaction_discard`.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `is_concurrent` - If non-zero, creates a concurrent transaction
/// * `is_read_only` - If non-zero, creates a read-only transaction
#[export_name = "TransactionCreate"]
pub extern "C" fn transaction_create(
    node_ptr: usize,
    _is_concurrent: c_int,
    is_read_only: c_int,
) -> NewTxnResult {
    let rt = get_runtime!(NewTxnResult);

    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return NewTxnResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let handle = runner
            .begin_txn(is_read_only != 0)
            .await
            .map_err(|e| format!("failed to begin transaction: {}", e))?;

        Ok::<String, String>(handle.to_string())
    });

    match result {
        Ok(txn_id) => {
            let txn_handle = TXNS.insert(node_ptr, txn_id);
            NewTxnResult::success(txn_handle)
        }
        Err(e) => NewTxnResult::error(e),
    }
}

/// Commit a transaction.
///
/// After commit, all operations performed within the transaction become permanent.
/// The transaction handle is no longer valid after this call.
///
/// # Arguments
///
/// * `txn_ptr` - Transaction handle from `transaction_create`
#[export_name = "TransactionCommit"]
pub extern "C" fn transaction_commit(txn_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let (node_handle, txn_id) = match TXNS.remove(txn_ptr) {
        Some(entry) => entry,
        None => return FfiResult::error("invalid transaction handle"),
    };

    let runner = match NODES.get(node_handle, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let handle: query::txn::TransactionHandle = txn_id
            .parse()
            .map_err(|e| format!("invalid transaction ID: {}", e))?;

        runner
            .commit_txn(&handle)
            .await
            .map_err(|e| format!("failed to commit transaction: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Discard (rollback) a transaction.
///
/// After discard, all operations performed within the transaction are discarded.
/// The transaction handle is no longer valid after this call.
/// This function has no return value (void in Go).
///
/// # Arguments
///
/// * `txn_ptr` - Transaction handle from `transaction_create`
#[export_name = "TransactionDiscard"]
pub extern "C" fn transaction_discard(txn_ptr: usize) {
    let rt = match crate::runtime::RUNTIME.get() {
        Some(rt) => rt,
        None => return,
    };

    let (node_handle, txn_id) = match TXNS.remove(txn_ptr) {
        Some(entry) => entry,
        None => return,
    };

    let runner = match NODES.get(node_handle, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return,
    };

    rt.block_on(async {
        if let Ok(handle) = txn_id.parse::<query::txn::TransactionHandle>() {
            let _ = runner.rollback_txn(&handle).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_transaction_lifecycle() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type TxnTest { value: Int }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);

        // Begin transaction (returns handle now, not string ID)
        let result = transaction_create(node, 0, 0);
        assert_eq!(result.status, 0, "transaction_create should succeed");
        assert_ne!(result.txn_ptr, 0);

        let txn_handle = result.txn_ptr;

        // Commit transaction
        let result = transaction_commit(txn_handle);
        assert_eq!(result.status, 0, "transaction_commit should succeed");

        node_close(node);
    }

    #[test]
    fn test_transaction_discard() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type DiscardTest { value: Int }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);

        // Begin transaction
        let result = transaction_create(node, 0, 0);
        assert_eq!(result.status, 0);
        let txn_handle = result.txn_ptr;

        // Discard transaction (void return)
        transaction_discard(txn_handle);

        node_close(node);
    }

    #[test]
    fn test_invalid_txn_handle() {
        assert!(crate::runtime::init_runtime());

        // Commit with invalid handle should fail
        let result = transaction_commit(999999);
        assert_eq!(result.status, 1, "commit with invalid handle should fail");
    }

    #[test]
    fn test_readonly_transaction() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type ReadOnlyTest { value: Int }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);

        // Begin readonly transaction
        let result = transaction_create(node, 0, 1); // is_read_only = 1
        assert_eq!(result.status, 0, "begin readonly txn should succeed");
        let txn_handle = result.txn_ptr;

        // Commit readonly transaction
        let result = transaction_commit(txn_handle);
        assert_eq!(result.status, 0);

        node_close(node);
    }
}
