use std::ffi::c_char;

use crate::helpers::{get_node_runner, get_rt, require_c_str};
use crate::state::NODES;
use crate::types::{FfiResult, NewTxnResult};
use crate::{ffi_async_ok, try_ffi, ERR_INVALID_NODE_HANDLE};

/// Begin a new transaction.
///
/// Returns a transaction ID that can be used with `exec_request_in_txn`,
/// `commit_txn`, and `rollback_txn`.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `readonly` - If non-zero, creates a read-only transaction
///
/// # Returns
///
/// A `NewTxnResult` containing the transaction ID on success.
#[no_mangle]
pub extern "C" fn begin_txn(node_ptr: usize, readonly: i32) -> NewTxnResult {
    let rt = match crate::runtime::RUNTIME.get() {
        Some(rt) => rt,
        None => return NewTxnResult::error("runtime not initialized - call defra_init() first"),
    };

    // Validate node handle before entering async block
    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return NewTxnResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let handle = runner
            .begin_txn(readonly != 0)
            .await
            .map_err(|e| format!("failed to begin transaction: {}", e))?;

        Ok::<String, String>(handle.to_string())
    });

    match result {
        Ok(txn_id) => NewTxnResult::success(txn_id),
        Err(e) => NewTxnResult::error(e),
    }
}

/// Commit a transaction.
///
/// After commit, all operations performed within the transaction become permanent.
/// The transaction ID is no longer valid after this call.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `txn_id` - Transaction ID from `begin_txn`
///
/// # Safety
///
/// `txn_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn commit_txn(node_ptr: usize, txn_id: *const c_char) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let txn_str = try_ffi!(require_c_str(txn_id, "txn_id"));
    let runner = try_ffi!(get_node_runner(node_ptr));

    ffi_async_ok!(rt, {
        let handle: query::txn::TransactionHandle = txn_str
            .parse()
            .map_err(|e| format!("invalid transaction ID: {}", e))?;

        runner
            .commit_txn(&handle)
            .await
            .map_err(|e| format!("failed to commit transaction: {}", e))?;

        Ok(())
    })
}

/// Rollback (discard) a transaction.
///
/// After rollback, all operations performed within the transaction are discarded.
/// The transaction ID is no longer valid after this call.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `txn_id` - Transaction ID from `begin_txn`
///
/// # Safety
///
/// `txn_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn rollback_txn(node_ptr: usize, txn_id: *const c_char) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let txn_str = try_ffi!(require_c_str(txn_id, "txn_id"));
    let runner = try_ffi!(get_node_runner(node_ptr));

    ffi_async_ok!(rt, {
        let handle: query::txn::TransactionHandle = txn_str
            .parse()
            .map_err(|e| format!("invalid transaction ID: {}", e))?;

        runner
            .rollback_txn(&handle)
            .await
            .map_err(|e| format!("failed to rollback transaction: {}", e))?;

        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;

    #[test]
    fn test_invalid_txn_id() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Try to commit invalid transaction
        let invalid_txn = std::ffi::CString::new("invalid-txn-id-12345").unwrap();
        let result = unsafe { commit_txn(node, invalid_txn.as_ptr()) };
        assert_eq!(result.status, 1, "commit with invalid txn should fail");

        node_close(node);
    }
}
