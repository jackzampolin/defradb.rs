//! Transaction operations for FFI.
//!
//! This module exposes transaction management functions that allow
//! explicit transaction control from Go/C callers.

use std::ffi::c_char;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::query::nac_permission_for_query;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult, NewTxnResult};
use crate::ERR_INVALID_NODE_HANDLE;

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
    let rt = get_runtime!(NewTxnResult);

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
    let rt = get_runtime!(FfiResult);

    let txn_str = match c_str_to_string(txn_id) {
        Some(s) => s,
        None => return FfiResult::error("txn_id is null"),
    };

    // Validate node handle before entering async block
    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let handle: query::txn::TransactionHandle = txn_str
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
    let rt = get_runtime!(FfiResult);

    let txn_str = match c_str_to_string(txn_id) {
        Some(s) => s,
        None => return FfiResult::error("txn_id is null"),
    };

    // Validate node handle before entering async block
    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let handle: query::txn::TransactionHandle = txn_str
            .parse()
            .map_err(|e| format!("invalid transaction ID: {}", e))?;

        runner
            .rollback_txn(&handle)
            .await
            .map_err(|e| format!("failed to rollback transaction: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Execute a GraphQL query or mutation within a transaction.
///
/// The operation will be part of the specified transaction and will not
/// be visible to other transactions until committed.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `txn_id` - Transaction ID from `begin_txn`
/// * `identity_did` - Optional DID of the caller for ACP permission checks (null for anonymous)
/// * `request_query` - GraphQL query string (required)
/// * `operation_name` - Optional operation name (null if not used)
/// * `variables` - Optional JSON string of variables (null if not used)
///
/// # Safety
///
/// All string pointers must be either null or valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn exec_request_in_txn(
    node_ptr: usize,
    txn_id: *const c_char,
    identity_did: *const c_char,
    request_query: *const c_char,
    operation_name: *const c_char,
    variables: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let txn_str = match c_str_to_string(txn_id) {
        Some(s) => s,
        None => return FfiResult::error("txn_id is null"),
    };

    let query_str = match c_str_to_string(request_query) {
        Some(s) => s,
        None => return FfiResult::error("request_query is null"),
    };

    let permission = nac_permission_for_query(&query_str);
    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, permission) {
        return e;
    }

    let identity_str = c_str_to_string(identity_did);
    let op_name = c_str_to_string(operation_name);
    let vars_str = c_str_to_string(variables);

    // Parse identity DID if provided
    let did = match identity_str {
        Some(s) if !s.is_empty() => match identity::Did::new(&s) {
            Ok(d) => Some(d),
            Err(e) => return FfiResult::error(format!("invalid identity DID: {}", e)),
        },
        _ => None,
    };

    // Validate node handle before entering async block
    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let handle: query::txn::TransactionHandle = txn_str
            .parse()
            .map_err(|e| format!("invalid transaction ID: {}", e))?;

        // Build request
        let mut request = query::QueryRequest::new(query_str);
        if did.is_some() {
            request = request.with_identity(did);
        }
        if let Some(op) = op_name {
            request = request.with_operation_name(op);
        }
        if let Some(vars) = vars_str {
            let vars_json: serde_json::Value = serde_json::from_str(&vars)
                .map_err(|e| format!("failed to parse variables: {}", e))?;
            request = request.with_variables(vars_json);
        }

        // Execute in transaction
        let response = runner.execute_in_txn(request, &handle).await;

        // Serialize response
        let json = serde_json::to_string(&response)
            .map_err(|e| format!("failed to serialize response: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
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

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type TxnTest { value: Int }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Begin transaction
        let result = begin_txn(node, 0);
        assert_eq!(result.status, 0, "begin_txn should succeed");
        assert!(!result.txn_id.is_null());

        let txn_id = unsafe { std::ffi::CStr::from_ptr(result.txn_id).to_string_lossy() };
        assert!(!txn_id.is_empty());

        // Execute in transaction
        let mutation =
            CString::new(r#"mutation { create_TxnTest(input: {value: 42}) { _docID value } }"#)
                .unwrap();
        let txn_id_cstr = CString::new(txn_id.as_ref()).unwrap();
        let result = unsafe {
            exec_request_in_txn(
                node,
                txn_id_cstr.as_ptr(),
                std::ptr::null(),
                mutation.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "exec_request_in_txn should succeed");

        // Commit transaction
        let result = unsafe { commit_txn(node, txn_id_cstr.as_ptr()) };
        assert_eq!(result.status, 0, "commit_txn should succeed");

        // Cleanup
        node_close(node);
    }

    #[test]
    fn test_transaction_rollback() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type RollbackTest { value: Int }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Begin transaction
        let result = begin_txn(node, 0);
        assert_eq!(result.status, 0);
        let txn_id = unsafe { std::ffi::CStr::from_ptr(result.txn_id).to_string_lossy() };
        let txn_id_cstr = CString::new(txn_id.as_ref()).unwrap();

        // Execute in transaction
        let mutation =
            CString::new(r#"mutation { create_RollbackTest(input: {value: 99}) { _docID } }"#)
                .unwrap();
        let result = unsafe {
            exec_request_in_txn(
                node,
                txn_id_cstr.as_ptr(),
                std::ptr::null(),
                mutation.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        assert_eq!(result.status, 0);

        // Rollback transaction
        let result = unsafe { rollback_txn(node, txn_id_cstr.as_ptr()) };
        assert_eq!(result.status, 0, "rollback_txn should succeed");

        node_close(node);
    }

    #[test]
    fn test_invalid_txn_id() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Try to commit invalid transaction
        let invalid_txn = CString::new("invalid-txn-id-12345").unwrap();
        let result = unsafe { commit_txn(node, invalid_txn.as_ptr()) };
        assert_eq!(result.status, 1, "commit with invalid txn should fail");

        node_close(node);
    }

    #[test]
    fn test_readonly_transaction() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type ReadOnlyTest { value: Int }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Begin readonly transaction
        let result = begin_txn(node, 1); // readonly = true
        assert_eq!(result.status, 0, "begin readonly txn should succeed");

        let txn_id = unsafe { std::ffi::CStr::from_ptr(result.txn_id).to_string_lossy() };
        let txn_id_cstr = CString::new(txn_id.as_ref()).unwrap();

        // Query should work in readonly transaction
        let query = CString::new("{ ReadOnlyTest { value } }").unwrap();
        let result = unsafe {
            exec_request_in_txn(
                node,
                txn_id_cstr.as_ptr(),
                std::ptr::null(),
                query.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "query in readonly txn should succeed");

        // Commit readonly transaction
        let result = unsafe { commit_txn(node, txn_id_cstr.as_ptr()) };
        assert_eq!(result.status, 0);

        node_close(node);
    }
}
