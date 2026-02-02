//! NAC (Node Access Control) permission checking for FFI functions.
//!
//! Provides a reusable helper that FFI functions call before executing
//! their operations. When NAC is not configured or disabled, all
//! operations pass through. When enabled, the caller's identity is
//! checked against the required permission.

use std::ffi::c_char;
use std::sync::Arc;

use acp::nac::NodePermission;

use crate::state::{FfiNacManager, NODES};
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Check NAC permission for an FFI operation.
///
/// Returns `Ok(())` if authorized, `Err(FfiResult)` if denied.
///
/// - NAC not configured or disabled -> always Ok
/// - NAC enabled + no identity -> Err (not authorized)
/// - NAC enabled + identity lacks permission -> Err (not authorized)
/// - NAC enabled + identity has permission -> Ok
pub fn check_nac_permission(
    rt: &tokio::runtime::Runtime,
    nac_manager: &Arc<FfiNacManager>,
    identity_did: *const c_char,
    permission: NodePermission,
) -> Result<(), FfiResult> {
    let is_enabled = rt.block_on(nac_manager.is_enabled());
    if !is_enabled {
        return Ok(());
    }

    let identity_str = unsafe { c_str_to_string(identity_did) };

    let did = match identity_str {
        Some(s) if !s.is_empty() => match identity::Did::new(&s) {
            Ok(d) => d,
            Err(_) => return Err(FfiResult::error("not authorized to perform operation")),
        },
        _ => return Err(FfiResult::error("not authorized to perform operation")),
    };

    let has_perm = rt
        .block_on(nac_manager.check_permission(&did, permission))
        .unwrap_or(false);

    if has_perm {
        Ok(())
    } else {
        Err(FfiResult::error("not authorized to perform operation"))
    }
}

/// Convenience: extract NAC manager from node and check permission.
///
/// Returns `Ok(())` if authorized, `Err(FfiResult)` if the node handle
/// is invalid or the caller is not authorized.
pub fn check_nac_for_node(
    rt: &tokio::runtime::Runtime,
    node_ptr: usize,
    identity_did: *const c_char,
    permission: NodePermission,
) -> Result<(), FfiResult> {
    let nac_manager = NODES
        .get(node_ptr, |state| state.nac_manager.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))?;

    check_nac_permission(rt, &nac_manager, identity_did, permission)
}
