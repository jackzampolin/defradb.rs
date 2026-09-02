//! Query execution for FFI.
//!
//! This module exposes GraphQL query execution that matches
//! Go's cbindings/query.go behavior.

mod exec;
mod subscription;

#[cfg(test)]
mod tests;

use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::state::NODES;
use crate::types::c_str_to_string;

// Re-export all public items so external imports remain unchanged.
pub use exec::{exec_request, exec_request_with_signing};
pub(crate) use subscription::{
    subscription_accepts_doc_id, subscription_doc_ids, subscription_to_scoped_query,
};

/// Determine NAC permission based on query content.
/// Delete mutations require DocumentDelete, other mutations require DocumentUpdate,
/// and queries require CollectionGet (matching Go's cbindings/query.go).
pub(crate) fn nac_permission_for_query(query_str: &str) -> NodePermission {
    let trimmed = query_str.trim_start();
    if trimmed.starts_with("mutation") {
        if let Some(brace_pos) = trimmed.find('{') {
            let after_brace = trimmed[brace_pos + 1..].trim_start();
            if after_brace.starts_with("delete_") {
                return NodePermission::DocumentDelete;
            }
        }
        NodePermission::DocumentUpdate
    } else {
        NodePermission::CollectionGet
    }
}

/// Check if the identity has DAC bypass permission (NAC admin/owner).
///
/// Sets the thread-local `dac_bypass` flag when the identity has the
/// `DacBypass` NAC permission.
pub(crate) fn check_and_set_dac_bypass(
    rt: &tokio::runtime::Runtime,
    node_ptr: usize,
    identity_did: *const c_char,
) {
    defra_core::dac_bypass::set_dac_bypass(false);

    // SAFETY: `identity_did` is either null or a valid C string from the FFI caller.
    let identity_str = unsafe { c_str_to_string(identity_did) };
    let did = match identity_str {
        Some(s) if !s.is_empty() => match identity::Did::new(&s) {
            Ok(d) => Some(d),
            Err(_) => return,
        },
        _ => None,
    };

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return,
    };

    let status = rt.block_on(nac_manager.status());
    let bypass = rt.block_on(acp::nac::should_bypass_dac(
        status,
        did.as_ref(),
        |d, p| async move { nac_manager.check_permission(d, p).await.unwrap_or(false) },
    ));

    defra_core::dac_bypass::set_dac_bypass(bypass);
}
