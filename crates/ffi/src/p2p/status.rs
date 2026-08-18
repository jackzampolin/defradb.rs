use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::ffi_entry;
use crate::helpers::get_rt;
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::FfiResult;

use super::{into_ffi_result, FfiP2PError};

/// Return the node's live P2P sync status as JSON.
///
/// # Safety
///
/// `identity_did` must be a valid null-terminated UTF-8 string when non-null.
/// `node_ptr` must reference a live node handle created by this library.
#[no_mangle]
pub unsafe extern "C" fn p2p_sync_status(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pPeerInfo
        ));

        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|s| !s.is_empty()),
        );

        let result = NODES
            .get(node_ptr, |state| state.p2p.clone())
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|p2p| p2p.ok_or_else(FfiP2PError::no_p2p_system))
            .and_then(|p2p| {
                rt.block_on(async {
                    let status = p2p
                        .system
                        .ops()
                        .sync_status()
                        .await
                        .map_err(FfiP2PError::from)?;
                    serde_json::to_string(&status).map_err(|error| {
                        FfiP2PError::internal(format!(
                            "failed to serialize P2P sync status: {}",
                            error
                        ))
                    })
                })
            });

        into_ffi_result(result)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr;

    use crate::node::{new_node, node_close};
    use crate::p2p::new_node_with_p2p;
    use crate::types::{defra_free_string, NodeInitOptions};

    use super::*;

    #[test]
    fn p2p_sync_status_exports_peer_info_style_ffi_signature() {
        let symbol: unsafe extern "C" fn(usize, *const c_char) -> FfiResult = p2p_sync_status;
        let _ = symbol;
    }

    #[test]
    fn p2p_sync_status_returns_live_snapshot() {
        assert!(crate::runtime::init_runtime(), "runtime init must succeed");
        let listen_addr = CString::new("/ip4/127.0.0.1/tcp/0").unwrap();
        let result = unsafe { new_node_with_p2p(NodeInitOptions::default(), listen_addr.as_ptr()) };
        assert_eq!(result.status, 0, "P2P node creation must succeed");

        let status = unsafe { p2p_sync_status(result.node_ptr, ptr::null()) };
        assert_eq!(status.status, 0, "sync status lookup must succeed");
        let value: serde_json::Value = unsafe {
            serde_json::from_str(&CStr::from_ptr(status.value).to_string_lossy()).unwrap()
        };
        assert!(value.get("push_backlog").is_some());

        unsafe { defra_free_string(status.value) };
        node_close(result.node_ptr);
    }

    #[test]
    fn p2p_sync_status_requires_p2p() {
        assert!(crate::runtime::init_runtime(), "runtime init must succeed");
        let result = new_node(NodeInitOptions::default());
        assert_eq!(result.status, 0, "node creation must succeed");

        let status = unsafe { p2p_sync_status(result.node_ptr, ptr::null()) };
        assert_eq!(status.status, 1);
        assert!(!status.error.is_null());

        unsafe { defra_free_string(status.error) };
        node_close(result.node_ptr);
    }
}
