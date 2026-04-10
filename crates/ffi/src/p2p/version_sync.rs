use std::ffi::c_char;

use crate::ffi_entry;
use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{try_ffi, ERR_INVALID_NODE_HANDLE};

/// Sync collection versions (schema definitions) from connected peers.
///
/// # Safety
///
/// `identity_did` and `version_ids_json` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn p2p_sync_collection_versions(
    node_ptr: usize,
    identity_did: *const c_char,
    version_ids_json: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::P2pSyncCollectionVersions
        ));

        let version_ids_str = try_ffi!(require_c_str(version_ids_json, "version_ids_json"));
        let version_ids: Vec<String> = match serde_json::from_str(&version_ids_str) {
            Ok(ids) => ids,
            Err(error) => {
                return FfiResult::error(format!(
                    "failed to parse version_ids_json: {}",
                    error
                ));
            }
        };

        if version_ids.is_empty() {
            return FfiResult::ok();
        }

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err("no p2p system configured".to_string()),
                };

                let version_ids = version_ids.clone();
                rt.block_on(async move {
                    p2p.system
                        .ops()
                        .sync_collection_versions(version_ids)
                        .await
                        .map_err(|error| error.to_string())
                })
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string())
            .and_then(|result| result);

        match result {
            Ok(()) => FfiResult::ok(),
            Err(error) => FfiResult::error(error),
        }
    }
}
