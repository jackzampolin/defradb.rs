use crate::ffi_entry;
use crate::helpers::get_rt;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::FfiResult;

use super::{into_ffi_ok, FfiP2PError};

/// Retry pushing existing documents to all registered replicators.
///
/// # Safety
///
/// `node_ptr` must be a valid node handle.
#[no_mangle]
pub unsafe extern "C" fn p2p_retry_replicators(node_ptr: usize) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());

        let result = NODES
            .get(node_ptr, |state| {
                let p2p = match &state.p2p {
                    Some(p2p) => p2p,
                    None => return Err(FfiP2PError::no_p2p_system()),
                };
                let push_options = embedded::ReplicatorPushOptions {
                    se_encryption_key: state.se_encryption_key.as_ref().map(|key| key.to_vec()),
                    se_identity_pubkey: state
                        .node_identity_did
                        .as_ref()
                        .map(|identity| identity.as_bytes().to_vec()),
                };

                rt.block_on(async {
                    p2p.system
                        .ops()
                        .retry_replicators(push_options)
                        .await
                        .map_err(FfiP2PError::from)
                })
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
    }
}
