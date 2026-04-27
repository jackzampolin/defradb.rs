use crate::ffi_entry;
use crate::state::NODES;
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
        let result = NODES
            .get(node_ptr, |state| {
                if state.p2p.is_none() {
                    return Err(FfiP2PError::no_p2p_system());
                }

                Err(FfiP2PError::unsupported(
                    "retry_replicators is not part of the HTTP P2P operations surface",
                ))
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
    }
}
