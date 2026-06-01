use crate::ffi_entry;
use crate::helpers::get_rt;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::FfiResult;

use super::{into_ffi_ok, FfiP2PError};

/// Retry pushing existing documents to all registered replicators, and
/// regenerate/re-push their searchable-encryption artifacts.
///
/// Mirrors Go's `RetryReplicators`: an on-demand retry pass over peerstore retry
/// entries that re-pushes failed doc blocks AND the SE artifacts the replicator
/// needs to answer `encrypted_<Collection>` queries (#976).
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

                rt.block_on(async { p2p.system.retry_replicators().await })
                    .map_err(FfiP2PError::internal)
            })
            .ok_or_else(FfiP2PError::invalid_node_handle)
            .and_then(|result| result);

        into_ffi_ok(result)
    }
}
