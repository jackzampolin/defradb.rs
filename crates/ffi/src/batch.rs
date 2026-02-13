//! FFI batch signing functions.
//!
//! These expose the batch CID collection and signing to Go callers.

use std::ffi::c_char;

use crate::helpers::get_rt;
use crate::state::NODES;
use crate::try_ffi;
use crate::types::{c_str_to_string, FfiResult};

/// Start (or reset) batch CID collection for the node's signing identity.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_did` - Optional DID string (null to use node identity)
///
/// # Safety
///
/// String pointers must be either null or valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn batch_start(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    let _rt = try_ffi!(get_rt());
    let identity_str = c_str_to_string(identity_did);

    let node_did = NODES
        .get(node_ptr, |state| state.node_identity_did.clone())
        .flatten();

    let signing =
        defra_core::signing::resolve_signing_config(identity_str.as_deref(), node_did.as_deref());

    match signing {
        Some(config) => {
            defra_core::batch_signing::batch_start(&config.public_key_hex);
            FfiResult::ok()
        }
        None => FfiResult::error("no signing identity available"),
    }
}

/// Sign all collected batch CIDs and return the batch signature as JSON.
///
/// Returns JSON with fields: sig_type, identity, value (hex), merkle_root (hex), cid_count.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_did` - Optional DID string (null to use node identity)
///
/// # Safety
///
/// String pointers must be either null or valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn batch_sign(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    let _rt = try_ffi!(get_rt());
    let identity_str = c_str_to_string(identity_did);

    let node_did = NODES
        .get(node_ptr, |state| state.node_identity_did.clone())
        .flatten();

    let signing =
        defra_core::signing::resolve_signing_config(identity_str.as_deref(), node_did.as_deref());

    let config = match signing {
        Some(c) => c,
        None => return FfiResult::error("no signing identity available"),
    };

    let cids = match defra_core::batch_signing::batch_take_cids(&config.public_key_hex) {
        Some(c) => c,
        None => return FfiResult::error("no batch session active"),
    };

    if cids.is_empty() {
        return FfiResult::error("batch has no CIDs");
    }

    match crypto::batch::sign_batch(&cids, &config) {
        Ok(sig) => match serde_json::to_string(&sig) {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(format!("failed to serialize batch signature: {}", e)),
        },
        Err(e) => FfiResult::error(format!("batch sign failed: {}", e)),
    }
}
