//! Block operations for FFI.
//!
//! This module exposes block-level operations like signature verification.

use std::ffi::c_char;

use crate::state::NODES;
use crate::types::FfiResult;
use crate::ERR_INVALID_NODE_HANDLE;

/// Verify a block's signature.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `key_type` - Crypto key type (null/empty for default Secp256k1)
/// * `public_key` - Public key as string
/// * `cid` - Content ID to verify signature for
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "BlockVerifySignature"]
pub unsafe extern "C" fn block_verify_signature(
    node_ptr: usize,
    _key_type: *const c_char,
    _public_key: *const c_char,
    _cid: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    if NODES.get(node_ptr, |_| ()).is_none() {
        return FfiResult::error(ERR_INVALID_NODE_HANDLE);
    }

    FfiResult::error("block signature verification not yet implemented in Rust")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_verify_signature_invalid_handle() {
        assert!(crate::runtime::init_runtime());

        let result = unsafe {
            block_verify_signature(0, std::ptr::null(), std::ptr::null(), std::ptr::null(), 0)
        };
        assert_eq!(result.status, 1);
        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("invalid"), "should indicate invalid handle");
        unsafe { crate::types::defra_free_string(result.error) };
    }
}
