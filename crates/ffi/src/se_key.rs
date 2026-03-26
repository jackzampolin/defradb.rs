//! SE encryption key FFI.
//!
//! Allows Go to pass the searchable encryption key to the Rust FFI node.

use zeroize::Zeroizing;

use crate::ffi_entry;
use crate::state::NODES;
use crate::types::FfiResult;

/// Set the searchable encryption key for a node.
///
/// The key is a 32-byte AES-256 key used by the SE coordinator
/// for HMAC tag generation during artifact creation.
///
/// # Safety
///
/// * `node_ptr` must be a valid node handle
/// * `key_ptr` must point to `key_len` valid bytes
#[no_mangle]
pub unsafe extern "C" fn set_se_encryption_key(
    node_ptr: usize,
    key_ptr: *const u8,
    key_len: usize,
) -> FfiResult {
    ffi_entry! {
        if key_ptr.is_null() || key_len == 0 {
            return FfiResult::error("se encryption key is null or empty");
        }

        if key_len != 32 {
            return FfiResult::error(format!(
                "se encryption key must be 32 bytes, got {}",
                key_len
            ));
        }

        // SAFETY: `key_ptr` is non-null (checked above) and `key_len` is
        // exactly 32 (checked above). The caller guarantees the pointer is
        // valid for 32 bytes.
        let key = Zeroizing::new(std::slice::from_raw_parts(key_ptr, key_len).to_vec());

        let found = NODES.get_mut(node_ptr, |state| {
            state.se_encryption_key = Some(key);
        });

        if found.is_none() {
            return FfiResult::error(crate::ERR_INVALID_NODE_HANDLE);
        }

        FfiResult::ok()
    }
}
