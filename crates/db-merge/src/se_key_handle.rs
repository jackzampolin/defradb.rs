//! Lazy SE-key handle shared between the SE options callback (writer side) and
//! the SE query transport (owner/querier side).
//!
//! The CLI knows the SE key at P2P-setup time, but embedded nodes receive it at
//! RUNTIME via `set_se_options` (FFI `set_se_encryption_key` →
//! `ReplicatorPushOptions`). The query transport therefore reads the key
//! LAZILY (at query time) through this handle rather than holding a fixed key.

use std::sync::Arc;

use parking_lot::RwLock;
use zeroize::ZeroizeOnDrop;

/// The SE key material used to generate search tags. `identity_pubkey` MUST
/// match the value used at artifact-generation time (anonymous `None` for the
/// CLI/embedded write side; see #976 trap 3). The 32-byte key is zeroized on
/// drop; `identity_pubkey` is public, non-sensitive data.
#[derive(Clone, ZeroizeOnDrop)]
pub struct SeKeyMaterial {
    pub key: [u8; 32],
    #[zeroize(skip)]
    pub identity_pubkey: Option<Vec<u8>>,
}

impl SeKeyMaterial {
    pub fn new(key: [u8; 32], identity_pubkey: Option<Vec<u8>>) -> Self {
        Self {
            key,
            identity_pubkey,
        }
    }
}

/// Lock-free-ish shared cell for the SE key. `parking_lot::RwLock` is used
/// because `arc_swap` is not a workspace dependency; reads are off the query
/// hot path's critical section (one short read per query), so this is fine.
pub type SeKeyHandle = Arc<RwLock<Option<Arc<SeKeyMaterial>>>>;

/// Create an empty handle (no key provisioned yet). Used by embedded setup,
/// which receives the key at runtime.
pub fn empty_se_key_handle() -> SeKeyHandle {
    Arc::new(RwLock::new(None))
}

/// Create a handle pre-filled with a known key. Used by the CLI, which knows
/// the SE key at P2P-setup time.
pub fn filled_se_key_handle(key: [u8; 32], identity_pubkey: Option<Vec<u8>>) -> SeKeyHandle {
    Arc::new(RwLock::new(Some(Arc::new(SeKeyMaterial::new(
        key,
        identity_pubkey,
    )))))
}

/// Store new key material (or clear it with `None`).
pub fn store_se_key(handle: &SeKeyHandle, material: Option<SeKeyMaterial>) {
    *handle.write() = material.map(Arc::new);
}

/// Load the current key material, if any.
pub fn load_se_key(handle: &SeKeyHandle) -> Option<Arc<SeKeyMaterial>> {
    handle.read().clone()
}
