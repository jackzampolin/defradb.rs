//! Defradb-specific Zanzibar store implementations.

// Requires `S: Store + Send + Sync`, which the wasm LevelDB store cannot
// satisfy, so this store is native-only.
#[cfg(not(target_arch = "wasm32"))]
mod persistent;

#[cfg(not(target_arch = "wasm32"))]
pub use persistent::PersistentZanzibarStore;
