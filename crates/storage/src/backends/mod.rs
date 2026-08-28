//! Storage backends.
//!
//! There is one: regolith. It is the only store on every target DefraDB
//! runs on, so there is nothing here to select between and no feature
//! flag to get wrong.

pub(crate) mod shared;

pub mod regolith;

#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod test_suite;

pub use regolith::{RegolithStore, RegolithStoreOptions, RegolithTxn};
pub use shared::{
    CallbackCounts, DurabilityMode, TransactionStatsHandle, TransactionStatsSnapshot,
};
