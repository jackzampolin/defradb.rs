//! Helpers shared by more than one benchmark target.
//!
//! Only helpers that are byte-identical at every call site live here. A helper
//! that differs between targets stays with its target: for tokio runtimes in
//! particular, the builder configuration is part of what a benchmark measures,
//! and four of the six variants in this suite are deliberately unique to their
//! target.

#![allow(dead_code)]

pub mod sift;
pub mod vector;

use std::sync::OnceLock;

use tokio::runtime::Runtime;

/// Built once per process and reused. `Runtime::new()` is multi-threaded across
/// all available cores.
pub fn shared_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

/// A fresh multi-threaded runtime per call, for targets that build one outside
/// the measured region and hand it to criterion.
pub fn owned_runtime() -> Runtime {
    Runtime::new().expect("a tokio runtime")
}
