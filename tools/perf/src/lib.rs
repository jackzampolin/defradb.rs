//! The contract between a DefraDB benchmark and the performance dashboard.
//!
//! A bench describes what it measured as an [`emit::Family`]; `collect` folds
//! every family a platform recorded into one platform document; `publish.py`
//! merges the platforms of a commit into the run document the dashboard reads.
//!
//! The contract lives here rather than inside the bench crate so the two sides
//! cannot drift: a bench that emits a family and the collector that reads one
//! are compiled against the same types. It is also why `collect` builds in
//! seconds instead of pulling the database in behind it, which matters when
//! every platform in the matrix runs it.
//!
//! See `tools/perf-site/` for the dashboard those documents are read by.

pub mod emit;

// The browser can describe what it measured, but it cannot read a process's
// peak RSS, name the host it ran on, or walk a criterion directory. Those live
// behind the native gate so a wasm benchmark can link the contract without
// dragging in what its target has no answer for.
#[cfg(not(target_arch = "wasm32"))]
pub mod criterion;
#[cfg(not(target_arch = "wasm32"))]
pub mod measure;
#[cfg(not(target_arch = "wasm32"))]
pub mod run_meta;
