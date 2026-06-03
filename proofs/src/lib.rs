//! Conformance: binds the formal models in `proofs/tla/` and `proofs/lean/` to
//! the real defradb.rs release binary.
//!
//! Two axes, matching the two proof tools:
//! - **Lean axis (auto):** `proofs/lean/Conformance.lean` emits a JSON contract
//!   that the live Rust types are asserted against — anti-drift, no binary.
//!   See [`lean_contract`] and `tests/lean_conformance.rs`.
//! - **TLA axis (behavioral):** each family's invariant is driven against the
//!   running release binary via the backbone `defra-harness`.
//!   See `tests/tla_conformance.rs`.
//!
//! [`registry::PROPERTIES`] is the spine; [`matrix`] asserts every modeled
//! family is bound so a model cannot land without a conformance hook.

pub mod lean_contract;
pub mod matrix;
pub mod registry;
