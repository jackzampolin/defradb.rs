//! Lean axis: assert the auto-generated Lean contract still matches the live
//! Rust types. A drift in either side fails here. No binary needed.

use conformance::lean_contract::{load_contract, ContractSnapshot};

/// Mirrors `crates/crdt/src/traits.rs` `enum MergeResult`. If a variant is
/// added/renamed there, update this list and `Conformance.lean` together — the
/// assertion is what forces that.
const MERGE_RESULT_VARIANTS: &[&str] = &["Applied", "RejectedLowerPriority", "RejectedTieBreak"];

#[test]
fn lean_merge_result_vocab_matches_rust() {
    let snapshot: ContractSnapshot = load_contract().expect("load Lean conformance contract");
    let vocab = snapshot
        .vocab("MergeResult")
        .expect("MergeResult vocabulary present in Lean contract");
    assert_eq!(
        vocab.values, MERGE_RESULT_VARIANTS,
        "Lean MergeResult vocabulary drifted from Rust crdt::MergeResult variants"
    );
}
