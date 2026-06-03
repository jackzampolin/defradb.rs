//! The conformance registry: every modeled family's headline property, the
//! model that proves it, the source it is anchored to, and how it is bound to
//! the real binary.
//!
//! This table is the spine. `matrix::every_modeled_family_is_bound` asserts that
//! each family in `proofs/README.md`'s *Modeled* list appears here, so a new
//! model cannot land without declaring how it is kept honest against the code.

/// Which proof tool establishes the property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Axis {
    /// TLA+ / TLC temporal-safety model (`proofs/tla/`).
    Tla,
    /// Lean 4 functional/algebraic proof (`proofs/lean/`).
    Lean,
}

/// How a property is checked for conformance with the implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Exercised against the running release binary (`tests/tla_conformance.rs`).
    Behavioral,
    /// A model vocabulary/contract asserted against the live Rust types
    /// (`tests/lean_conformance.rs`) — anti-drift, no binary needed.
    Contract,
    /// An assumed boundary (crypto / connectivity / bounded-N / foreign
    /// substrate): surfaced, never asserted, so a green matrix never reads as
    /// "this was proven against the artifact."
    Boundary,
}

pub struct Property {
    /// Family name — must match a row in `proofs/README.md`'s Modeled table.
    pub family: &'static str,
    /// The headline invariant or theorem.
    pub name: &'static str,
    pub axis: Axis,
    /// `file:symbol` the model is grounded to.
    pub anchor: &'static str,
    /// The TLC config (`*.cfg`) or Lean theorem that establishes it.
    pub model_ref: &'static str,
    /// How this property is bound to the implementation.
    pub tiers: &'static [Tier],
}

use Axis::{Lean, Tla};
use Tier::{Behavioral, Boundary, Contract};

pub const PROPERTIES: &[Property] = &[
    Property {
        family: "B3 filtered replication",
        name: "INV_DagComplete — filtered push still converges per-document",
        axis: Tla,
        anchor: "crates/p2p filtered replication; crates/db-merge",
        model_ref: "MC_S4_ModelB.cfg / M1Convergence.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "DAG convergence (partition / eviction / restart)",
        name: "INV_Converged — every node receives every delta under eventual connectivity",
        axis: Tla,
        anchor: "crates/db-merge merge handler; crates/blockstore",
        model_ref: "MC_Conv_Eventual.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "CRDT merge laws",
        name: "lww/counter merge commutative+associative+idempotent",
        axis: Lean,
        anchor: "crates/crdt/src/lww.rs set_value; crates/crdt/src/traits.rs MergeResult",
        model_ref: "DefraConvergence (lake build)",
        tiers: &[Contract, Behavioral],
    },
    Property {
        family: "Replicator lifecycle (no-loss / resume)",
        name: "INV_NoLoss — reconnect recomputes the target gap, no block dropped",
        axis: Tla,
        anchor: "crates/p2p/src/replicator.rs; crates/db-merge/src/push_docs_transport.rs",
        model_ref: "MC_Replicator_Resumable_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "Multi-instance claim",
        name: "INV_EventualUnique — claim CAS converges to a single winner",
        axis: Tla,
        anchor: "defra-agent claim.rs (foreign substrate, not the defradb.rs binary)",
        model_ref: "MC_Claim_Filtered_Eventual.cfg",
        tiers: &[Boundary],
    },
    Property {
        family: "Block integrity / signatures",
        name: "INV_OnlyVerifiedMerged — verify-before-merge, author bound to verified DID",
        axis: Tla,
        anchor: "crates/db-merge sig verify; crates/defra-core/src/batch_signing.rs",
        model_ref: "MC_Integrity_Green.cfg",
        tiers: &[Behavioral, Boundary],
    },
    Property {
        family: "KMS key distribution",
        name: "INV_AuthorizedEventuallyGets / no-unauthorized-usable",
        axis: Tla,
        anchor: "crates/kms PubsubKeyTransport",
        model_ref: "MC_Kms_Green.cfg",
        tiers: &[Behavioral, Boundary],
    },
    Property {
        family: "Management-channel auth (NAC gate)",
        name: "INV_OnlyAuthorizedManages — management ops require a valid scoped token",
        axis: Tla,
        anchor: "crates/http manage channel; crates/db-nac",
        model_ref: "MC_Auth_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "ACP soundness + revocation + dual-path commits",
        name: "INV_RevocationConsistent + both User and _commits paths gated",
        axis: Tla,
        anchor: "crates/acp; crates/zanzibar; crates/query dag-scan",
        model_ref: "MC_Acp_Green.cfg / MC_Commits_Green.cfg",
        tiers: &[Behavioral, Contract],
    },
    Property {
        family: "Storage SSI serializability (point + range/scan carve-out)",
        name: "INV_Serializable — MVSG acyclicity (no write-skew)",
        axis: Tla,
        anchor: "crates/storage ConflictTracker",
        model_ref: "MC_Ssi_Green.cfg / MC_SsiRange_Green_Correct.cfg",
        tiers: &[Behavioral, Boundary],
    },
    Property {
        family: "P2P explicit-replay capability gate",
        name: "INV_OnlyLegitAccepted — forged/expired/wrong-target capability rejected",
        axis: Tla,
        anchor: "crates/p2p capability replay gate",
        model_ref: "MC_Capability_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "NAC lifecycle privilege-escalation",
        name: "INV_NoPrivEsc — no escalation across enable/disable/restart",
        axis: Tla,
        anchor: "crates/db-nac lifecycle",
        model_ref: "MC_Nac_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "Transaction & merge-queue concurrency",
        name: "INV_SameDocSerialized — per-doc merge serialized, no loss/double-apply",
        axis: Tla,
        anchor: "crates/db txn registry; crates/db-merge merge queue",
        model_ref: "MC_MergeQueue_Green.cfg / MC_TxnRegistry_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "JWT issuer / algorithm binding",
        name: "INV_TokenBindsGenuineDid — alg/iss/sig bound to did(pubkey)",
        axis: Tla,
        anchor: "crates/identity token verification",
        model_ref: "MC_Jwt_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "CID content-addressing determinism + Block canonicalization",
        name: "cid_injective_mod_hash — same normal-form content yields same CID",
        axis: Lean,
        anchor: "crates/defra-core/src/block.rs Block::generate_cid",
        model_ref: "Cid (lake build)",
        tiers: &[Behavioral, Contract],
    },
    Property {
        family: "Deferred-ACP overlay consistency",
        name: "INV_FailClosedActive — txn-local ACP projection gates as committed state would",
        axis: Tla,
        anchor: "crates/query-plan deferred-acp overlay",
        model_ref: "MC_DeferredAcp_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        // Lean marker values (mNull=0..mIntMax=253) verified to match the live
        // `crates/storage/src/encoding/mod.rs` constants by inspection, but those
        // are `pub(crate)` (no static assert without widening prod visibility) and
        // a plain ordered query may sort in memory rather than exercise the index
        // key encoding — so neither tier is cleanly realizable. Boundary.
        family: "Order-preserving key encoding",
        name: "asc_strictly_order_preserving_* — encoding is a strict-order embedding",
        axis: Lean,
        anchor: "crates/storage/src/encoding/mod.rs type-marker constants",
        model_ref: "OrderEncoding (lake build)",
        tiers: &[Boundary],
    },
    Property {
        family: "Index-maintenance consistency",
        name: "onDocumentUpdate_correct / _no_stale / _none_missing",
        axis: Lean,
        anchor: "crates/db-index index maintenance",
        model_ref: "IndexMaintenance (lake build)",
        tiers: &[Behavioral, Contract],
    },
];
