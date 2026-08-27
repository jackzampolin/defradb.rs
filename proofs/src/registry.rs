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
        anchor: "crates/p2p filtered replication; crates/db (merge)",
        model_ref: "MC_S4_ModelB.cfg / M1Convergence.cfg",
        tiers: &[Behavioral],
    },
    Property {
        // Two behavioral legs: live-forward convergence (replication.rs) and
        // convergence across a real partition (partition.rs) — node1 is restarted
        // to sever the link, each side writes independently, and both converge to
        // hold every write once reconnected.
        family: "DAG convergence (partition / eviction / restart)",
        name: "INV_Converged — every node receives every delta under eventual connectivity",
        axis: Tla,
        anchor: "crates/db (merge) merge handler; crates/blockstore",
        model_ref: "MC_Conv_Eventual.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "CRDT merge laws",
        // Generic CrdtField core (DefraConvergence.CrdtField): comm+assoc => the merge
        // fold is order-independent (every field). Idempotence is the dividing line:
        // LWW (lwwMerge) is idempotent => a join-semilattice, re-delivery-safe, no dedup
        // (lww_dup_safe); the counter (Int +, including PNCounter negative deltas)
        // is NOT idempotent (counter_not_idempotent) => it must apply each delta
        // exactly once, the algebraic root of the #4935 double-apply. Counter, LWW,
        // and the mixed Counter×LWW product fully instantiate the core (counterCM /
        // lwwCM / mixedCM). PNCounter is the same signed-delta algebra with
        // decrement enabled, covered by singleStore_pncounter_converges and exact
        // live/restart/storm behavioral tests. The mixed product's cross-field
        // materialization hazard is checked by MixedFieldMaterialization.
        name: "CrdtField: comm+assoc => order-independent; PNCounter refines to signed Counter; mixed Counter×LWW inherits dedup",
        axis: Lean,
        anchor: "crates/crdt/src/lww.rs set_value; crates/crdt/src/counter.rs; crates/crdt/src/composite.rs componentwise field merge",
        model_ref: "DefraConvergence.MixedField (lake build); MC_MixedFieldMaterialization_Green.cfg",
        tiers: &[Contract, Behavioral],
    },
    Property {
        // Two behavioral legs: backfill (a write that pre-dates the replicator is
        // delivered) and full resume across a real disconnect (node1 restarted —
        // its state survives on disk and a post-disconnect write arrives after
        // reconnect). The resume leg needs an actual partition: config-level
        // replicator delete does not gate sync between already-connected peers, so
        // it restarts node1 via the harness `restart_node` (fixed upstream in
        // backbone 025d396 to respawn the configured binary, not the debug path).
        family: "Replicator lifecycle (no-loss / resume)",
        name: "INV_NoLoss — reconnect recomputes the target gap, no block dropped",
        axis: Tla,
        anchor: "crates/p2p/src/replicator.rs; crates/db/src/merge/push_docs_transport.rs",
        model_ref: "MC_Replicator_Resumable_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        family: "Sync ownership transfer (head hint / receiver pull)",
        name: "INV_ObligationConservation / INV_PendingServiceable / INV_PendingHasAuthenticatedProvider / INV_SingleFlight / INV_SingleMergeWriter / INV_SenderMarkersOnly",
        axis: Tla,
        anchor: "crates/p2p/src/sync coordinator + pending DAG clock; crates/db (merge) push replay; crates/storage peerstore retry markers",
        model_ref: "MC_SyncOwnership_Green.cfg",
        // The deterministic A/B and restart/fanout witnesses live in the
        // integration-test crate, outside this conformance binary's behavioral
        // harness. Keep this binding honest until that harness drives them.
        tiers: &[Boundary],
    },
    Property {
        family: "Sync ownership transfer (head hint / receiver pull)",
        name: "LIVE_EventualCurrency / LIVE_EventualReceiverQuiescence — document scope",
        axis: Tla,
        anchor: "crates/p2p/src/sync coordinator + pending DAG clock; crates/db (merge) document marker rederive",
        model_ref: "MC_SyncOwnership_Green_Liveness_Doc.cfg",
        tiers: &[Boundary],
    },
    Property {
        family: "Sync ownership transfer (head hint / receiver pull)",
        name: "LIVE_EventualCurrency / LIVE_EventualReceiverQuiescence — collection scope",
        axis: Tla,
        anchor: "crates/p2p/src/sync coordinator + pending DAG clock; crates/db (merge) collection marker rederive",
        model_ref: "MC_SyncOwnership_Green_Liveness_Collection.cfg",
        tiers: &[Boundary],
    },
    Property {
        family: "Sync ownership transfer (head hint / receiver pull)",
        name: "INV_FetchHasQualifiedProvider / LIVE_EventualCurrency — immutable origin plus durable same-root alternate",
        axis: Tla,
        anchor: "crates/p2p/src/sync/manager/process/pushlog.rs; crates/p2p/src/sync/pending_store.rs; crates/p2p/src/sync/coordinator/mod.rs",
        model_ref: "MC_SyncOwnership_Green_Liveness_IrohRelay.cfg",
        tiers: &[Boundary],
    },
    Property {
        family: "Multi-instance claim",
        name: "INV_EventualUnique — claim CAS converges to a single winner",
        axis: Tla,
        anchor: "gents:crates/gents/src/lifecycle/claim.rs:claim_inner (foreign substrate, not the defradb.rs binary)",
        model_ref: "MC_Claim_Filtered_Eventual.cfg",
        tiers: &[Boundary],
    },
    Property {
        // Verify-before-merge fires only on the P2P merge path; injecting a
        // forged/unsigned block needs an adversarial peer, not reachable through
        // the public CLI. Rust makes signature verification mandatory (model->code
        // audit) and EUF-CMA is a named crypto boundary, so this is structural.
        family: "Block integrity / signatures",
        name: "INV_OnlyVerifiedMerged — verify-before-merge, author bound to verified DID",
        axis: Tla,
        anchor: "crates/db (merge) sig verify; crates/defra-core/src/batch_signing.rs",
        model_ref: "MC_Integrity_Green.cfg",
        tiers: &[Boundary],
    },
    Property {
        // The unauthorized-denied leg is now observable: a serve-side
        // `tracing::warn!("DEK release DENIED")` at the KMS policy check makes the
        // previously-silent denial loggable, and the behavioral test reads node0's
        // log to confirm node0 denies the DEK to an unauthorized node1's fetch.
        // ECIES secrecy of a released envelope remains a crypto Boundary (proven
        // by crates/kms ecies_envelope unit tests).
        family: "KMS key distribution",
        name: "INV_AuthorizedEventuallyGets / no-unauthorized-usable",
        axis: Tla,
        anchor: "crates/kms/src/defra_kms.rs serve_request; crates/kms/src/nac_dac_policy.rs",
        model_ref: "MC_Kms_Green.cfg",
        tiers: &[Behavioral, Boundary],
    },
    Property {
        // Encryption reuses PriorityReconcile.lwwCM unchanged: ciphertext and
        // key timing affect when a value can materialize, not which value wins.
        // The behavioral legs preserve encrypted fields selected for filtered
        // replication and receive both encrypted siblings after a real restart,
        // proving identical DAGs and the decrypted winner.
        family: "Encrypted LWW restart/replay",
        name: "INV_NoFilteredLoss / INV_LwwWinner — encrypted delivery remains complete and LWW-convergent",
        axis: Tla,
        anchor: "crates/db/src/merge/push_docs.rs; crates/db/src/merge/push_docs_transport.rs; crates/crdt/src/lww.rs",
        model_ref: "MC_EncryptedLwwReplay_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        // The model and focused KMS/merge unit tests cover the retry mechanics,
        // but the external harness cannot deterministically hold a DEK request
        // unavailable between ciphertext acknowledgement and receiver restart.
        family: "Encrypted LWW restart/replay",
        name: "INV_AckBacked — acknowledged encrypted replay remains re-drivable across transient KMS failure and restart",
        axis: Tla,
        anchor: "crates/db/src/merge/merge_handler/composite_fields.rs; crates/p2p/src/sync/pending_store.rs",
        model_ref: "MC_EncryptedLwwReplay_Green.cfg",
        tiers: &[Boundary],
    },
    Property {
        family: "Management-channel auth (NAC gate)",
        name: "INV_OnlyAuthorizedManages — management ops require a valid scoped token",
        axis: Tla,
        anchor: "crates/http manage channel; crates/db (nac)",
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
        // The replay-capability gate lives inside the P2P replication wire
        // protocol; presenting a forged/expired capability needs a custom peer,
        // not the public CLI. Rust-only hardening Go lacks; structural.
        family: "P2P explicit-replay capability gate",
        name: "INV_OnlyLegitAccepted — forged/expired/wrong-target capability rejected",
        axis: Tla,
        anchor: "crates/p2p capability replay gate",
        model_ref: "MC_Capability_Green.cfg",
        tiers: &[Boundary],
    },
    Property {
        family: "NAC lifecycle privilege-escalation",
        name: "INV_NoPrivEsc — no escalation across enable/disable/restart",
        axis: Tla,
        anchor: "crates/db (nac) lifecycle",
        model_ref: "MC_Nac_Green.cfg",
        tiers: &[Behavioral],
    },
    Property {
        // No-loss / no-double-apply under concurrent same-document mutation is now
        // BEHAVIORAL: `partition::convergence_concurrent_same_doc_merge_storm`
        // storms one PCounter doc from a 3-node mesh and asserts the exact sum
        // (below => a delta dropped, above => double-applied). Mixed-field legs
        // assert the product state (LWW name + counter views) after live, restart,
        // and 3-node full-mesh replay; `MC_MixedFieldMaterialization` proves the
        // stale whole-document commit hazard RED/GREEN.
        // This found and fixed #1021's residual two-store counter race — local writes and merges both
        // RMW the authoritative accumulation store, serialized per-doc by
        // `crates/db/src/write/queue.rs` (shared by the local-write and merge
        // paths). The internal `INV_SameDocSerialized` "≤1 worker in the critical
        // section" + the txn-registry sweep remain a structural Boundary.
        // `MC_TwoStoreCounter` proves BOTH counter hazards RED-then-GREEN: the
        // two-store split lost-update (`MC_TwoStoreCounter_Red_Split`, INV_NoLoss) and
        // the merged-set/is_merged double-apply (`MC_TwoStoreCounter_Red_DoubleApply`,
        // INV_NoDoubleApply — the model twin of upstream Go #4935 / our #1043).
        family: "Transaction & merge-queue concurrency",
        name: "INV_NoLoss / INV_NoDoubleApply under concurrent same-doc mutation",
        axis: Tla,
        anchor: "crates/db/src/write/queue.rs (per-doc write lock, shared with crates/db (merge) merge handler); crates/db txn registry",
        model_ref: "MC_MergeQueue_Green.cfg / MC_TxnRegistry_Green.cfg / MC_TwoStoreCounter_Green.cfg / MC_MixedFieldMaterialization_Green.cfg",
        tiers: &[Behavioral, Boundary],
    },
    Property {
        // `partition::convergence_delete_update_race_preserves_tombstone`
        // partitions a replicated doc, deletes on one side, updates a mutable field
        // on the other, then heals and asserts the merged materialized view stays
        // tombstoned on both replicas. The TLA red leg captures the bad policy:
        // committing an active update from a stale whole-document snapshot clears
        // the delete marker.
        family: "Document materialization status convergence",
        name: "INV_DeletedMarkerAbsorbs — active rematerialization never clears a tombstone",
        axis: Tla,
        anchor: "crates/db/src/merge/merge_handler/composite_persist.rs handle_deletion / persist_merged_document; crates/crdt/src/composite.rs status",
        model_ref: "MC_DocumentMaterialization_Green.cfg; DefraConvergence.DocumentMaterialization.delete_active_age_converge",
        tiers: &[Behavioral],
    },
    Property {
        // The DID-binding half (a distinct identity cannot impersonate another)
        // IS observed behaviorally by the ACP test (ungranted bob denied). The
        // signer-binding half — forging a malformed / alg-confused token, or one
        // whose iss/signer DID mismatches its signature — is unreachable through
        // the public CLI (the client always signs correctly), so it is Boundary.
        family: "JWT issuer / algorithm binding",
        name: "INV_TokenBindsGenuineDid — alg/iss/sig bound to did(pubkey)",
        axis: Tla,
        anchor: "crates/identity token verification",
        model_ref: "MC_Jwt_Green.cfg",
        tiers: &[Boundary],
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
        anchor: "crates/query deferred-acp overlay",
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
        anchor: "crates/db (index) index maintenance",
        model_ref: "IndexMaintenance (lake build)",
        tiers: &[Behavioral, Contract],
    },
    Property {
        // `index::index_reconciles_lww_merge_after_restart` drives a restart
        // partition where two replicas write different values for the same indexed
        // LWW field. Once the DAG converges, indexed filters must expose exactly
        // the winning value and no stale seed/loser keys.
        family: "Index-maintenance consistency",
        name: "INV_IndexMatchesWinner — indexed filters equal the converged CRDT value",
        axis: Tla,
        anchor: "crates/db (merge) LWW materialization; crates/db (index) on_document_update",
        model_ref: "MC_IndexReconciliation_Green.cfg",
        tiers: &[Behavioral],
    },
];
