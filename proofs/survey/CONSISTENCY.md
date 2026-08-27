# Cross-Cutting Consistency & Completeness Report

Sweep of all 18 formal-model families across 5 clusters (replication-convergence,
integrity-cid, acp-security, keys-identity, storage-txn). Synthesizes the 5 cluster
audits plus independent re-verification of the load-bearing anchors and the regression
harness wiring (`proofs/tla/run-all.sh`, `proofs/lean/lakefile.lean`).

Verdict: **internally consistent, acyclic assumption graph, no contradictions.** One
cluster-audit harness claim was found to be inaccurate and is corrected below (the
replication-convergence configs ARE wired into `run-all.sh`).

---

## (a) Assumption → Discharge Graph

Notation: `A ──assumes──▶ B` means slice A relies on a fact that slice B proves.
The graph is a **DAG** (verified acyclic below). Three kinds of leaf:
**[CRYPTO]** = assumed primitive boundary; **[CONN]** = eventual-connectivity /
fair-delivery boundary; **[BOUND]** = bounded-N / scope boundary.

### Cross-slice discharge edges (the load-bearing skeleton)

```
                         ┌─────────────────────────────────────────────┐
                         │   CONTENT-ADDRESSING DETERMINISM (Lean Cid)  │
                         │   same delta+parents ⇒ same CID, order-indep │
                         └───────────────┬─────────────┬───────────────┘
                  discharges │           │ discharges  │ discharges
                             ▼           ▼             ▼
        ┌────────────────────────┐  ┌──────────┐  ┌──────────────────────┐
        │ Convergence / M1 /     │  │ Integrity│  │ DagReplication       │
        │ DagReplication / Repl. │  │ (sig=cnt)│  │ (parametric base)    │
        │ (treat CID as opaque   │  └────┬─────┘  └──────────────────────┘
        │  deterministic addr)   │       │
        └───────────┬────────────┘       │ EUF-CMA [CRYPTO]
       merge-order  │ assumes            │
       independence ▼                    ▼
        ┌────────────────────────┐  (signature-before-merge gate feeds
        │ DefraConvergence (Lean)│   replication/Commits content paths)
        │ LWW/counter/applied-set│
        │ comm/assoc/idem        │
        └────────────────────────┘

   KEYS-IDENTITY ──────────────────────────▶ ACP-SECURITY
   ┌──────┐   binds actor-DID    ┌──────────────────────────────────────┐
   │ Jwt  │─────────────────────▶│ Auth (ActorGate: fresh JWT-verified  │
   │      │  discharges Auth's   │       actor-DID + NAC grant)         │
   └──────┘  PresentedActor      └───────┬──────────┬──────────┬────────┘
              ASSUME                      │ gate     │ gate     │ gate
                                          ▼          ▼          ▼
                                     ┌────────┐ ┌─────────┐ ┌────────┐
                                     │ Nac    │ │DeferredAcp│ │Commits │
                                     │lifecycle│ │ identity │ │ paths │
                                     └───┬────┘ └────┬────┘ └───┬────┘
                                         │ admin-set │ fail-    │ already-
                                         │ integrity │ closed   │ committed
                                         ▼           ▼ projection▼ ACP
                                     ┌────────────────────────────────┐
                                     │ Acp (committed soundness +      │
                                     │ revocation; Lean: check =       │
                                     │ rewrite-closure, no over-grant) │
                                     └────────────────────────────────┘

   STORAGE-TXN (self-contained sub-DAG)
   ┌──────────────┐  discharges range-membership + d/i vs d/d classification
   │ OrderEncoding│──────────────────────────────────▶ ┌──────────┐
   │ (Lean)       │                                     │ SsiRange │
   └──────────────┘                                     └────┬─────┘
                                                  inherits MVSG oracle │
                                                  + snap==read_version ▼
                                                              ┌──────────┐
                                                              │   Ssi    │
                                                              └──────────┘
   ┌───────────┐ faithful set/clear (durability)  ┌──────────────────┐
   │IndexMaint.│◀─────────────────────────────────│ Ssi storage layer│
   └───────────┘                                   └──────────────────┘
   MergeQueue (per-doc mutex) ── complements ──▶ Ssi (merge-vs-merge NOT in SSI; by design)
   TxnRegistry (idle-handle sweep) ── orthogonal to ──▶ Ssi commit path

   KMS / Capability (keys-identity) ── feed ──▶ Commits/replication decryptability & live-replay gate
```

### Who assumes / who discharges / what is an explicit boundary

| Assumption | Assumed by | Discharged by | Status |
|---|---|---|---|
| Content-addressing determinism (same content ⇒ same CID, order-independent) | Convergence, M1, DagReplication, Replicator, Integrity | **Cid (Lean)** | DISCHARGED (prose link; TLA↔Lean cannot cross-import) |
| Merge is order-independent + idempotent (treat Merge as monotone set-union under parent guard) | Convergence, M1, DagReplication, Replicator, Claim (LWW only) | **DefraConvergence (Lean)** | DISCHARGED (prose link) |
| Verified bearer token soundly binds actor-DID (`PresentedActor`) | Auth | **Jwt (TLA)** | DISCHARGED — confirmed `Auth.tla:30 ASSUME PresentedActor ∈ [Requests→Actors]`; `Jwt INV_ReturnedDidIsSignerDid` supplies the binding |
| Per-request management auth gate sits in front | Nac, DeferredAcp, Commits | **Auth (TLA)** | DISCHARGED |
| Already-committed ACP state is sound + revocation-consistent | Commits, DeferredAcp | **Acp (TLA + Lean)** | DISCHARGED |
| Relation engine does not over-grant (check = closure membership) | Nac, DeferredAcp (Grant set) | **Acp (Lean)** | DISCHARGED |
| Uncommitted projection fail-closes to committed transition | Acp, Commits (rely on "already committed") | **DeferredAcp (TLA)** | DISCHARGED |
| Range membership (`ReadRange::contains`) + DocScan-vs-IndexRange prefix classification | SsiRange | **OrderEncoding (Lean)** | DISCHARGED — `cross_type_total_order` + marker disjointness |
| MVSG serializability oracle (point keys) | SsiRange (extends it) | **Ssi (TLA)** | DISCHARGED (verbatim oracle reuse) |
| Faithful storage set/clear (durability) | IndexMaintenance | Ssi storage layer | DISCHARGED (assumed-faithful, delegated) |
| DEKs delivered only to currently-authorized DIDs; recipient-bound | Commits/replication decryptability | **Kms (TLA)** | DISCHARGED |
| Live encrypted-replay soundly gated (peer/collection/TTL/revocation) | sync/merge replay path | **Capability (TLA)** | DISCHARGED |
| Merge-vs-merge serialization (NOT covered by SSI) | (gap SSI deliberately leaves) | **MergeQueue (TLA)** | DISCHARGED (per-doc mutex + fail-closed) |
| Signature-before-merge / verified-creator binding | replication/Commits content paths | **Integrity (TLA)** | DISCHARGED |
| EUF-CMA signature unforgeability | Integrity, Capability, Jwt | — | **[CRYPTO] BOUNDARY** (assumed, red configs prove load-bearing) |
| CBOR canonicality + SHA-256 collision-resistance | Cid, Capability (digest-keyed revoke) | — | **[CRYPTO] BOUNDARY** (`encode_injective`, `hashOf_injective` named axioms) |
| ECIES recipient-binding | Kms | — | **[CRYPTO] BOUNDARY** |
| Eventual connectivity / fair delivery | Convergence, Replicator, Kms | — | **[CONN] BOUNDARY** (conditional liveness, honestly declared) |
| Bounded N (2 nodes/3 blocks, 2 txns, etc.) | every slice | — | **[BOUND] BOUNDARY** (uniformly disclosed) |
| IEEE-754 float merge / float key encoding | DefraConvergence, OrderEncoding | — | **[BOUND] BOUNDARY** + `float_add_not_assoc` flags a REAL latent divergence (see gaps) |
| `snap == read_version` (TOCTOU window) | Ssi, SsiRange | — | **[BOUND] BOUNDARY** (conservative: wider window ⇒ more aborts) |

### Acyclicity

The graph is **acyclic**. The longest chain is
`Cid/DefraConvergence (Lean leaves) → Convergence/Integrity → replication consumers`,
and on the ACP side `Jwt → Auth → {Nac, DeferredAcp, Commits} → Acp`, and on storage
`OrderEncoding → SsiRange → Ssi`. No back-edge exists:

- Acp is assumed by DeferredAcp/Commits, but Acp does NOT assume anything DeferredAcp/Commits prove — it operates only on already-committed state and a fuel-bounded rewrite closure. DeferredAcp closes the *uncommitted→committed* transition that Acp/Commits take as a precondition; this is a lower layer feeding an upper one, not a cycle. **The seeming "A assumes B, B assumes A" is resolved by the committed/uncommitted phase split: DeferredAcp proves the projection fail-closes to the committed oracle that Acp itself defines; Acp never reads DeferredAcp's overlay.**
- SsiRange ⊃ Ssi (faithful superset on point keys); Ssi cedes the carve-out to SsiRange and never imports it back.
- Cid/DefraConvergence are pure Lean leaves with only [CRYPTO] axioms below them.

No undischarged *internal* assumption was found: every assumption is either discharged
by a named slice or declared as an explicit [CRYPTO]/[CONN]/[BOUND] boundary.

---

## (b) Cross-Cluster Consistency Verdict: **CONSISTENT**

**1. Access/ACP model (acp-security ↔ keys-identity Auth-DID binding).** AGREE.
The actor-DID binding that Auth treats as a black box (`Auth.tla:30 ASSUME
PresentedActor ∈ [Requests→Actors]`, with `Verify` checking only `FreshCredential`) is
exactly what Jwt's `INV_TokenBindsGenuineDid` / `INV_ReturnedDidIsSignerDid` supply.
The hand-off is named on both sides. "Permission" = DID ∈ ACP grant set uniformly
across Commits/DeferredAcp/Acp; "admin" = verified DID with Auth establishing
freshness/scope and Nac establishing admin-set integrity. No double-modeling.

**2. Content-addressing (integrity-cid ↔ replication-convergence).** AGREE.
Cid's normal-form Block (sorted heads by `cid.to_string()`, sorted links, empty→None)
matches `crates/defra-core/src/block.rs` and the Go upstream `block.go` wire contract.
Convergence/M1/DagReplication/Replicator all treat CIDs as opaque deterministic content
addresses — precisely the determinism Cid proves. Integrity relies on the same fact for
its `signedContent = content` comparison. One delta-DAG model (acyclic `Parents`,
`RECURSIVE AncestorsOf`, parent-guarded merge) is shared verbatim across all four
replication slices.

**3. Crypto boundary (keys-identity ↔ integrity-cid).** CONSISTENT, cleanly PARTITIONED.
The full crypto boundary is the **union** {EUF-CMA signature unforgeability, CBOR
canonicality, SHA-256 collision-resistance, ECIES recipient-binding, ed25519+CBOR for
capabilities}, split with **no overlap and no gap**: Cid uses CBOR+SHA (not EUF-CMA);
Integrity/Jwt/Capability use EUF-CMA; Kms uses ECIES. Each red config flips its boundary
OFF to prove it load-bearing. The audit-prompt framing that these are "the same crypto
boundary" slightly over-unifies — they are *related but distinct* primitives, correctly
catalogued (see `survey/blockstore.md` tying CID-verification back to the union). Not a
contradiction.

**4. Transaction/commit model (storage-txn ↔ committers).** CONSISTENT.
- DeferredAcp explicitly assumes "storage txn commit is atomic (the SSI slice's
  concern)" — matching Ssi's atomic-commit critical section. No contradiction.
- MergeQueue and Ssi agree by construction that merge-vs-merge conflicts are NOT
  detected by SSI (`docVer` bumped only by user-writes); the per-doc mutex serializes
  them while user-vs-merge conflicts flow through the SSI retry loop. Coherent division
  of labor.
- Convergence/Replicator commit semantics are abstract set-union under a parent guard
  (no byte-level storage), so they do not collide with the SSI key-level model — they
  operate at different abstraction layers, and the Lean order-independence justifies the
  set-union abstraction.

No cross-cluster contradiction found.

---

## (c) Gap / Inconsistency List

### Corrected cluster-audit claim (was reported as a gap; it is NOT)

- **HARNESS WIRING — replication-convergence audit was INACCURATE.** That audit stated
  M1Convergence, Convergence, Replicator, and Claim are "intentionally NOT wired into
  `run-all.sh`." **They ARE wired** (`run-all.sh` lines 10–25 cover M1Convergence/M1Naive/
  S2/S3/S4/MC_Conv_*/MC_Claim_*; lines 36–37 cover MC_Replicator_*). The regression
  sweep already includes them. This correction *improves* harness-readiness vs. the
  audit's pessimistic claim.

### Real gaps / honest named boundaries (carry forward)

1. **COMPOSITION NOT MECHANIZED (named boundary, not a defect).** The headline
   "DAG convergence = TLA delivery × Lean merge-order-independence" is composed in prose
   (`proofs/README.md` states it: "(TLA … under eventual connectivity) × (Lean: applying
   deltas in any order yields the same …)"). No shared symbol mechanically links
   `Convergence.AllConverged` to `DefraConvergence.composite_merge_assoc`. TLA and Lean
   cannot cross-import, so this is structurally unavoidable; the hand-off is human-checked.
   Same status for every Lean→TLA discharge (Cid→Convergence/Integrity,
   OrderEncoding→SsiRange, Acp Lean→Acp TLA).

2. **REAL LATENT BUG SURFACED, DECLARED OUT OF SCOPE — float counters.**
   `DefraConvergence.float_add_not_assoc` proves IEEE-754 float-counter merge is
   order-dependent; both Go and Rust ship Float32/64 counters with raw `+` and no
   mitigation. Only Int64 convergence is *claimed*. Downstream consumers must know float
   counters are **outside the convergence guarantee**. Mirrored by OrderEncoding's
   unmodeled float key encoding — float-typed index columns are not covered by the
   `contains`-classifies-correctly discharge.

3. **DagReplication.tla is the only genuinely-unwired TLA file** — it has no `.cfg`
   wrapper and is absent from `run-all.sh`. This is **correct by design**: it is the
   parametric base module, and its checkable instances are the M1/S2/S3/S4 wrappers that
   ARE in the sweep. No checkable property is lost.

4. **Lean build is a SEPARATE regression entry point from `run-all.sh`.** `run-all.sh`
   is TLA-only (it shells `tools/tlc`, never `lake`). The Lean half of the corpus is
   gated by `cd proofs/lean && lake build` (documented in `proofs/README.md:71-72`).
   A unified one-shot "verify everything" entry point does not exist; CI must invoke both.

5. **STALE/IMPRECISE ANCHORS (cosmetic, verified).**
   - `Replicator_DESIGN.md` cites `replicator.rs:35`/`:129`; actual is
     `ReplicatorStatus` enum at **:43**, `ReplicatorInfo` struct at **:135** (confirmed).
   - `Cid` DESIGN says `compute_merkle_root` sorts "by CID string"; actual
     `crates/defra-core/src/batch_signing.rs:32` sorts `by_key(|a| a.to_bytes())`
     (bytes). Order-independence holds either way (Lean uses an abstract key).
   - Convergence/Claim cites (`lww.rs`, `dag_sync/state.rs`) drifted a few lines.
   Symbols all present and semantically matching; only line numbers drifted.

6. **CLAIM SUBSTRATE SEAM (named, justified).** Claim models claim-block delivery with
   its own fair `Deliver/seen` relation rather than importing the DagReplication/
   Convergence DAG modules. Consistent at the abstraction level (fair gated delivery +
   LWW resolution that DefraConvergence proves order-independent), but the cluster does
   not share ONE TLA delivery module end-to-end. Appropriate for Claim's question
   (LWW-CAS races, not DAG completeness).

7. **DISCLOSED-BUT-UNEXERCISED storage assumptions.** Ssi/SsiRange GC-soundness
   (`committed.len()>1000` pruning) is assumed and not model-checked at bound;
   `snap == read_version` TOCTOU window is abstracted and routed to a separately-surveyed
   slice. Both honestly disclosed, low risk, but unguarded.

No outright **contradiction** between any two slices was found.

---

## (d) Harness-Readiness Assessment

Target next step: a Gents-style **binary-conformance harness** binding each model's
property to the real Rust binary.

### Ready now

- **Property statements are explicit and named.** Every slice exposes its safety/liveness
  property as a named TLA invariant (`INV_*`) or named Lean theorem, each with a RED
  oracle proving it load-bearing. This is directly extractable: a harness can enumerate
  the `INV_*` / theorem names as the conformance checklist.
- **Source anchors exist for nearly every slice.** Properties point at concrete Rust
  symbols (e.g. `merge.rs validate_authorization`, `txn/registry/cleanup.rs:59-62`,
  `batch_signing.rs:31-32`, `mod.rs:284-323` for Jwt, `block.rs` for Cid). Spot-checks
  this run matched within a line or two.
- **Regression oracle is automated for TLA.** `run-all.sh` encodes the expected
  red/green verdict per config and exits non-zero on any mismatch — a CI-ready gate that
  already covers all 64 TLA configs including the replication-convergence family.
- **Lean is `sorry`-free with audited axioms.** All five Lean libs build clean; axiom
  audits show only standard core axioms (`propext`, `Quot.sound`) plus the two NAMED
  crypto-boundary axioms in Cid and `native_decide`/`ofReduceBool` for negatives.
  Greens are non-vacuous (independent oracles + anti-vacuity probes throughout).

### Missing to be fully harness-ready

1. **No model↔code conformance harness exists for ANY slice** (uniformly disclosed as
   "Model ≠ code" in every DESIGN). The anchors are the *manual* link; **anchor drift is
   the standing risk** and there is no automated guard against it. Building the harness
   should start by pinning these anchors (and fixing the 3 stale ones in §c.5).
2. **Anchors are line-numbered, hence brittle.** A conformance harness should bind to
   *symbols* (function/type names), not line numbers, so it survives refactors. Several
   DESIGN anchors are line-based and already drifted.
3. **No machine-checked TLA↔Lean composition.** The delivery×merge-order and the
   Cid/OrderEncoding/Acp Lean→TLA discharges are prose. A harness exercising the real
   binary could *empirically* close these (run real concurrent merges, observe
   convergence) — turning two prose hand-offs into binary-tested obligations.
4. **No unified verify entry point.** TLA (`run-all.sh`) and Lean (`lake build`) are
   separate; the harness wrapper should invoke both plus the future binary checks.
5. **[CRYPTO]/[CONN]/[BOUND] boundaries are out of binary scope by construction** — the
   harness can validate the *guard structure* (e.g. "merge refuses an unsigned block",
   "revoked token rejected") against the binary, but cannot test unforgeability or
   eventual connectivity. The harness checklist should mark these as
   structurally-tested-only.

**Bottom line:** the models are *property-explicit and anchor-bearing* — the two
prerequisites for a conformance harness — and the TLA regression sweep is already wired
and broader than the audits credited. The remaining work is (a) fix 3 stale anchors,
(b) re-key anchors from line numbers to symbols, (c) build the binary binding that turns
each `INV_*`/theorem + anchor into an executable check against the real defradb.rs node,
and (d) provide a single entry point invoking TLA + Lean + binary checks together.
