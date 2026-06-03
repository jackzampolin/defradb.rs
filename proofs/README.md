# Formal methods for defradb.rs

Machine-checked models of defradb.rs's correctness-critical protocols. Two tools,
split by the kind of obligation each is good at:

| Dir | Tool | Proves | Entry |
|---|---|---|---|
| [`tla/`](tla/) | TLA+ / TLC | **temporal / distributed / concurrent** properties — convergence, "eventually merges", safety under adversarial interleavings | [`tla/README.md`](tla/README.md) |
| [`lean/`](lean/) | Lean 4 | **functional / algebraic** properties — CRDT merge laws (commutativity, associativity, idempotence), order-independence | [`lean/README.md`](lean/README.md) |

Some results need both. **DAG convergence = (TLA+: every node receives every delta
under eventual connectivity) × (Lean: applying deltas in any order yields the same
state).** Neither half alone is convergence.

## Coverage map

Status of the effort across the 40-crate surface, from the per-crate survey in
[`survey/`](survey/) (full index, incl. out-of-scope rationale, in
[`survey/INDEX.md`](survey/INDEX.md)). Of 40 crates, **12 are model-worthy**; the other 28
are plumbing covered by integration tests / Go-FFI parity / unit tests. The point of this
section is the **diff**: what is proven vs. the accepted gap.

### Modeled — 9 families (proven)
| Family | Tool | Crates it covers |
|---|---|---|
| B3 filtered replication | TLA+ | p2p, db-merge |
| DAG convergence (partition / eviction / restart) | TLA+ | p2p, db-merge, blockstore |
| CRDT merge laws | Lean | crdt, defra-core |
| Replicator lifecycle (no-loss / resume) | TLA+ | p2p, db-merge, embedded |
| Multi-instance claim | TLA+ | defra-agent |
| Block integrity / signatures | TLA+ | db-merge, defra-core, blockstore |
| KMS key distribution | TLA+ | kms, db-merge |
| Management-channel auth (NAC gate) | TLA+ | http, db-nac, pg-compat |
| ACP soundness + revocation + dual-path commits | TLA+ & Lean | acp, zanzibar, sourcehub, query, db |

### Backlog — want to model (13: 2 high, 11 medium)
The accepted gap (from the survey accept/reject pass). Each new slice reads its crate's
`survey/<crate>.md` first.

| # | Candidate | Crate | Tool | Property to prove | Pri |
|---|---|---|---|---|---|
| 1 | SSI snapshot-isolation | storage | TLA+ | every accepted commit serializable; no lost-update / write-skew survives (`ConflictTracker`) | **high** |
| 2 | Explicit-replay capability gate | p2p | TLA+ | capability tokens unforgeable, peer+collection-bound, TTL-capped, revocable | **high** |
| 3 | SSI scan carve-out soundness | storage | TLA+ | scan-prefix carve-out drops only false positives, never a real write-skew | med |
| 4 | Order-preserving encoding monotonicity | storage | Lean | a<b ⟹ encode_asc(a) <lex encode_asc(b); cross-type markers total order | med |
| 5 | NAC lifecycle priv-esc safety | acp | TLA+ | enable→disable→re-enable: no non-admin mutates admin set; disabled-flag persists | med |
| 6 | TxnRegistryCleanupRace | db | TLA+ | stale-txn sweep never evicts a still-live transaction | med |
| 7 | merge-queue-serialization | db-merge | TLA+ | per-doc mutex serializes same-doc merges; bounded retry loses/dups no block; fails closed | med |
| 8 | Deferred-ACP overlay consistency | query-plan | TLA+ | txn-local ACP projection gates as committed state; fail-closed across commit/rollback | med |
| 9 | JWT issuer-binding / alg-confusion | identity | Lean/TLA+ | token→DID only iff iss==did(pubkey) & alg matches key — **discharges an Auth-slice assumption** | med |
| 10 | Capability revocation consistency | p2p | TLA+ | once revoked, every later verify denies; monotone under concurrent verify/revoke | med |
| 11 | CID content-addressing determinism | defra-core | Lean | equal blocks ⟹ same canonical CID; injective — **discharges a Convergence/Integrity assumption** | med |
| 12 | Block.new canonicalization | defra-core | Lean | unique normal form (sorted heads/links); CID independent of input link order | med |
| 13 | Index-maintenance consistency | db-index | Lean | after `on_document_update`, stored entries == `extract(new)`: no stale, none missing | med |

> Items 9 and 11–12 **discharge assumptions** existing slices currently take for granted
> (content-addressing determinism; JWT identity binding) — proving them hardens what's
> already built, not just new surface.

### Deferred — Lean-lemma appendix (13 low)
Low-risk hardening lemmas parked for later: index-extraction determinism, cartesian-product
laws, LWW tie-break total order, storage encode round-trip, EncryptedStore key-binding,
capability rate-limiter fairness, Merkle batch-root determinism, DocID parse round-trip,
token clock-skew validity, DID-derivation determinism, schema update-immutability,
single-active-version invariant, blockstore cache↔storage coherence. (Full rows: `survey/INDEX.md` §2.)

### Out of scope — 28 crates
Plumbing/glue with no novel modelable invariant; one-line rationale per crate in
`survey/INDEX.md` §3 (cli, ffi, wasm, http-transport, storage-encoding-glue, query-parse, …).

## Build / run everything

Prereqs: **Java 11+** (TLC) and **Lean via elan** (`lean-toolchain` pins the version).

```bash
# all TLA+ models (26 runs, red+green oracle, exits non-zero on any mismatch):
cd proofs/tla && ./run-all.sh

# all Lean proofs (builds clean, zero `sorry`):
cd proofs/lean && lake build
```

`proofs/tla/tools/tla2tools.jar` is git-ignored; the wrapper re-downloads it if missing.

## Conventions (follow these in new slices)

- **One model family per concern.** Each family = a base spec (`tla/<Family>.tla`) plus
  thin scenario wrappers (`tla/MC_<Family>_*.tla` + `.cfg`) that `EXTENDS` it. Lean
  mirrors this with a barrel module + focused submodules.
- **Red/green TDD-for-models.** Every claim has a config the *buggy* policy violates (a
  TLC counterexample) and a config where the fix holds. A property that only ever passes
  green proves little.
- **Naming:** invariants/safety `INV_*`; liveness as `<>[]...` properties; scenario
  wrappers `MC_<Family>_<Case>`.
- **Source-anchored.** Every family has a `<Family>_DESIGN.md` grounding the abstraction
  in specific defradb.rs / defra-agent source modules (file:line). Model the real code
  paths, not an abstraction in a vacuum.
- **Lean:** mathlib-free, toolchain pinned, **no `sorry`/`admit`**, and `#print axioms`
  status recorded in `lean/README.md`.

## Boundaries — what is proven vs assumed vs out of scope

Read this before trusting a verdict; it states the model's honest reach.

- **Bounded instances.** TLC runs at small N (2–3 nodes, ≤~6 blocks). These are the
  *minimal witnessing shapes* for each property; conclusions are structural (a guard on a
  fetch, a datum on a block), not quantity-sensitive — but this is exhaustive over the
  bound, not a proof for all deployments.
- **Environment assumptions (encoded, not proven):** `ProviderAvailable` (a fetch needs a
  reachable holder of the block); **eventual connectivity** + fair head rediscovery for
  the convergence liveness results; weak fairness on protocol actions.
- **Crypto boundary (KMS):** ECIES is abstracted — a node can use an envelope iff it is
  the intended recipient. Real ECIES/IND-CCA security is assumed, not modeled.
- **Float counter divergence (proven, real):** IEEE-754 addition is not associative, so
  float counters are NOT order-independent — proven by `float_add_not_assoc` in
  `lean/DefraConvergence/LocalState.lean`. The 2026-06-03 model→code audit confirmed both
  Go and Rust ship `Float32/64` counters with raw `+` and no mitigation: a real latent
  replica divergence (fix is a Go-parity-constrained product decision, tracked separately).
- **Excluded by design:** key rotation (a node revoked *after* already holding a key is
  out of scope). Model B's `INV_DagComplete` is deliberately relaxed (see `tla/README.md`).
- **Audit outcomes (2026-06-03):** model→code audit of all 9 families confirmed the GREEN
  claims hold in Go+Rust, and surfaced real Go-side gaps the models predicted — `_commits`
  is ACP-ungated in Go (Rust gates it; a known DefraDB limitation), and Go's P2P block
  signature verification is optional with an unverified author/creator field (Rust is
  hardened: mandatory verify + author-binding). See the slice `*_DESIGN.md` for details.
- **Model ≠ code.** These prove properties of the *models*. They are anchored to source by
  the `*_DESIGN.md` docs, but there is **no automated conformance harness yet** — keeping
  the models in step with the code is currently a manual discipline. (defra-agent's
  `lean_vocab_test.rs` JSON-extraction approach is the reference if/when we add one.)
