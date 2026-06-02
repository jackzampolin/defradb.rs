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
- **Excluded by design:** Float32/64 counter merge laws (IEEE-754 addition is not
  associative); key rotation (a node revoked *after* already holding a key is out of
  scope). Model B's `INV_DagComplete` is deliberately relaxed (documented in `tla/README.md`).
- **Model ≠ code.** These prove properties of the *models*. They are anchored to source by
  the `*_DESIGN.md` docs, but there is **no automated conformance harness yet** — keeping
  the models in step with the code is currently a manual discipline. (defra-agent's
  `lean_vocab_test.rs` JSON-extraction approach is the reference if/when we add one.)
