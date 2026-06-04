# Survey: `crates/ffi/`

## Purpose
C-compatible FFI binding layer. Exposes DefraDB.rs to Go (and other languages)
via a staticlib matching Go's `cbindings/` interface, so the Go test suite runs
against the Rust implementation. Every exported `extern "C"` fn marshals C
strings <-> Rust, validates an opaque `usize` handle, enters the shared tokio
runtime, and **delegates to an underlying crate** (`db`, `query`, `p2p`,
`acp`, `embedded`, `events`). Modules: node lifecycle, schema, collection,
document, query, txn, index, p2p, acp/nac, subscription, backup, lens, mobile,
encrypted_index.

## State machines
- **Handle registries** (`state/registry.rs`): `NodeRegistry` /
  `SubscriptionRegistry` / `GraphQLSubscriptionRegistry` — `RwLock<HashMap>` +
  `AtomicUsize` insert/get/remove. A monotonic-counter handle table, not a
  protocol. No transition logic worth proving.
- **`NodeState`** (`state/mod.rs`): a container of `Arc`-wrapped components owned
  by other crates (database, query_runner, nac_manager, document_acp, p2p,
  event_bus). Holds references, implements no state machine of its own.
- **Txn lifecycle** (`txn/lifecycle.rs`): begin/commit/rollback are thin
  pass-throughs to `query::txn` runner; the txn semantics live in `db`/`query`.
- **Panic boundary** (`ffi_entry!` + `panic=abort` in release): a no-op catch in
  release; pure FFI-safety plumbing, not a modelable property.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | All correctness-bearing behavior (replication, convergence, ACP gating, commit integrity, content-addressing) lives in delegated crates, already covered by B3 / convergence / acp / commits / integrity / replicator / kms slices. | yes (upstream crates) | — |

## Verdict
**Plumbing — not model-worthy.** The FFI crate is a marshaling/glue layer: C
string conversion, opaque handle bookkeeping, panic containment, and delegation.
It introduces no new concurrency, consistency, security, or algebraic invariant
beyond those of the crates it wraps, all of which already have TLA+/Lean slices.
Correctness here (no use-after-free across FFI, null/UTF-8 handling, handle
validity) is covered by the Go compatibility + Rust integration test suites, not
by formal models. `model_worthy: false`, no candidates.
