# Survey: `crates/datastore/`

## Purpose
Transaction-wrapper / namespace-prefixing shim sitting between the `storage`
(corekv) layer and the `db` layer, mirroring Go's `internal/datastore/`. Provides:
- `BasicTxn`: wraps a corekv txn, exposes namespaced store views, holds lifecycle callbacks.
- `SharedTxn` (`Arc<RwLock<Box<dyn Txn>>>`): one underlying txn shared across namespace views.
- `NamespaceView` / `RootView`: prefix every key with a 1-byte namespace tag (`b/d/e/h/p/s`).
- `Txn` trait: common interface; callbacks fire on commit/error/discard.

## State machines
- **TxnState lifecycle**: `Active -> Committed` (commit ok) / `Active -> Discarded` (discard or commit err path). Explicit 3-state enum. Transitions guarded by a `state != Active` check AND Rust ownership — `commit`/`discard` consume `self`, so use-after-terminal is mostly compile-time-impossible. Single txn, no cross-node or concurrent-interleaving dimension.
- **Namespace prefix round-trip** (implicit): `unprefix_key(ns, prefix_key(ns, k)) == k`; views over distinct namespaces are key-disjoint. One-byte concat; correctness obvious by inspection.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Txn lifecycle no-double-finalize | TLA+ | no commit-after-commit / use-after-discard; callbacks fire exactly once in correct phase | no | low |
| Namespace prefix injectivity / isolation | Lean | prefix/unprefix are inverse; distinct namespaces yield disjoint keyspaces | no | low |

Both are low priority: the lifecycle is ownership-enforced (not a concurrent/distributed
protocol with adversarial interleavings), and the prefix law is a trivial single-byte
concat already exercised by in-crate unit tests (`test_*_namespace_isolation`,
`test_root_view_sees_prefixed`). Transaction *isolation/atomicity* semantics live in the
`storage`/corekv backends, not here — this crate only forwards calls.

## Verdict
**Plumbing.** No model-worthy candidates. The crate is a glue layer that forwards reads/
writes to corekv with a namespace prefix and runs callbacks on a 3-state lifecycle. All
behavior is covered by existing unit tests (`multistore.rs` tests, `tests/txn_tests.rs`)
and integration tests. The properties worth proving (CRDT convergence, replication,
ACP, content-addressing) live in other crates and are already covered by existing
TLA+/Lean slices (convergence, CRDT-laws, acp, replicator, commits, integrity).
