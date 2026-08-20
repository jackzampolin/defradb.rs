# Survey: `crates/db/`

## Purpose
High-level database API layer (mirrors Go `internal/db/`). Provides `DB`, `DbTxn`
(explicit/implicit transactions), `Collection` CRUD, schema patch/migration,
commits/versioned fetchers, ACP gating on doc ops, block signature verification,
downsample GC, and a transaction registry for query execution. It is largely an
orchestration crate: CRDT merge, content-addressing, P2P sync, KMS, and ACP
algebra all live in dependency crates (`crdt`, `db-merge`, `db-blocks`, `acp`,
`db-nac`) that have their own slices.

## State machines
- **Explicit/implicit txn lifecycle** (`txn/mod.rs`): `Some(txn)`→committed/discarded
  (`None`); explicit txns reject `commit()`/`discard()` (must `force_*`). Plumbing.
- **Stale-txn registry cleanup** (`txn/registry/cleanup.rs` `cleanup_stale_transactions`):
  collect candidates under read lock → re-check idle under per-ctx action lock →
  remove under write lock with `Arc::ptr_eq` + final idle re-check. A real
  concurrency protocol guarding against evicting a transaction a concurrent
  request just touched. NOT covered by an existing slice.
- **Schema patch / version transition** (`patch/`, `migration/`): old→new
  version_id, IsActive-only vs Transform-only vs structural; rejects unsafe
  multi-active states. Validation logic; integration-test territory.
- **block_verify** (`block_verify.rs`): verify-then-merge + dual-path ACP read
  gate — already modeled (Integrity, Commits slices).
- **Schema version_id CID** (`patch/version_id.rs`): content-addressed schema
  version from field CIDs — a content-addressing instance.

## Candidates
| name | kind | property | already-modeled | priority |
|---|---|---|---|---|
| TxnRegistryCleanupRace | TLA+ | a concurrent get/touch on a transaction during a stale-cleanup sweep never evicts a still-live txn (no lost active transaction); only genuinely-idle txns are removed | no | medium |
| BlockVerifyDualPathAcp | none | verify-then-merge + ACP-on-commits | yes (Integrity, Commits) | low |
| SchemaVersionContentAddr | Lean | determinism: equal schema content ⇒ equal version_id CID | partial (content-addressing/CRDT-laws) | low |
| FetcherConvergence | none | CRDT merge/convergence via `crdt`/`db-merge` | yes (Convergence, CRDT-laws) | low |
| PatchVersionTransition | none | safe schema version transitions | no (integration tests suffice) | low |

## Verdict
**Marginally model-worthy.** The crate is mostly orchestration whose hard
correctness (convergence, content-addressing, integrity, ACP dual-path, claim,
KMS, replicator) is already proved in dependency-crate slices. The one genuinely
novel, un-modeled concurrency hazard is the **transaction-registry stale-cleanup
race** — a lock-ordering / TOCTOU protocol whose correctness is not obvious from
the code and not exercised deterministically by integration tests. That is the
only candidate worth a (medium-priority) TLA+ slice. Everything else is plumbing
or already covered.
