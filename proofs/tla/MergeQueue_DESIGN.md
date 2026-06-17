# MergeQueue — per-document merge-queue serialization (TLA+ design)

Slice #5 of the survey backlog (`proofs/README.md`, `proofs/survey/db-merge.md`).
Models the per-document async merge mutex and the bounded txn-conflict retry loop in
defradb.rs's `db-merge` crate.

## Property

> The per-document write guard (the `DocWriteQueue` owned by the DB, shared by BOTH the
> local-write path and the db-merge merge handler since #1021) serializes same-document
> writes — merge-vs-merge AND local-write-vs-merge — while allowing cross-document
> parallelism; the bounded (5×) conflict-retry loop loses and duplicates no block; retry
> exhaustion **fails closed** (errors, never silently drops a block).

`UserWriteMode="PerDoc"` (the #1021 GREEN config) models a local user-write taking the
same per-doc guard the merge takes: it acquires `lockOwner[d]`, writes inside the critical
section, and releases. `INV_NoLocalMergeInterleave` then checks that a local write and a
same-doc merge are never both in the critical section — the mutual exclusion the counter
fix relies on. `UserWriteMode="LockFree"` keeps the pre-#1021 adversary (a lock-free
user-write that only drives the txn-conflict retry loop), used by the other configs.

## Source anchors (read the real code, not an abstraction)

### Rust (the mechanism under test)

| Symbol in model | Rust source | What it abstracts |
|---|---|---|
| per-doc mutex / `lockOwner`, `CanAcquire`, `Acquire` | `crates/db-merge/src/merge_handler/queue.rs:13-47` (`MergeQueue`, `acquire`) | `Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>`; `acquire(key)` returns an `OwnedMutexGuard` — same key blocks, different keys parallel |
| acquire-before-loop | `crates/db-merge/src/merge_handler/batch.rs:45` `let _guard = self.merge_queue.acquire(&block.doc_id).await;` | the guard is held across the whole retry loop |
| `MaxRetries = 5`, retry loop, `Retry`/`Exhaust` | `crates/db-merge/src/merge_handler/batch.rs:29` `const MAX_MERGE_RETRIES: usize = 5;` and `batch.rs:57-89` | `for attempt in 0..MAX_MERGE_RETRIES { … Err(e) if e.is_txn_conflict() => continue; … }` |
| `NoConflict` / txn conflict | `crates/db-merge/src/merge_handler/error.rs:56-67` (`is_txn_conflict`) | the retry trigger: storage SSI `ErrTxnConflict` |
| `AlreadyApplied` / `seenMerged` (is_merged guard) | `crates/db-merge/src/merge_handler/composite.rs:82-90`, `counter.rs:22-28` (`is_merged(cid)` → `terminal_skip("already merged")`) | the blockstore merged-set: the single source of CRDT idempotency (survey §State machines; #847) |
| fail-CLOSED on exhaustion (`FailMode="Closed"`) | `crates/db-merge/src/merge_handler/batch.rs:78-89` — `final_result = last_result.unwrap()`; the last `Err(txn_conflict)` is returned, not swallowed | exhausted retries yield `Err`, not a fake success |
| `marked` oracle (merged-set marking) | `crates/p2p/src/sync/replication/handlers.rs:630-659` — `Ok(Merged)`/`Ok(terminal Skip)` → `merged_cids.push`; `Err(_)` → `ReplicationResult::Failed`, **not** pushed | a block is recorded "done" (won't be re-delivered) iff merged or terminally skipped; an `Err` leaves it re-deliverable |
| `MergeOutcome` (`Merged` / terminal vs retryable `Skipped`) | `crates/p2p/src/sync/merge.rs:74-122` | only `terminal` skips are marked as merged |

### Go (the upstream parity reference — fetched from `origin/develop`, not the stale checkout)

`git -C …/sourcenetwork/defradb show origin/develop:internal/db/merge.go`

| Go anchor | Note |
|---|---|
| `merge.go:50-55` `docMergeQueue.add(evt.DocID)` / `colMergeQueue.add(evt.CollectionID)` + `defer …done()` | the per-doc / per-collection serialization queue (Rust keys the same way) |
| `merge.go:137-176` `mergeQueue` (`add`/`done` over `map[string]chan struct{}`) | Go's queue: `add` blocks on the key's channel until `done` closes it |
| `merge.go:61-72` retry loop `for i := 0; i < db.MaxTxnRetries(); i++ { … if errors.Is(err, corekv.ErrTxnConflict) { continue } … }` | matches Rust's 0..5 loop |
| `node/node.go:85` `MaxTxnRetries: immutable.Some(5)` | default retry budget = 5, matching `MAX_MERGE_RETRIES` |
| **`merge.go:71` `return nil` after the loop** | **the fail-OPEN footgun.** On retry exhaustion Go's `Merge` falls through to `return nil`: the caller treats the event as a success and will not re-deliver, but nothing was merged — a **silent drop**. This is the RED `FailMode="Open"` variant. |

## The independent oracle (why GREEN is not vacuous)

Correctness is judged from two ground-truth ledgers that are **not** the mutex/retry
mechanism's own decision:

- `applied[b]` — the number of times block `b`'s delta was actually committed into doc
  state. Incremented **only** inside a committing txn that found `b` un-applied.
- `marked[b]` — whether the *caller* recorded `b` as done (added to `merged_cids` in
  `process_merge_batch`). `Err` results are never marked.
- `docState[d]` / `Delivered(b)` — the merged-set: which originals reached doc state.

The headline invariants relate these ledgers; the mechanism cannot "agree with itself"
into a green. Anti-vacuity is also checked directly: a probe confirms the GREEN run
reaches `docState = [d1 ↦ {b1}, d2 ↦ {b3}]` (both blocks genuinely delivered), and the
`CrossDocParallel` probe forces TLC to exhibit two different-doc workers in their
critical sections at once.

## Invariants

| Name | Plain English |
|---|---|
| `INV_SameDocSerialized` | at most one occupant per doc inside its critical section (merge workers + a local user-write under `UserWriteMode="PerDoc"`) |
| `INV_NoLocalMergeInterleave` | a local user-write and a merge are never both in the critical section on the same doc (#1021 shared guard) |
| `INV_NoDoubleApply` | an original block's delta is committed at most once (idempotency) |
| `INV_NoSilentDrop` | a block marked done is actually delivered (applied, or its original already was) |
| `INV_NoLoss` | a terminated block is either delivered or still un-marked (hence re-deliverable) |
| `NoCrossDocParallel` | (negated probe) different-doc workers never run concurrently — asserted **only** to be refuted |

## Scenarios, configs, verdicts

Run from `proofs/tla` (Java resolved by `./tools/tlc`; on this box
`JAVA=/opt/homebrew/opt/java/bin/java`):

```bash
# GREEN — correct mechanism: per-doc mutex + fail-closed exhaustion. All 5 safety invs hold.
./tools/tlc -metadir states/mq_green   -config MC_MergeQueue_Green.cfg            MC_MergeQueue_Green.tla
# RED — no per-doc mutex: same-doc merges interleave -> INV_SameDocSerialized AND
#       INV_NoDoubleApply violated (applied[b1] reaches 2 via a stale is_merged snapshot).
./tools/tlc -metadir states/mq_nomutex -config MC_MergeQueue_Red_NoMutex.cfg     MC_MergeQueue_Red_NoMutex.tla
# RED — fail-open exhaustion (Go merge.go return-nil): retry-exhausted block marked done
#       without being applied -> INV_NoSilentDrop violated (silent drop).
./tools/tlc -metadir states/mq_failopen -config MC_MergeQueue_Red_FailOpen.cfg   MC_MergeQueue_Red_FailOpen.tla
# RED (anti-vacuity probe) — under the correct mutex, NoCrossDocParallel is violated:
#       the counterexample shows two different-doc merges running concurrently.
./tools/tlc -metadir states/mq_xdoc    -config MC_MergeQueue_CrossDocParallel.cfg MC_MergeQueue_CrossDocParallel.tla
```

| Config | Module | Knobs | Expected | Observed |
|---|---|---|---|---|
| `MC_MergeQueue_Green` | `MC_MergeQueue_Green.tla` | PerDoc + Closed | GREEN (all hold) | No error; 2921 distinct states |
| `MC_MergeQueue_Red_NoMutex` | `MC_MergeQueue_Red_NoMutex.tla` | None + Closed | RED | `INV_SameDocSerialized` violated; with only `INV_NoDoubleApply`, `applied[b1]=2` |
| `MC_MergeQueue_Red_FailOpen` | `MC_MergeQueue_Red_FailOpen.tla` | PerDoc + Open | RED | `INV_NoSilentDrop` violated |
| `MC_MergeQueue_CrossDocParallel` | `MC_MergeQueue_CrossDocParallel.tla` | PerDoc, probe | RED (witness) | `NoCrossDocParallel` violated: `inCrit = [d1↦{b1}, d2↦{b3}]` |

## Modeling boundaries (honest reach)

- **Bounded instances.** ≤3 blocks, ≤2 docs, retry budget 5, ≤6 user-writes. The
  witnessing shapes are minimal: one duplicate delivery on a shared doc (double-apply),
  one persistently-conflicted block (exhaustion), two docs (parallelism). Conclusions are
  structural, not quantity-sensitive.
- **`docVer` (conflict) is bumped only by user-writes, never by a concurrent same-doc
  merge.** This is deliberate and load-bearing: the system does **not** rely on storage
  SSI to detect merge-vs-merge conflicts on the merged-set marker — the per-doc mutex is
  what serializes them. Modeling SSI as also catching merge-vs-merge would mask the very
  bug the mutex exists to prevent. The `is_merged` snapshot (`seenMerged`, read at txn
  start per `composite.rs`/`counter.rs`, not at commit) is therefore the *only*
  idempotency guard in the no-mutex variant — and it is stale across the race window, so
  the duplicate double-applies. The user-vs-merge conflict (the Go comment "conflicts
  occur when a user updates a document while a merge is in progress", `merge.go:59-60`) is
  modeled and is what drives the retry loop in all variants.
- **One delta type / no DAG depth.** The composite-DAG ancestry walk, signature gate, ACP
  gate, encryption, and head-advance are out of scope here (already covered by
  Convergence / Integrity / Acp / Commits slices, per the survey). This slice isolates the
  queue + retry concurrency invariant only.
- **Model ≠ code.** Anchored by file:line above; no automated conformance harness.

## Findings

1. **Rust fails closed; Go's `Merge` fails open.** The `FailMode="Open"` RED reproduces the
   real `merge.go:71` `return nil`-after-exhaustion path: a block whose merge keeps
   conflicting past 5 retries is reported as a success and dropped silently. Rust's
   `merge_blocks_individually` (`batch.rs:78`) returns the last `Err`, which
   `process_merge_batch` turns into `ReplicationResult::Failed` and does **not** add to
   `merged_cids` — so the block stays re-deliverable. The Rust path is the fix; the model
   shows the Go path violates `INV_NoSilentDrop`. (Consistent with the README boundary
   note that Go-side gaps the models predicted are real Go-parity-constrained items.)
2. **The mutex is load-bearing for idempotency, not just for the lock invariant.** Because
   `is_merged` is read at txn start, a duplicate delivery of the same CID can race a
   not-yet-committed first merge; without the per-doc mutex both pass the guard and
   double-apply (`applied=2`). With the mutex, the second acquires only after the first
   commits, snapshots `seenMerged=true`, and terminal-skips. So serialization is what
   makes the merged-set idempotency guard effective under concurrency.
3. **Same-doc serialization does not over-serialize.** The `CrossDocParallel` probe proves
   the per-doc lock permits two different-doc merges to run at once — the parallelism the
   `MergeQueue` is designed to preserve.
