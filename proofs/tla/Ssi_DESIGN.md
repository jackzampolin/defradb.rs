# Ssi — SSI snapshot-isolation serializability (`ConflictTracker`)

## What this slice proves

**Property (GREEN invariant).** Every set of transactions *accepted* (committed) by the
`ConflictTracker` is **conflict-serializable**: the multiversion serialization graph
(MVSG) over the accepted commits is acyclic. Equivalently, the tracker aborts a committing
txn iff a concurrent, later-committed txn wrote a key this txn *read*, or read a key this
txn *wrote* — so no lost-update and no write-skew survives.

**Teeth (RED).** A nearby buggy variant that drops half of the SSI test — checking only
write-write conflicts (the classic "snapshot isolation, not serializable" backend) —
*accepts* a write-skew schedule. TLC finds the MVSG cycle: a real counterexample.

This is the standard SI-vs-SSI distinction. Plain snapshot isolation prevents lost updates
on the *same* key but admits **write-skew** (two txns each read a set, then write disjoint
keys, invalidating each other's read predicate). SSI's read/write intersection test is
exactly what closes that gap. The invariant is the acyclicity oracle, computed independently
of the mechanism, so a mechanism that "agrees with itself" cannot fake green.

## Source anchors (the real code this abstracts)

All paths under `crates/storage/src/`.

| Spec symbol | Real code | Anchor |
|---|---|---|
| global monotonic `version` | `ConflictTracker.version: AtomicU64` | `backends/shared.rs:250` |
| `committed` log of `(ver, writes, reads)` | `ConflictTracker.committed: Mutex<Vec<(u64,HashSet,ReadSet)>>` | `backends/shared.rs:245,253` |
| `txnSnapshot[t]` = read_version | `current_version()` captured at `new_txn` | `backends/shared.rs:267`; `backends/memory/store.rs:56,64` |
| `Commit(t)` → `check_and_record` | `ConflictTracker::check_and_record` | `backends/shared.rs:275-324` |
| ww-conflict: `committed_writes.contains(write_key)` | first `for` test | `backends/shared.rs:294` |
| **rw-conflict A**: committed *read* hit a key I write — `committed_reads.conflicts_key(write_key)` | first `for` test, 2nd disjunct | `backends/shared.rs:295` |
| **rw-conflict B**: committed *write* hit a key/range I read — `read_set.conflicts_key(committed_write)` | second test | `backends/shared.rs:301-303` |
| "committed after my snapshot" filter `commit_ver > read_version` | the `if *commit_ver > read_version` guard | `backends/shared.rs:292` |
| assign new version + append on success | `fetch_add(1)`; `committed.push(...)` | `backends/shared.rs:311-313` |
| read recorded into read-set | `read_set.record_key` on `get/has/get_size` | `backends/memory/transaction.rs:90,103,116` |
| range read recorded | `record_iter_options` | `backends/memory/transaction.rs:125`; `backends/shared.rs:195` |
| commit gated on `check_and_record` before applying | `MemoryTxn::commit` | `backends/memory/transaction.rs:206-215` |
| empty-write fast path (read-only never conflicts) | `if write_keys.is_empty() { return Ok(()) }` | `backends/shared.rs:284` |

**Carve-out (NOT modeled here; deliberately abstracted away — see Boundaries).**
`is_document_collection_scan_prefix` at `backends/shared.rs:214-223` suppresses conflicts
for `d/d/` / `/d/` full-collection scans. That is a separate slice (survey item #3, "scan
carve-out soundness"). This slice models the core SSI engine on **point keys**, where the
carve-out never fires. Modeling it here would muddy the teeth of the core invariant.

## Parity / divergence with Go

Go DefraDB has **no equivalent hand-rolled `ConflictTracker`**. It runs on badger
(`node/store_badger.go`, via `github.com/sourcenetwork/corekv/badger`), and badger's
transactions provide SSI natively (`detectConflicts` in badger's `txn.go`). The Rust
`ConflictTracker` is a **reimplementation of badger's serializable-snapshot-isolation** for
the non-badger Rust backends (memory/redb/fjall/rocksdb), as the doc comment at
`backends/shared.rs:170-174,238-243` states. So:

- **Parity claim:** Rust ConflictTracker must match badger's accept/abort decision on the
  same read/write/commit schedule. This model formalizes *badger's intended guarantee*
  (conflict-serializability of accepted commits) and proves the Rust engine's test
  (ww + rw_A + rw_B against `commit_ver > read_version`) realizes it.
- **Divergence risk this model guards:** if a future refactor drops the rw checks (keeping
  only ww — easy mistake, "looks like optimistic CC"), Rust silently degrades from
  serializable to plain SI, diverging from badger. That is exactly the RED config.

## Model (state machine)

`Txns` is a fixed finite set; each `t` has constants `Reads[t], Writes[t] \subseteq Keys`.
A txn moves `idle -> active -> committed | aborted`.

- `version` — global monotonic counter (committed-version source).
- `snap[t]` — snapshot version captured at Begin (`current_version`).
- `status[t]` — idle / active / committed / aborted.
- `cver[t]` — commit version assigned on accept (0 if not committed).
- `log` — ordered sequence of accepted records `<<t, ver, Writes, Reads>>` (mirrors
  `committed` Vec).

Actions:
- `Begin(t)`: idle→active, `snap[t] := version`.
- `Commit(t)`: active→ either committed (if `~Conflicts(t)`, assign `cver`, append to log,
  bump version) or aborted (if `Conflicts(t)`). Read-only (`Writes[t] = {}`) always accepts
  (matches the `write_keys.is_empty()` fast path).

`Conflicts(t)` (the GREEN / real test) = `\E rec \in log : rec.ver > snap[t] /\`
  `( (rec.writes \cap Writes[t]) # {}        \* ww`
  ` \/ (rec.reads  \cap Writes[t]) # {}       \* rw_A: committed read hit my write`
  ` \/ (rec.writes \cap Reads[t])  # {} )`    \* rw_B: committed write hit my read

`ConflictsWWOnly(t)` (the RED variant, `SSIMode = "WWOnly"`) keeps only the `ww` disjunct —
plain snapshot isolation. This is the one-line bug the model is built to catch.

## Invariant (the oracle — independent of the mechanism)

`INV_Serializable`: the MVSG over the **committed** txns is acyclic.

MVSG edges among committed txns (ordered by `cver`), for any shared key `k`:
- **ww**: `k \in Writes[a] \cap Writes[b]` and `cver[a] < cver[b]` ⇒ `a -> b`.
- **wr**: `b` read `k` from `a`'s version, i.e. `a` is the last committer of `k` at or
  before `b`'s snapshot, `k \in Reads[b]` ⇒ `a -> b`.
- **rw (anti-dependency)**: `b` overwrites a version that `a` read — `k \in Reads[a]`,
  `k \in Writes[b]`, and `a` did *not* see `b`'s write (`b` committed after `a`'s snapshot,
  i.e. `cver[b] > snap[a]`) ⇒ `a -> b`. This is the write-skew edge.

`INV_Serializable == IsAcyclic(MVSGEdges)`.

Because the oracle is the textbook MVSG, a mechanism that accepts a non-serializable
schedule (write-skew) produces a 2-cycle `a -> b -> a` and TLC reports the violation. The
green mechanism's rw_A/rw_B tests abort exactly the txn that would close such a cycle.

Auxiliary sanity invariants (kept, not the headline):
- `INV_TypeOK`.
- `INV_MonotoneCommit`: committed `cver` values are distinct and ≤ `version`.

## Red / green scenarios

| Config | SSIMode | Expect | Why |
|---|---|---|---|
| `MC_Ssi_Green` | `"Full"` | **green** (no violation) | full ww+rw_A+rw_B test ⇒ every accepted set is MVSG-acyclic |
| `MC_Ssi_Red_WriteSkew` | `"WWOnly"` | **red** (violation) | ww-only accepts the classic write-skew; MVSG has an `a↔b` cycle |
| `MC_Ssi_Red_NoSnapFilter` | `"NoSnapFilter"` | **red** OR safe-but-degenerate | drops the `rec.ver > snap[t]` guard; see note |

The headline pair is Green (Full) vs Red (WWOnly). `NoSnapFilter` is a secondary probe of
the snapshot guard; its primary purpose is documented as a liveness/over-abort check rather
than a safety violation, and is reported honestly per observed verdict.

The write-skew witness shape (minimal): two keys `kx, ky`; txn `a` reads `{kx,ky}` writes
`{kx}`; txn `b` reads `{kx,ky}` writes `{ky}`; both Begin at version 0, both commit. Full
SSI aborts the second (its write `ky` was read by the first committed txn → rw_A). WWOnly
sees no shared *write* key, accepts both → write-skew → MVSG cycle.

## Run commands (integrator wires into run-all.sh; not edited here)

```
cd proofs/tla
./tools/tlc -metadir states/ssi_green        -config MC_Ssi_Green.cfg            MC_Ssi_Green.tla
./tools/tlc -metadir states/ssi_red_skew     -config MC_Ssi_Red_WriteSkew.cfg    MC_Ssi_Red_WriteSkew.tla
./tools/tlc -metadir states/ssi_red_nosnap   -config MC_Ssi_Red_NoSnapFilter.cfg MC_Ssi_Red_NoSnapFilter.tla
```

Expected: green → "No error" (invariant holds); red_skew → invariant `INV_Serializable`
violated with a 2-txn write-skew trace.

## Boundaries / what is abstracted away (load-bearing honesty)

- **Point keys only.** Ranges (`ReadRange::Prefix/Range`, `record_iter_options`) and the
  `d/d/`/`/d/` scan carve-out are out of scope here (separate slice). The core engine's
  ww/rw_A/rw_B structure is identical for point keys; the range case adds a `contains`
  predicate but not new control flow.
- **Atomic commit critical section.** Real code holds `committed.lock()` across the whole
  check-then-append (`backends/shared.rs:288`), so commits serialize. The model makes
  `Commit` a single atomic action — faithful to that mutex. The non-atomicity between
  `current_version()` (read_version) and the snapshot read at `new_txn`
  (`store.rs:56` then `:59`) is a **separately surveyed** TOCTOU-ish window; this slice
  takes the snapshot == read_version assumption (conservative: a wider real snapshot can
  only cause *more* aborts, never fewer).
- **Pruning** (`committed.len() > 1000` GC at `shared.rs:318`) is not modeled — at the
  bounded N it never triggers, and it only drops records too old to conflict.
- **GC soundness assumed.** The 1000-entry cap assumes no live txn has a snapshot older
  than 1000 commits ago; not exercised at this bound.
