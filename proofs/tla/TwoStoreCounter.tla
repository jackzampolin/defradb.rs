---- MODULE TwoStoreCounter ----
\* Counter materialization under concurrent local-write vs remote-merge, abstracting
\* crates/db-merge/src/merge_handler/counter.rs (process_counter_delta /
\* process_counter_delta_in_txn: reconcile_int64(blob) -> merge(+delta) -> blob:=acc)
\* and the local-write path in the `db` crate's doc_mutator. Anchors in the
\* TwoStoreCounter_DESIGN.md (and project memory same_doc_merge_storm_undercount).
\*
\* THE BUG (found 2026-06-15, storm probe): a PCounter's value lives in TWO stores —
\* the materialized document `blob` (advanced by LOCAL writes AND by merges) and the
\* CRDT accumulation store `acc` (advanced ONLY by merges). Each merge RECONCILES
\* `acc := blob` then applies its delta then writes `blob := acc`. That is a
\* read-modify-write of `blob`. A concurrent LOCAL increment touches only `blob`,
\* on a DIFFERENT key, so the storage layer's transactional conflict detection
\* never fires between the two — the merge commits a stale-reconciled value and
\* CLOBBERS the local increment. The DAG converges (every delta block is merged-
\* marked) yet the materialized sum is short by the clobbered increments.
\*
\* HOW GO AVOIDS IT (parity): Go keeps the value in ONE key (counter.go
\* incrementValue RMW on key.WithValueFlag()); BOTH local writes and merges RMW
\* that same key, so a concurrent local-write and merge conflict at commit and the
\* loser retries against the fresh value. No two-store split, no clobber.
\*
\* One knob selects the buggy split vs. the Go-parity fix:
\*   StoreMode = "Split"   - reconcile-from-blob with NO cross-store conflict; the
\*                           merge commits its stale snapshot+delta unconditionally,
\*                           clobbering an interleaved local write          [RED]
\*           = "Unified"   - the merge's RMW is conflict-checked: it commits only if
\*                           `blob` is unchanged since reconcile, else it re-reconciles
\*                           against the fresh value and retries (= Go's shared-key
\*                           txn-conflict / a per-doc lock spanning both paths) [GREEN]
\*
\* ORACLE: `doneCount` counts every increment whose effect has been committed
\* (a local apply, or a merge commit). It is independent of the stores. The headline
\* safety property is that the materialized value equals that count — no increment is
\* lost. `acc` is carried to mirror the code but the user-visible value is `blob`.
\*
\* HOW THE FIX REALIZES GREEN (#1021): the GREEN "Unified" mode here abstracts the
\* conflict-free RMW as a re-check-and-retry. The implementation achieves the SAME
\* no-lost-update invariant via a per-doc write lock (crates/db/src/doc_write_queue.rs,
\* shared by the local-write and merge paths; see MergeQueue.tla) PLUS reconcile being
\* init-if-absent (PCounter migrate-via-max), so a local write and a merge on one doc
\* never interleave their store RMW. The conflict-retry and the lock are two
\* realizations of the same GREEN safety story; this model checks the invariant, and
\* MergeQueue.tla checks the per-doc serialization that the code uses to enforce it.
EXTENDS Naturals, FiniteSets

CONSTANTS
  LocalOps,   \* finite set of local-write increment ids (db crate doc_mutator path)
  RemoteOps,  \* finite set of remote-merge increment ids (db-merge counter merge path)
  StoreMode,  \* "Split" | "Unified"   — the two-store split (lost-update axis)
  Dedup       \* "On" | "Off"          — the per-block merged-set guard (double-apply axis)

ASSUME LocalOps \cap RemoteOps = {}
ASSUME StoreMode \in {"Split", "Unified"}
ASSUME Dedup \in {"On", "Off"}

Ops == LocalOps \cup RemoteOps

VARIABLES
  blob,       \* Nat: materialized document value (queries read this; local writes RMW it)
  acc,        \* Nat: counter accumulation store (value_key); merges RMW it
  pc,         \* [Ops -> phase] per-increment control state
  snap,       \* [RemoteOps -> Nat] value snapshotted by a merge's reconcile step
  doneCount   \* Nat: ORACLE — increments committed so far (must equal blob)

vars == <<blob, acc, pc, snap, doneCount>>

\* local ops: "todo" -> "done"; remote ops: "todo" -> "recon" -> "done".
Phases == {"todo", "recon", "done"}

TypeOK ==
  /\ blob \in Nat
  /\ acc \in Nat
  /\ pc \in [Ops -> Phases]
  /\ snap \in [RemoteOps -> Nat]
  /\ doneCount \in Nat

Init ==
  /\ blob = 0
  /\ acc = 0
  /\ pc = [o \in Ops |-> "todo"]
  /\ snap = [o \in RemoteOps |-> 0]
  /\ doneCount = 0

\* ---- Local write: blob += 1, NOT touching acc (the two-store gap). No lock. ----------
LocalApply(o) ==
  /\ o \in LocalOps
  /\ pc[o] = "todo"
  /\ blob' = blob + 1
  /\ doneCount' = doneCount + 1
  /\ pc' = [pc EXCEPT ![o] = "done"]
  /\ UNCHANGED <<acc, snap>>

\* ---- Merge step 1: reconcile the accumulation store up to the current blob. ----------
\* counter.rs: `counter.reconcile_int64(datastore, current_blob_value)`.
MergeReconcile(o) ==
  /\ o \in RemoteOps
  /\ pc[o] = "todo"
  /\ snap' = [snap EXCEPT ![o] = blob]
  /\ acc' = blob
  /\ pc' = [pc EXCEPT ![o] = "recon"]
  /\ UNCHANGED <<blob, doneCount>>

\* ---- Merge step 2: apply the delta and write the value back. -------------------------
\* counter.rs: `counter.merge(+delta)` then materialize blob := acc.
MergeCommit(o) ==
  /\ o \in RemoteOps
  /\ pc[o] = "recon"
  /\ IF StoreMode = "Split"
       \* RED: commit the stale reconciled snapshot + delta unconditionally. If a
       \* local write bumped `blob` since reconcile, this OVERWRITES it (lost update).
       THEN /\ acc' = snap[o] + 1
            /\ blob' = snap[o] + 1
            /\ doneCount' = doneCount + 1
            /\ pc' = [pc EXCEPT ![o] = "done"]
            /\ UNCHANGED snap
       \* GREEN: conflict-checked RMW. Commit only if `blob` is unchanged since
       \* reconcile; otherwise re-reconcile against the fresh value and retry. This is
       \* Go's shared-key txn-conflict (or a per-doc lock spanning local+merge).
       ELSE IF blob = snap[o]
              THEN /\ acc' = blob + 1
                   /\ blob' = blob + 1
                   /\ doneCount' = doneCount + 1
                   /\ pc' = [pc EXCEPT ![o] = "done"]
                   /\ UNCHANGED snap
              ELSE /\ snap' = [snap EXCEPT ![o] = blob]
                   /\ acc' = blob
                   /\ UNCHANGED <<blob, doneCount, pc>>

\* ---- Re-delivery (#4935): a remote delta is DELIVERED A SECOND TIME. ----------------
\* A counter field block delivered as its own PushLog head (or re-delivered via two
\* channels) is handed to the merge handler a SECOND time. Go's `coreblock.ProcessBlock`
\* applies the delta unconditionally; idempotency rests entirely on the blockstore
\* merged-set / is_merged guard. This action models the re-delivery itself; the guard is
\* modeled INLINE as a no-op transition (not a disabled action) so BOTH settings exercise
\* the re-delivery path:
\*   Dedup="On"  (GREEN): is_merged(cid) finds the block already in the merged-set and
\*                        SUPPRESSES the re-apply -> UNCHANGED. This no-op is what makes
\*                        GREEN non-vacuous on the double-apply axis (the guard actively
\*                        fires, rather than the re-delivery being structurally absent).
\*   Dedup="Off" (RED):   no guard -> the re-merge re-applies the +1 (blob/acc climb)
\*                        WITHOUT a new increment (doneCount unchanged) -> blob exceeds
\*                        the committed count.
\* The counter's idempotency axis, orthogonal to the two-store split above; the Lean twin
\* is `CounterReconcile.counter_not_idempotent`.
MergeRedeliver(o) ==
  /\ o \in RemoteOps
  /\ pc[o] = "done"      \* its delta was already applied once; now re-delivered
  /\ IF Dedup = "On"
       \* GREEN: the merged-set guard suppresses the re-apply (a no-op). The re-delivery
       \* is exercised; dedup actively prevents the double-count.
       THEN UNCHANGED <<blob, acc>>
       \* RED: no guard -> the delta is applied a second time.
       ELSE /\ blob' = blob + 1
            /\ acc'  = acc + 1
  /\ UNCHANGED <<pc, snap, doneCount>>

Next == \E o \in Ops : LocalApply(o) \/ MergeReconcile(o) \/ MergeCommit(o) \/ MergeRedeliver(o)

Spec == Init /\ [][Next]_vars

\* ===================================================================================
\* SAFETY: the materialized value is EXACT — it equals the number of committed
\* increments, neither short nor over. Two independent hazards can break it:
\*   * lost update  (blob < doneCount): RED under StoreMode="Split" — a merge's
\*     reconcile-from-blob clobbers an interleaved local increment.
\*   * double-apply (blob > doneCount): RED under Dedup="Off" — a re-delivered delta
\*     is merged twice (the #4935 field-block-as-head re-merge).
\* GREEN (StoreMode="Unified", Dedup="On") holds equality in every reachable state.
\* ===================================================================================
INV_NoLoss == blob >= doneCount        \* no increment dropped (Split breaks this)
INV_NoDoubleApply == blob <= doneCount \* no increment counted twice (Dedup=Off breaks this)
INV_Exact == blob = doneCount          \* both directions (the headline)

INV_TypeOK == TypeOK
====
