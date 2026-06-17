# InteractiveTxnCounter — interactive-txn counter guard lifecycle (TLA+ design)

Models the guard-acquisition **lifecycle** of the interactive / explicit-transaction
counter mutator for issue **#1044** (follow-up to #1041). Companion to `MergeQueue.tla`,
which models the per-doc serialization mechanism itself; this slice isolates **when** the
process-wide `batch_gate` is held over the interactive transaction's user-controlled
lifetime.

> **Status: TARGET design, not yet fully implemented.** #1041 shipped a correct (no
> lost-increment) interactive counter path, but it holds the process-wide gate across the
> whole user-controlled txn lifetime (the liveness HIGH this issue removes). The GREEN
> behavior modeled here — gate taken only at a bounded commit-time finalize — is the #1044
> redesign that is not all in the tree yet (the blob-mirror-at-commit coupling described in
> the issue is the hard part). The RED config reproduces the #1041 lifecycle.

## Mechanism (corrected)

The **bounded process-wide `batch_gate` is the deadlock-freedom mechanism.** Held only
*briefly* during a bounded acquisition phase, it serializes the acquire phase of every
multi-doc acquirer, so two of them never acquire per-doc guards concurrently and a
circular wait cannot form.

**Sorted acquisition is NOT the mechanism.** Because the gate serializes the acquisition
phase, two multi-doc acquirers never run concurrently and sorted order never gets a chance
to matter — sorted order is incidental/defensive *given* the gate. (A prior review/Codex
P2 correctly found the earlier model's "sorted acquisition prevents deadlock" claim is
false-as-modeled; that claim is removed.)

**Why the gate is required:** some acquirers are **irreducibly incremental** — the
`BatchMutator` (and the interactive txn before #1044) discover their docs one mutation at a
time and **cannot pre-sort** their doc set, so a sorted discipline is simply unavailable to
them. The gate is the compensator that protects the system from these incremental
acquirers. **Go has no such gate** because Go never holds more than one per-doc lock at a
time (pure storage-txn isolation); the Rust gate exists only because Rust holds multiple
per-doc guards at once.

## Property

> The interactive (explicit-transaction) counter mutator holds the process-wide
> `batch_gate` **only during a bounded commit-time finalize**, never across the
> user-controlled active/idle transaction lifetime; the **bounded gate** (not the acquire
> order) keeps the design **deadlock-free**; and a local interactive RMW is never in a
> doc's critical section concurrently with a merge on the same doc (the per-doc guard
> excludes them).

Two knobs:

- `GateMode = "On"` (GREEN): multi-doc acquirers take the gate (real system). `"Off"`
  (RED, `Red_NoGate`): they skip the gate and acquire concurrently — with the
  arbitrary-order incremental batch actor racing the finalize, they grab docs in opposite
  orders and **deadlock**, proving the gate is load-bearing.
- `InteractiveGate = "AtCommitOnly"` (GREEN, #1044) takes the gate inside the atomic
  `IBeginFinalize`/finalize action only. `"AcrossLifetime"` (RED, the #1041 path) grabs the
  gate on the first counter write (`IGoIdle`) and holds it across active/idle until commit —
  exactly the design #1044 removes.

## Source anchors (read the real code, not the abstraction)

These are the **#1044 TARGET** symbols. Where noted they are not yet all implemented.

| Symbol in model | Rust source | What it abstracts |
|---|---|---|
| interactive actor, `iPhase`, `IGoIdle`/`IGoActive`/`IBeginFinalize`/`IFinalizeCommit` | `crates/db/src/doc_mutator.rs` (`DbDocMutator`) | the explicit-txn mutator; today it acquires the gate + per-doc guards incrementally (`doc_mutator.rs:113-140`, `acquire_batch_gate()` then `acquire(doc_id)`), holding them on `DbTxn` across the lifetime — **#1044 moves this to commit-time** |
| pending counter deltas recorded during the txn (no guard yet) | `crates/db/src/txn.rs` (`DbTxn` — counter pending-ops; **target: add per-doc pending counter deltas**, holding no guard while active/idle) | "during the txn: record deltas, take no guard"; the `iHeld`/finalize split |
| commit-time finalize (gate + sorted guards + RMW + release) | `crates/db/src/txn.rs` `DbTxn::commit` (`txn.rs:540`) — **target: sorted acquire of touched-counter-doc guards at commit**, RMW under them, release on durable commit | `IBeginFinalize` -> `IAcquire` (sorted) -> `IFinalizeCommit` -> `IExitCrit` |
| process-wide `batch_gate` (`gate`), `acquire_batch_gate` / `try_acquire_batch_gate` | `crates/db/src/doc_write_queue.rs:82` `acquire_batch_gate`, `:91` `try_acquire_batch_gate` | the single process-wide mutex serializing the guard-acquisition phase of multi-doc acquirers |
| per-doc guard (`lockOwner`), `acquire` | `crates/db/src/doc_write_queue.rs:61` `DocWriteQueue::acquire` | per-doc `OwnedMutexGuard`; same key blocks, different keys parallel |
| arbitrary-order incremental acquire (batch actor `BAcquire`) | `crates/db-merge/src/merge_handler/batch.rs` (`try_batch_merge`) / `create_many` / the `BatchMutator` | the irreducibly-incremental multi-doc acquirer the **gate** protects against (it discovers docs one at a time, so it cannot pre-sort — modeled as arbitrary-order acquire) |
| idle reaper bound (~600s) the RED hold is exposed to | `crates/db/src/txn_registry.rs:39` `DEFAULT_TRANSACTION_IDLE_TIMEOUT = 600s`, `:252` cleanup | why an across-lifetime gate hold is a node-wide stall: it can persist for the whole idle window |
| single-doc merge (`mPhase`, `MAcquire`/`MRelease`) | `crates/db-merge/src/merge_handler/counter.rs` (`process_counter_delta`, `self.merge_queue.acquire(&doc_id_str)`) | a same-doc merge contending on the per-doc guard (no gate) |

## Invariants

| Name | Plain English | Falsified by |
|---|---|---|
| `INV_GateBoundedHold` | the process-wide gate is never held while the interactive actor is in `active`/`idle` (non-finalize) | `InteractiveGate="AcrossLifetime"` (RED) — gate held across idle |
| deadlock-freedom | no deadlock state reachable (TLC `CHECK_DEADLOCK TRUE`) | `GateMode="Off"` (RED) — the **gate** is what guarantees it; with the gate removed the arbitrary-order batch actor and the finalize deadlock |
| `INV_NoLocalMergeInterleave` | a local interactive RMW and a merge are never both in the critical section on one doc (carried from `MergeQueue.tla`) | structurally held by the per-doc guard (guard held ⇔ `iInCrit`) |
| `INV_SingleGuardOwner` | a per-doc guard has at most one holder | sanity / lock model witness |

## Scenarios, configs, verdicts

Run from `proofs/tla` (the module argument is the real `.tla` filename
`MC_InteractiveTxnCounter_Common.tla`; the `-config` may be MC-prefixed):

```bash
# GREEN (#1044): gate On + gate taken only at commit-finalize. Deadlock-free; INV_GateBoundedHold holds.
./tools/tlc -config MC_InteractiveTxnCounter_Green.cfg              MC_InteractiveTxnCounter_Common.tla
# RED (gate load-bearing): gate Off -> arbitrary-order batch vs finalize circular-wait DEADLOCK.
./tools/tlc -config MC_InteractiveTxnCounter_Red_NoGate.cfg         MC_InteractiveTxnCounter_Common.tla
# RED (#1041 old path): gate held across the user-controlled idle lifetime ->
#   INV_GateBoundedHold violated (witness: gate="itxn" while iPhase="idle").
./tools/tlc -config MC_InteractiveTxnCounter_Red_AcrossLifetime.cfg MC_InteractiveTxnCounter_Common.tla
```

| Config | Knobs | Checks | Expected | Observed |
|---|---|---|---|---|
| `MC_InteractiveTxnCounter_Green` | `GateMode="On"`, `AtCommitOnly`, `CHECK_DEADLOCK TRUE` | all 4 invariants | GREEN | No error; 76 distinct states, no deadlock |
| `MC_InteractiveTxnCounter_Red_NoGate` | `GateMode="Off"`, `AtCommitOnly`, `CHECK_DEADLOCK TRUE` | deadlock-freedom | RED | `Deadlock reached`; `lockOwner=<<"itxn","batch">>`, `iHeld={1}` (finalize) ∧ `bHeld={2}` (acquiring), `gate="none"` |
| `MC_InteractiveTxnCounter_Red_AcrossLifetime` | `GateMode="On"`, `AcrossLifetime` | `INV_GateBoundedHold` | RED | violated: `gate="itxn"` ∧ `iPhase="idle"` |

## Modeling boundaries (honest reach)

- **Bounded instances.** 2 docs (both touched by the interactive txn and the batch
  acquirer, so they contend), 1 single-doc merge on doc 1, one interactive txn, one batch
  acquirer. The witnessing shapes are minimal: contention on a shared doc set, an idle
  interactive state, a same-doc merge.
- **Batch actor acquires in ARBITRARY order.** `BAcquire` picks any free remaining doc
  (`\E nd \in Remaining(...)`), modeling `BatchMutator`'s irreducibly-incremental discovery
  (it cannot acquire in a sorted order it does not yet know). The interactive finalize uses
  `MinDoc` (`<`) only as an incidental/defensive order — it is **not** the deadlock-freedom
  mechanism, and with the gate Off this fixed order vs. the batch's arbitrary order is
  precisely what lets a circular wait form.
- **No counter VALUE / no-lost-increment here.** Correctness of the RMW value (no lost
  update, no double-apply) is the subject of `TwoStoreCounter.tla` and `MergeQueue.tla`.
  This slice models only the **guard lifecycle** (acquisition timing, bounded gate hold,
  release-after-durable-commit, deadlock-freedom). The interactive RMW is modeled as a
  critical-section enter/exit, not an arithmetic effect.
- **Finalize is bounded; the user lifetime is not.** `IGoIdle`/`IGoActive` model the
  unbounded user think-time; `IBeginFinalize` -> `IAcquire`* -> `IFinalizeCommit` ->
  `IExitCrit` is the bounded commit action. The gate is held only across the latter under
  GREEN — that is the whole property.
- **Model ≠ code.** Anchored by symbol above; no automated conformance harness. Several
  anchors are #1044 TARGET symbols not yet in the tree (noted inline).

## Findings

1. **The bounded-hold property is non-vacuous.** The RED config (`AcrossLifetime`)
   reaches `gate="itxn"` while `iPhase="idle"`, falsifying `INV_GateBoundedHold` — TLC
   exhibits exactly the #1041 lifecycle the issue describes (an idle interactive txn
   holding the process-wide gate). The GREEN config holds the invariant in every reachable
   state, so the property genuinely distinguishes the two designs.
2. **The bounded gate — not the acquire order — is the deadlock-freedom mechanism.** The
   `Red_NoGate` config (`GateMode="Off"`, only the gate removed vs. GREEN) reaches a
   `Deadlock reached` state: `lockOwner=<<"itxn","batch">>` with the finalize holding doc 1
   and waiting on doc 2 while the arbitrary-order batch holds doc 2 and waits on doc 1 — a
   circular wait. GREEN (`GateMode="On"`, `CHECK_DEADLOCK TRUE`) finds no deadlock because
   the gate serializes the acquisition phase so the two acquirers never run concurrently.
   This proves the gate is load-bearing and corrects the earlier (false) claim that sorted
   order is what prevents deadlock.
3. **The per-doc guard, not the gate, is what serializes local-vs-merge.**
   `INV_NoLocalMergeInterleave` holds in GREEN even though the gate is released before the
   RMW critical section ends, because the per-doc guard is held across the RMW — confirming
   the bounded gate hold does not weaken the counter-correctness serialization that
   `MergeQueue.tla`/`TwoStoreCounter.tla` rely on.
