# PendingDagQuarantine — terminal-failure disposition for pending-DAG roots (TLA+ design)

Models the disposition of a pending-DAG root once its merge is retried and comes back
**deterministically rejected** on the block's own content (e.g. a unique-index
violation), for issue **#1128**. Companion to `PendingDagRestart.tla` (does a
registration *survive* a hub restart?) and `SyncOwnership.tla` (who *owns* a
registration's completion) — both ask about the registration's lifecycle; this model
asks what happens to it once a retry comes back with a verdict that will never change.

> **Status: the RED 1 config is current-main behavior.** `MergeOutcome::Rejected` was
> treated as a retryable skip, so a root whose merge fails on content (not timing) is
> swept forever by the retry clock and the resync sweep. The GREEN config quarantines
> it durably instead — write-first ordering so a crash mid-disposition self-heals
> instead of losing the quarantine. RED 2 is the fix's own failure mode to avoid:
> over-eager quarantine of a doc that only failed *transiently*.

## Mechanism

A registered pending-DAG root is re-driven by a sweep (retry clock, resync, peer
reconnect — the trigger is irrelevant, as in `PendingDagRestart`). **Poison** docs (a
doc whose content will never merge, e.g. a unique-index collision with data already
committed) are rejected on every attempt —
`crates/db-merge/src/merge_handler/composite.rs:261-284` converts
`MergeError::UniqueConstraintViolation` into `MergeOutcome::Rejected` (defined at
`crates/p2p/src/sync/merge.rs:89-97`), which
`crates/p2p/src/sync/replication/handlers.rs:353-362` routes to
`SyncManager::quarantine_pending_dag`. **Sound** docs merge, possibly after one
transient hiccup first (network blip, lock contention — not a function of content).

`quarantine_pending_dag` (`crates/p2p/src/sync/manager/process/pending_dag.rs:838-880`)
writes the durable quarantine record **before** deleting the live
`/p2p/pending_dag/` record, so a crash in that window leaves both records on disk
rather than losing the live one; the resync sweep's `is_quarantined` check
(`pending_dag.rs:712-723`) treats a leftover live record for an already-quarantined
root as cleanup, not a recovery obligation — the crash window self-heals instead of
re-driving a merge now known to fail forever.

One knob, three settings — each isolates one way to get the disposition wrong:

| Knob | GREEN | RED 1 | RED 2 |
|------|-------|-------|-------|
| `QuarantineMode` | `"Quarantine"` — `Rejected` quarantines durably; a transient failure leaves the root registered | `"RetryForever"` — today's bug: `Rejected` is treated as a retryable skip | `"QuarantineTransient"` — the forbidden overcorrection: a *sound* doc's transient failure also quarantines it |

## Properties

- `INV_NoSilentDrop` — **every doc is registered, quarantined, or merged.** Holds by
  construction in all three configs; stated because it is the property the quarantine
  record exists to preserve at the granularity that matters. Neither RED config loses a
  doc from the ledger — they get the *disposition* wrong (a liveness failure), not the
  accounting (a safety failure).
- `LIVE_SoundEventuallyMerged` (GREEN, under `FairSpec`) — every sound doc eventually
  merges and stays merged. Violated by `QuarantineTransient` (RED 2, 3 steps: transient
  failure quarantines the doc before it ever gets a chance to retry).
- `LIVE_PoisonQuiesces` (GREEN, under `FairSpec`) — every poison doc eventually
  quarantines and stays quarantined — the #1128 fix's headline promise: a
  deterministically failing merge is disposed of, not retried indefinitely. Violated by
  `RetryForever` (RED 1): the counterexample is a genuine infinite sweep loop, not a
  starvation artifact — `attempts` saturating at `MaxAttempts` keeps the state space
  finite while the loop (never reaching `"quarantined"`) still witnesses the bug.

Each RED checks `INV_TypeOK` + `INV_NoSilentDrop` (both hold everywhere) plus only the
one liveness property it violates, so failures stay attributable.

## Abstractions

- As in `PendingDagRestart`, the sweep trigger (retry clock tick, resync sweep, peer
  reconnect) is irrelevant — only the content-determined outcome matters.
- `"registered"` collapses `PendingDagRestart`'s in-memory/durable distinction — that
  durability question is already fenced there; this model owns only the
  terminal-vs-retryable disposition and assumes every registered root is durable.
- One optional transient failure per sound doc, before it succeeds, is enough to give
  `QuarantineTransient` something to bite without needing an unbounded retry ladder —
  `SweepSoundTransientFail` can fire at most once per doc (guarded by `transientDone`).
- `attempts` is a **saturating** counter capped at `MaxAttempts` rather than an
  unbounded one. `RetryForever`'s bug is "sweeps forever, never disposes"; saturating
  keeps the state space finite (TLC needs a complete, closed graph to check `<>[]`)
  while the never-disposes behavior still shows up as an infinite sweep loop under
  fairness — the loop *is* the counterexample TLC reports.
- Fairness is minimal: `WF(SweepPoisonReject(p))` and `WF(SweepSoundSucceed(s))` only.
  Sound docs' optional transient failure is never required to fire — it is optional by
  construction (a doc may merge on its first attempt). This is deliberately the
  smallest fairness under which GREEN's two liveness properties are real (see
  Anti-vacuity below), matching `SyncOwnership`'s "minimal fairness that makes the green
  liveness real" convention.

## Anti-vacuity

Re-running the GREEN config with `SPECIFICATION Spec` (no fairness) in place of
`FairSpec` fails `LIVE_SoundEventuallyMerged`: TLC finds a 3-state counterexample where
the sound doc's `SweepSoundTransientFail` fires once and then the trace stutters
forever, and flags the standard warning that the counterexample is an artifact of
missing fairness. This confirms the liveness properties have teeth — they are not
vacuously true from `Init`, and `FairSpec`'s two `WF` clauses are load-bearing.

## Configs

| Config | Knob | Verdict | Meaning |
|--------|------|---------|---------|
| `MC_PendingDagQuarantine_Green.cfg` | `QuarantineMode="Quarantine"` | GREEN | rejection quarantines durably, transient failure retries: no silent drop, all docs eventually disposed correctly (8 states, complete space) |
| `MC_PendingDagQuarantine_Red_RetryForever.cfg` | `QuarantineMode="RetryForever"` | RED | current main: poison root swept forever, `LIVE_PoisonQuiesces` violated (16 states) |
| `MC_PendingDagQuarantine_Red_OvereagerQuarantine.cfg` | `QuarantineMode="QuarantineTransient"` | RED | forbidden overcorrection: sound doc's transient failure quarantines it, `LIVE_SoundEventuallyMerged` violated (6 states) |

## Conformance fence

The Rust-side fence: `pending_dag.rs`'s
`quarantine_pending_dag_moves_live_record_and_clears_in_memory_entry` and
`quarantine_pending_dag_synthesizes_record_when_no_durable_record_exists` cover the
write-then-delete ordering and the never-fails-for-lack-of-provenance guarantee this
model assumes as `SweepPoisonReject`'s single atomic transition;
`resync_deletes_live_leftover_of_quarantined_root_without_redriving` exercises the
crash-window leftover the `is_quarantined` check exists to clean up without
re-registering. `crates/p2p/src/sync/replication/mod.rs`'s `Rejected`-in-a-batch
regression test is the sibling fence for the batch-merge path feeding the same
disposition.
