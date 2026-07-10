# PendingDagRestart — pending-DAG durability across hub restart (TLA+ design)

Models the hub-side pending-DAG registration's survival of a process restart for issue
**#1099**, abstracting `crates/p2p/src/sync/manager/process/pushlog.rs`
(`insert_pending_dag`) plus the pusher's persisted retry ladder. Companion to
`PushLogAdmission.tla` (#1088 W1): that slice fixed the **reply decision** at capacity —
kept here as `HubOverflowNack` — while this slice asks what survives a hub **crash**
after an *honest* success reply.

> **Status: the RED config is current-main behavior.** Pending-DAG registrations live
> only in the hub's in-memory map. A hub crash after the success ack destroys the
> registration while the pusher has already deleted its persisted retry record — silent,
> permanent loss. The GREEN config persists registrations and restores them on restart.

## Mechanism

An inbound PushLog with missing links is **registered** in the hub's bounded pending-DAG
map (`SyncConfig::max_pending_dags`) and **success-acked**; the pusher then deletes its
persisted retry record (terminal — it never re-pushes unless an unrelated later update
arrives). The ack is honest exactly as long as the registration outlives the process.
The fix persists the registration at admission time; on restart the durable set is
restored into the pending map and re-driven to completion by Bitswap.

One knob:

- `RecoveryMode = "Persist"` — `HubAdmit` persists alongside the in-memory
  registration; `Restore` reloads after a crash. [GREEN]
- `RecoveryMode = "ProcessLocal"` — current behavior: registrations die with the
  process. [RED]

## Property

`INV_AckBacked` — **a success ack is always backed by hub state that can still complete
the doc: merged, registered in memory, or durably persisted.** `ProcessLocal` violates
it at the crash step (minimal counterexample: `Send → HubAdmit → Crash`); `Persist`
preserves it in every reachable state, crash window included.

GREEN additionally checks `EventuallyAllMerged` under per-doc weak fairness plus
`WF(Restore)`: even with a crash, every pushed doc eventually merges — restore re-drives
what the ack promised.

## Abstractions

- **As in `PushLogAdmission`**: docs are the unit, "the pusher" is the per-doc
  ack/retry record, and how a push comes to have missing links is irrelevant.
- **The pusher process survives the hub crash** — its records are in another process.
- **The crash is one-shot and atomic with restart** (the hub is immediately back up);
  a modeled down-window adds states without adding durability-relevant behaviors.
- **Under `Persist` the admission capacity check runs against the durable set** — the
  superset of the in-memory map inside the crash window — so the `Cap` bound survives
  `Restore` (`TypeOK` keeps `Cardinality(pending) <= Cap` exact).

## Configs

| Config | Knob | Verdict | Meaning |
|--------|------|---------|---------|
| `MC_PendingDagRestart_Green.cfg` | `RecoveryMode="Persist"` | GREEN | registrations persisted + restored: invariant + liveness hold |
| `MC_PendingDagRestart_Red_ProcessLocal.cfg` | `RecoveryMode="ProcessLocal"` | RED | crash after success-ack → `INV_AckBacked` violated |

## Conformance fence

The Rust-side fence for the same invariant is the restart integration test
`tools/integration-test/tests/p2p_admission_restart.rs`
(`hub_restart_recovers_success_acked_pending_dags`) — kill and restart the hub after it
success-acks a missing-links push, then assert the doc still merges — plus the
`crates/p2p` unit tests around pending-DAG persistence
(`sync_manager_tests.rs::pending_persistence`). Note the record-lifetime
refinement the model's atomic `Resolve` glosses over: in Rust the durable
record is deleted only when the root is **successfully marked merged** —
never at DagReady emission, TTL eviction, or `clear_pending_dag` — so the
crash window between queueing and completing the merge stays covered.
