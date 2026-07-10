# PushBacklog — bounded outbound push backlog (TLA+ design)

Models the pusher-side admission of outbound PushLog work for issue **#1099**,
abstracting `crates/p2p/src/sync/coordinator/broadcast.rs` (the per-`(write, peer)`
fan-out feeding the `push_semaphore`) and `SyncShutdownHandle`'s JoinHandle retention.
Companion to `PushLogAdmission.tla`, which models the **hub's** reply decision at its
bounded pending map; this slice isolates the **pusher's** resource discipline before a
request ever leaves the process.

> **Status: the RED configs are current-main behavior.** Broadcast materializes PushLog
> request batches and spawns one tokio task per `(write, peer)` BEFORE the task acquires
> the 8-permit `push_semaphore`; waiting futures retain their payload buffers, so
> resident work grows with total arrivals. `SyncShutdownHandle` retains every completed
> JoinHandle until shutdown. There is no per-peer fairness: one nonresponsive peer's
> sends (30s timeout each) can occupy all permits. The GREEN config is the #1099 fix.

## Mechanism

The fix admits work through a **bounded queue** (item cap AND byte cap) as compact jobs
before anything is spawned; a **fixed pool** of `Workers` tasks drains it; scheduling is
per-peer with an active cap `PerPeerCap < Workers`; queue-full is an **explicit
outcome** — the arrival is rejected into the durable retry ledger, and an identical
`(peer, cid)` coalesces into the already-queued job; only the fixed pool's handles are
retained. Four knobs isolate the four failures of current main:

| Knob | GREEN | RED |
|------|-------|-----|
| `AdmissionMode` | `"BoundedQueue"` — admit/coalesce/reject against the caps | `"SpawnPerItem"` — every arrival becomes resident spawned work |
| `ReleaseMode` | `"Release"` — completion returns the worker slot | `"Leak"` — it does not |
| `HandleMode` | `"FixedWorkers"` — only pool handles retained | `"RetainAll"` — every completed handle retained, never pruned |
| `FairnessMode` | `"PerPeerCap"` — ≤ `PerPeerCap` active per peer | `"Unfair"` — one peer may hold every worker |

## Property

- `INV_QueueBounded` — **resident not-yet-active work is bounded by the queue caps
  (items ≤ `QueueCap`, bytes ≤ `ByteCap`) regardless of total arrivals.** Violated by
  `SpawnPerItem` as soon as arrivals outrun the workers.
- `INV_PermitConservation` — `freeWorkers + activeJobs = Workers`. Violated by `Leak`
  on the first completion.
- `INV_HandlesBounded` — retained handles ≤ `Workers`. Violated by `RetainAll` on the
  first completion.
- `INV_NoSilentLoss` — every arrival is accounted: queued, active, sent, coalesced into
  a queued job, or rejected with a retry-ledger entry.
- `LIVE_HealthyProgress` (temporal, under weak fairness on `StartJob`/`Complete`) —
  **every admitted job for a healthy peer is eventually sent.** Violated by `Unfair`:
  the slow peer's stuck sends hold both workers while an admitted healthy job waits
  forever; holds under `PerPeerCap < Workers`.

## Abstractions

- **`backlog` is admitted-but-not-active work per peer** — the compact bounded queue
  under `BoundedQueue`, the spawned-waiting futures (each retaining its payload buffer)
  under `SpawnPerItem`. The residency bound is the same question in both.
- **Payload size is a per-peer constant** (`Weight`, one heavy peer of weight 2) so the
  byte cap is exercised independently of the item cap.
- **Slow peers' sends never complete.** Stuck-forever is the cleanest starvation
  adversary, and permit conservation is an accounting identity that a stuck holder does
  not disturb — so the fairness property and the conservation property stay independent.
  Real sends time out after 30s; that timeout is a liveness relaxation the model omits
  (modeling it would let `Unfair` limp along and blur the starvation verdict).
- **Rejected arrivals are terminal ledger entries.** The re-push loop they feed is
  `Replicator.tla`'s subject; here rejection only has to be *explicit and accounted*.
- **Round-robin order is not modeled** — the per-peer active cap alone carries the
  starvation-freedom property.

## Configs

| Config | Knob | Verdict | Meaning |
|--------|------|---------|---------|
| `MC_PushBacklog_Green.cfg` | all GREEN knobs | GREEN | #1099 fix: all bounds hold + healthy peers progress |
| `MC_PushBacklog_Red_SpawnPerItem.cfg` | `AdmissionMode="SpawnPerItem"` | RED | current main: spawn before semaphore → `INV_QueueBounded` violated |
| `MC_PushBacklog_Red_PermitLeak.cfg` | `ReleaseMode="Leak"` | RED | completion keeps its worker slot → `INV_PermitConservation` violated |
| `MC_PushBacklog_Red_RetainHandles.cfg` | `HandleMode="RetainAll"` | RED | `SyncShutdownHandle` retention → `INV_HandlesBounded` violated |
| `MC_PushBacklog_Red_NoPeerCap.cfg` | `FairnessMode="Unfair"` | RED | slow peer holds every worker → `LIVE_HealthyProgress` violated |

Each RED config checks only `INV_TypeOK` plus the one property it is meant to violate,
so the failure is attributable.

## Conformance fence

The Rust-side fence for the same invariants is the `crates/p2p/src/sync/push_backlog.rs`
unit tests (queue item/byte caps, coalescing, explicit rejection, fixed-pool handle
retention), the `crates/p2p` worker fault-injection tests (permit conservation across
completion and failure paths), and the fan-out integration test
`tools/integration-test/tests/p2p_admission.rs`
(`outbound_backlog_bounded_under_fanout_with_dead_peer`) — which asserts bounded
residency and healthy-peer progress while one peer is dead.
