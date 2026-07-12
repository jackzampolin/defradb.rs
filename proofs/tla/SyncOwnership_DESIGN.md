# SyncOwnership — hint + receiver-pull ownership transfer (TLA+ design)

Models the **target** sync-ownership protocol of the #1116 convergence design
(`docs/design/sync-ownership-convergence.md`): PushLog demoted to an
idempotent, CID-verified head **hint**; the receiver owns completion through a
bounded, durable want-queue (the pending-DAG registry reframed); the sender
keeps **marker-plus-rederive** state only (Go's `/rep/retry/{id,doc}` shape,
`internal/db/p2p/replicator.go:567-599,863-895`, extended with a collection
scope). Companions: `PushLogAdmission.tla` (ack honesty at capacity) and
`PendingDagRestart.tla` (registration durability) fence the two ack-side
ingredients this model composes; `Convergence.tla` owns fetch/merge liveness
under partition; `PushCoalescing.tla`/`PushBacklog.tla` guard sender machinery
that stage 3 deletes or shrinks.

> **Status: models the target, not current main.** The GREEN config is the
> post-stage-3 protocol. Three of the four RED configs are reachable today:
> `DocKeyedLedger` is current main (#1113), `DupFetch` is pre-#1115 behavior
> at the fetch trigger, and `StaleAckClears` is the stage-3 hazard to avoid;
> `VolatileRegistration` was current main before #1099.

## Mechanism

A local update writes a durable scope marker and sends the current head as a
hint. The receiver: already-merged → ack (fast path, no state); missing links
→ durable registration in the bounded want-queue → ack; queue full → nack.
The ack **transfers** the obligation: the sender clears its marker (guarded —
only if the acked version is still the scope's newest head), the receiver's
paced fetch completes it. Re-hints rederive the current head from the store,
never a recorded CID, and are free to repeat — idempotence is exercised by
construction, not asserted.

Four knobs isolate four ways to get ownership wrong:

| Knob | GREEN | RED |
|------|-------|-----|
| `SenderMode` | `"MarkerRederive"` — markers for docs AND collection commits | `"DocKeyedOnly"` — collection scopes can't enter the ledger (`push_worker.rs:84-93`, `peerstore.rs:199-203`) |
| `RegisterMode` | `"Durable"` — registration survives receiver crash (#1099) | `"Volatile"` — dies with the process |
| `FlightMode` | `"SingleFlight"` — one active fetch per scope | `"Dup"` — duplicates overlap (#630 parade) |
| `AckGuardMode` | `"HeadCurrent"` — ack clears marker only for the current head | `"Unguarded"` — stale ack clears it |

## Properties

- `INV_ObligationConservation` — **the newest version of every scope is
  merged, or tracked by a durable sender marker, a hint in flight, or a
  durable receiver registration.** The heart of the design: no reachable
  state where a behind scope is nobody's problem. Violated by
  `DocKeyedOnly` (3 steps: update collection → drop hint), by `Volatile`
  (4 steps: update → hint → register+ack → crash), and by `Unguarded`
  (update → update → stale ack clears → newer hint drops).
- `INV_SingleFlight` — at most one active fetch per scope (`queue.rs:12-22`;
  Go `processQueue`). Violated by `Dup` at the second overlapping start.
- `INV_ReceiverQueueBounded` — the want-queue never exceeds `Cap`; overflow
  nacks and the sender marker survives (receiver pacing, not sender
  admission).
- `INV_SenderMarkersOnly` — sender durable state is a set of scopes: no
  versions, CIDs, or payloads. Holds by construction; stated so the green
  run records that markers alone SUFFICE.
- `LIVE_EventualCurrency` (GREEN, under `FairSpec`) — `<>[]` every scope
  merged at its newest head, with unconstrained hint drops, capacity nacks,
  free re-hints, and one receiver crash. `<>[]` not `<>`: Init trivially
  satisfies currency. Fairness is WF on re-hint/fetch-start plus **SF** on
  hint processing and fetch completion — the drop adversary keeps disabling
  arrivals, so WF alone would let the network eat every hint forever
  (verified: the no-fairness probe fails liveness).

## Abstractions

- One sender, one receiver; peer identity dropped — every replicator edge is
  an instance of this model. Fan-in contention is `PushLogAdmission`'s
  subject; here `Cap` only has to make nacks reachable.
- Heads are monotone naturals per scope. "Rederive at send time" = every
  (re)hint carries the **current** version; there is no way to express
  replaying a stored CID, which is exactly the point.
- Fetch reliability, provider rotation, and the per-root pacing clock
  (#1095/#1112) are abstracted into fair `CompleteFetch` — this model owns
  WHO holds the obligation, not how fast it drains (`Convergence.tla` owns
  the drain under partition).
- The crash is one-shot, receiver-only, atomic with restart (as in
  `PendingDagRestart`).
- Gossip is not modeled: it is already hint-shaped on both sides and carries
  no obligation (fire-and-forget; conservation is the replicator edge's
  contract).

## Configs

| Config | Knob | Verdict | Meaning |
|--------|------|---------|---------|
| `MC_SyncOwnership_Green.cfg` | all GREEN | GREEN | target protocol: all four invariants + eventual currency (864 states, complete space) |
| `MC_SyncOwnership_Red_DocKeyedLedger.cfg` | `SenderMode="DocKeyedOnly"` | RED | current main: collection scope lost on first drop/nack (#1113) |
| `MC_SyncOwnership_Red_VolatileRegistration.cfg` | `RegisterMode="Volatile"` | RED | ack without durable registration: crash destroys the transferred obligation |
| `MC_SyncOwnership_Red_DupFetch.cfg` | `FlightMode="Dup"` | RED | no single-flight: overlapping fetches for one head (#630) |
| `MC_SyncOwnership_Red_StaleAckClears.cfg` | `AckGuardMode="Unguarded"` | RED | stage-3 hazard: superseded-head ack erases the newer obligation |

Each RED checks `INV_TypeOK` plus only the property it violates, so failures
stay attributable.

## Conformance fence

Stage-2/3 Rust fences for the same invariants (from the staged plan): the
#1108 storm harness assertions (N same-CID announcements → one sync;
re-announcement of a merged head ~free; sender bytes per re-announcement
~constant), the defra-agent#696 empty-store genesis repro staying calm, the
collection-commit regression test (collection push rejected once → replayed
via `/rep/retry/col` → converges), and the existing
`p2p_admission_restart.rs` durability test which keeps fencing the
`VolatileRegistration` red. go-compat CI (mixed Rust↔Go clusters +
`parity_counter_storm_mixed`) fences the wire side of the demotion.
