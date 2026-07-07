# PushLogAdmission — hub PushLog admission control (TLA+ design)

Models the hub-side admission decision for incoming PushLog pushes for issue **#1088**
(slice 1, W1), abstracting `crates/p2p/src/sync/manager/process/pushlog.rs`
(`process_pushlog` + `insert_pending_dag`) and the reply seams in
`crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` (gossip-token and two-stream
paths). Companion to `Replicator.tla`, which models the pusher's resumable retry
lifecycle over a source→target edge; this slice isolates the **hub's reply decision**
when its bounded pending-DAG map is full.

> **Status: the RED config is current-main behavior.** The hub-side backpressure nacks
> delivered for #592 were removed by `fa4a84f7` ("align iroh replay and shutdown with go
> model", 2026-04-18) while the pusher-side consumer of those nacks
> (`broadcast.rs::send_ordered_pushlogs_via_transport`, #843) stayed live and tested.
> The GREEN config is the re-landed W1 behavior.

## Mechanism

A pushed head block whose DAG has missing links must be **registered** in the hub's
bounded pending-DAG map (`SyncConfig::max_pending_dags`) so Bitswap completion is
tracked and the DAG eventually merges. The pusher drives its **persisted retry ladder**
off the `PushLogReply`: a success reply is terminal — the pusher deletes its retry
record (`defra-node/src/lib.rs` `remove_retry_doc`) and never re-pushes unless an
unrelated later update arrives. Go's direct replicator channel has the same shape: its
retry ladder is driven by **error replies** (Go `replicator.go` retryInterval ladder),
so nack-on-overload is the Go-aligned behavior; overload nacks are orthogonal to the
trust/ACP bypasses that `fa4a84f7` was aligning.

When the map is at capacity, current main **drops the registration and replies
success** (`process/pushlog.rs` WARN + `Ok(())` → `PushLogReply::success`): the pusher's
retry record is destroyed while the hub holds neither a merge nor a registration.
Silent, permanent divergence (#1088 M1).

## Property

`INV_SuccessImpliesRegisteredOrMerged` — **a success PushLogReply implies the pushed
block is either merged or registered as pending on the hub.** No code path may reply
success after discarding state.

GREEN additionally checks `EventuallyAllMerged` under per-doc weak fairness: with
overflow nacked (`RATE_LIMITED_MESSAGE`) and retries fair, every pushed doc eventually
merges — the drop/re-push loop has a fixed point.

## Abstractions

- **Pusher identity is dropped**: any fan-in of pushers contends for the same global
  capacity, so docs are the unit and "the pusher" is the per-doc ack/retry record.
- **How a push comes to have missing links** (single-head-block live update, M3
  send-timeout truncation) is irrelevant to the reply decision: `HubComplete` models a
  complete arrival, `HubAdmit`/`HubOverflow*` an incomplete one.
- **TTL eviction of an admitted entry is not modeled.** Evicting a registered DAG after
  a success reply is the pending map's residual divergence window (#844, #1088 W2/W3
  follow-ups), orthogonal to the reply decision this slice fixes. With eviction the
  invariant would need reply-time phrasing; without it, the state invariant is exact.

## Configs

| Config | Knob | Verdict | Meaning |
|--------|------|---------|---------|
| `MC_PushLogAdmission_Green.cfg` | `ReplyMode="NackOnFull"` | GREEN | W1 fix: overflow nacks; invariant + liveness hold |
| `MC_PushLogAdmission_Red_SuccessOnFull.cfg` | `ReplyMode="SuccessOnFull"` | RED | current main: overflow acks success → invariant violated in 5 states |

## Conformance fence

The Rust-side fence for the same invariant is the fan-in integration test
(`tools/integration-test/tests/p2p/`, #1088 W5) plus the coordinator unit tests around
the capacity-nack reply — both assert that no document is success-acked on a pusher
while unmerged and unregistered on the hub.
