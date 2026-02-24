# Finding: Bounded Channels Create Backpressure-Induced Memory Accumulation

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: LOW
**Category**: Resource Exhaustion / Indirect DoS

## Summary

All event channels in the P2P stack use bounded `mpsc::channel` buffers (256 capacity). When the consumer (coordinator) processes events slower than producers generate them, producer tasks block on `.send().await`. Blocked tasks hold their allocated memory (including buffers from `read_to_end()`), creating a feedback loop where backpressure causes memory accumulation rather than message drops.

## Affected Channels

| Channel | Buffer Size | Source | Consumer |
|---------|-------------|--------|----------|
| `event_tx` (host) | 256 | `P2PHost::new()` at `mod.rs:218` | SyncCoordinator event loop |
| `two_stream_event_tx` | 256 | `P2PHost::new()` at `mod.rs:254` | P2PHost → HostEvent forwarding |
| `event_tx` (sync) | 256 | `SyncManager::new()` at `process/mod.rs:100` | Database merge loop |
| `command_tx` | 256 | `P2PHost::new()` at `mod.rs:217` | P2PHost command handler |

## Details

### Backpressure Path

1. Attacker floods the node with many concurrent streams (Finding 30)
2. Each stream task reads its message (possibly very large per Finding 00)
3. Stream task finishes reading, deserializes, creates a `TwoStreamEvent`
4. Task calls `event_tx.send(event).await` — blocks if channel is full
5. While blocked, the task holds the deserialized message in memory
6. With 256+ concurrent tasks blocked, all hold their messages simultaneously

### No Deadlock, But Memory Accumulates

The channels use `tokio::sync::mpsc` which is not susceptible to deadlock (producers block, consumer continues). But the bounded buffer means:
- At most 256 events queued in the channel
- Unlimited tasks can block waiting to enqueue
- Each blocked task retains its full context (including large deserialized messages)

### The `let _ = sender.send()` Pattern

Several locations use fire-and-forget channel sends:

```rust
// dag_fetcher.rs:49-56
let _ = event_tx
    .send(SyncEvent::DagReady { root_cid, doc_id, ... })
    .await;
```

The `let _ =` discards the send error, which only occurs if the receiver is dropped (shutdown). This is correct — not a finding. But the `.await` still blocks if the channel is full, holding resources.

### Comparison to Alternative Designs

- **Unbounded channels** would avoid backpressure but enable unbounded memory growth
- **Try-send with drop** would avoid blocking but lose events
- **Bounded channels** (current) are the correct choice, but need upstream admission control (Findings 00, 30) to prevent the backpressure from becoming a problem

## Remediation

This finding is low severity because it's a consequence of Findings 00 and 30. Fixing those (message size limits + task concurrency limits) makes channel backpressure a normal, healthy flow-control mechanism rather than a DoS vector.

If further hardening is desired:
1. Add per-task timeout on channel send (e.g., 5s) — drop the event and warn if send blocks too long
2. Add a metric for channel utilization (how full the buffer is) for monitoring
3. Consider `try_send` with logging for non-critical events (like DecodeError)

## Test Gap

No test verifies behavior under channel backpressure conditions.
