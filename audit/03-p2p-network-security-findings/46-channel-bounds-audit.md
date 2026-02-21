# Finding 46: Channel Bounds Audit — One Unbounded Channel Found

**Severity**: MEDIUM
**Category**: Resource Exhaustion
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

All major tokio channels in the P2P stack are bounded except one: the `failure_tx` channel for push failure notifications uses `UnboundedSender`. The bounded channels consistently use 256 as their buffer size.

## Evidence

### Bounded Channels (GREEN)

| Location | Size | Purpose |
|----------|------|---------|
| `host/p2p_host/mod.rs:217` | 256 | Command channel (host ← callers) |
| `host/p2p_host/mod.rs:218` | 256 | Event channel (host → coordinator) |
| `host/p2p_host/mod.rs:254` | 256 | Two-stream event channel |
| `sync/manager/process/mod.rs:100` | 256 | Sync events (default `event_buffer_size`) |
| `sync/manager/config.rs:13` | 256 | SyncConfig default event_buffer_size |

### Unbounded Channel (FINDING)

`sync/coordinator/mod.rs:115`:
```rust
pub(super) failure_tx: Option<tokio::sync::mpsc::UnboundedSender<PushFailure>>,
```

`sync/coordinator/constructor.rs:158`:
```rust
tx: tokio::sync::mpsc::UnboundedSender<super::PushFailure>,
```

This channel reports push failures back to the FFI layer for retry tracking. If the consumer is slow or stalled, failures accumulate without bound.

## Risk Assessment

**Bounded channels at 256**: If a sender tries to push when the channel is full, `send().await` blocks (backpressure). This is correct behavior — it prevents unbounded memory growth but could cause cascading stalls if the consumer is overwhelmed. The 256 buffer is reasonable for a P2P system — enough to absorb bursts without excessive memory.

**Unbounded `failure_tx`**: If the FFI layer stops consuming push failures (e.g., due to a bug or slow disk), every failed PushLog to a replicator peer creates a `PushFailure` struct that accumulates forever. In a scenario where a replicator is unreachable and the node has many documents, this could produce thousands of failures per minute.

## Recommendation

Replace `UnboundedSender<PushFailure>` with a bounded `mpsc::Sender<PushFailure>` with a reasonable limit (e.g., 1024). Drop oldest failures if the channel is full — retry logic can recover.
