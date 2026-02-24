# Finding 42: No Per-Peer Rate Limiting

**Severity**: HIGH
**Category**: Resource Exhaustion
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

There is no rate limiting anywhere in the P2P stack. A single malicious peer can send unlimited messages per second across all protocols (PushLog, DocSync, BranchableSync, CAR, GossipSub) without any throttling. The only rate-limiting construct found in the entire codebase is a `Semaphore` in the replication loop runner (`sync/replication/loop_runner.rs:152`) which limits concurrent merge *workers* — not inbound messages from peers.

## Evidence

Searched the entire `crates/p2p/src/` tree for: `Semaphore`, `RateLimiter`, `rate_limit`, `circuit_breaker`, `token_bucket`, `throttle`.

**Only match**: `crates/p2p/src/sync/replication/loop_runner.rs:152`:
```rust
let semaphore = Arc::new(Semaphore::new(config.max_workers)); // 32 default
```

This limits concurrent merge processing, not inbound message rate.

**No per-peer tracking of**:
- Messages received per second
- Bytes received per second
- Failed requests per time window
- Connection attempt frequency

**`PeerStateTracker`** (`sync/peer_state/tracker/mod.rs`) tracks CID knowledge and subscriptions per peer, but has no rate or count fields. It tracks `last_seen` timestamps and `connected` status — no message counters.

## Attack Scenario

A malicious peer sends PushLog requests at maximum speed. Each request:
1. Triggers CBOR deserialization (CPU)
2. Triggers access control check (HashMap lookup)
3. Triggers CID parsing
4. Triggers blockstore operations (I/O)
5. Triggers a response message (network I/O)

At 1000+ messages/second, this saturates the event loop, starving legitimate peers.

## Go Comparison

Go DefraDB also lacks explicit per-peer rate limiting, but Go's `ConnectionGater` interface provides hooks for rate-limiting at the connection level. Rust's implementation has no equivalent gater integration.

## Recommendation

Add a per-peer token bucket rate limiter (e.g., `governor` crate) that tracks message rates per `PeerId`. Apply at the event handler dispatch (`coordinator/event_handler/mod.rs:20`) before any protocol processing. Consider 100 messages/second/peer as a starting point.
