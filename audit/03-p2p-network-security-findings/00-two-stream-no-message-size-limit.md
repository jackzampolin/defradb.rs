# Finding: Two-Stream Protocol Has No Message Size Limit

**Stream**: 03 - P2P Network Security
**Severity**: HIGH
**Category**: Denial of Service
**Status**: CONFIRMED (Session 4 deep-dive verified)

## Summary

The two-stream P2P protocol uses `read_to_end()` without any size limit in 5 code sites across 3 files (7 call paths). A malicious peer can send arbitrarily large data through any two-stream channel and exhaust the node's memory. In contrast, the request-response codec correctly uses `reader.take(MAX_MESSAGE_SIZE)` with a 16MB limit.

## Affected Files

| File | Lines | Code Site | Call Paths |
|------|-------|-----------|------------|
| `crates/p2p/src/two_stream/handler/inbound.rs` | 33-37 | Request stream handler | PushLog, DocSync, BranchableSync requests |
| `crates/p2p/src/two_stream/handler/inbound.rs` | 98-102 | Response stream handler | PushLog, DocSync, BranchableSync replies |
| `crates/p2p/src/two_stream/runner.rs` | 149 | SE request stream | Searchable encryption artifacts |
| `crates/p2p/src/two_stream/runner.rs` | 165 | SE response stream | SE acknowledgements |
| `crates/p2p/src/two_stream/handler/car.rs` | 15 | CAR protocol stream | CAR request AND CAR response (shared `read_stream()`) |

**Session 4 verification**: Exhaustive `read_to_end` search across entire `crates/` tree found exactly these 5 unprotected sites + 1 protected site (`codec.rs:46` with `take()`). No additional instances exist.

## Details

### The Vulnerability

All five locations follow the same vulnerable pattern:

```rust
// crates/p2p/src/two_stream/handler/inbound.rs:33-37
let mut buf = Vec::new();
stream.read_to_end(&mut buf).await.map_err(|e| {
    tracing::error!(peer_id = %peer_id, error = %e, "Failed to read stream bytes");
    Error::CborDeserialization(format!("failed to read stream: {}", e))
})?;
```

No size limit is applied. The `Vec::new()` will grow to whatever size the remote peer sends.

### The Correct Implementation (Already Exists)

The request-response codec at `crates/p2p/src/codec.rs:25-46` has the right approach:

```rust
pub const MAX_MESSAGE_SIZE: u64 = 16 * 1024 * 1024; // 16 MB

pub async fn read_message<T, R>(reader: &mut R) -> io::Result<T> {
    let mut buf = Vec::new();
    reader.take(MAX_MESSAGE_SIZE).read_to_end(&mut buf).await?;
    // ...
}
```

This is used by `PushLogCodec` but is **not used** by the two-stream handler.

### Why Upstream Protections Don't Help

- **Yamux frame size** (16 KB default): Limits individual frames, not total message. `read_to_end()` reads across all frames.
- **libp2p-stream**: Provides stream multiplexing but has no per-stream or per-message size limit.
- **Connection timeout** (60s idle): Only triggers on idle connections, not slow continuous sends.

### Attack Scenario

1. Attacker connects as a P2P peer (no authentication required)
2. Opens a stream on any two-stream protocol (e.g., `/defradb/rep_req/0.0.1`)
3. Sends arbitrarily large data in 16KB Yamux frames
4. Victim's handler allocates unbounded memory via `read_to_end()`
5. Node OOMs and crashes

Each incoming stream spawns a new tokio task (`runner.rs` lines 82, 117, 146, 162, 179, 195), so multiple concurrent streams can accumulate memory independently with no global budget. See Finding 30 for the unbounded task spawning amplifier.

### Amplification via Post-Read Processing

After `read_to_end` completes, the buffer undergoes CBOR deserialization. In `inbound.rs`, the request handler tries THREE deserializations on the same buffer (PushLogRequest, DocSyncRequest, BranchableSyncRequest) — meaning a multi-GB buffer would be parsed up to 3 times before being rejected. See Finding 34 for details.

### Affected Message Types

All two-stream message types are vulnerable: PushLogRequest, DocSyncRequest, BranchableSyncRequest, PushLogReply, DocSyncReply, BranchableSyncReply, SE artifacts, and CAR data.

## Remediation

Apply `reader.take(MAX_MESSAGE_SIZE)` before `read_to_end()` in all 5 locations:

```rust
use futures::AsyncReadExt;
use crate::codec::MAX_MESSAGE_SIZE;

let mut buf = Vec::new();
stream.take(MAX_MESSAGE_SIZE).read_to_end(&mut buf).await?;
```

For CAR data, consider a separate `MAX_CAR_SIZE` if binary payloads need larger limits.

Optionally, add per-peer memory budgets and per-stream size logging for anomaly detection.

## Test Gap

No test sends an oversized message through the two-stream protocol and verifies it is rejected. Should add:
1. Open a two-stream connection
2. Send data exceeding MAX_MESSAGE_SIZE
3. Verify the stream is closed and no OOM occurs
