# Finding 49: PendingResponses HashMap Has No Eviction

**Severity**: MEDIUM
**Category**: Memory Leak
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

The `PendingResponses` HashMap in the two-stream handler maps message IDs to oneshot response channels. Entries are added when a request is sent and removed when a response arrives or a timeout fires. However, if neither happens (e.g., the response stream is dropped before the timeout path runs), entries leak.

## Evidence

**Data structure** (`two_stream/handler/mod.rs:35-39`):
```rust
pub(crate) struct PendingResponses {
    pub(crate) channels: HashMap<String, oneshot::Sender<PushLogReply>>,
}
```

**Entry lifecycle**:
1. **Added**: `doc_sync.rs:32` — when sending a DocSync request
2. **Removed (happy path)**: `inbound.rs:144` — when response arrives
3. **Removed (timeout)**: `doc_sync.rs:74-82` — 30s timeout fires, cleans up
4. **Removed (stream open failure)**: `doc_sync.rs:42-43` — cleanup on stream error

**Potential leak path**:
- `send_doc_sync_request_fire_and_forget()` (`doc_sync.rs:91-117`) does NOT register a pending response at all, so this path is safe.
- `send_doc_sync_request()` (`doc_sync.rs:17-84`) has timeout + cleanup, so this path is safe.
- PushLog two-stream path (`messaging.rs:71-111`) also has timeout + cleanup.

**Actual risk**: The timeout-based cleanup paths are well-implemented. The real concern is that there's no periodic sweep of stale entries — if a bug causes a timeout handler to not run (e.g., task cancellation), the entry persists forever. With message IDs being UUID v4 strings (~36 bytes) + oneshot sender overhead (~64 bytes), each leaked entry is ~100 bytes.

## Assessment

Low practical risk because timeout paths are correctly implemented. However, a defensive periodic sweep (e.g., every 60 seconds, remove entries older than 2 × RESPONSE_TIMEOUT) would provide defense-in-depth.

## Recommendation

Add a `created_at: Instant` field to pending entries. Run a periodic cleanup task that evicts entries older than 60 seconds.
