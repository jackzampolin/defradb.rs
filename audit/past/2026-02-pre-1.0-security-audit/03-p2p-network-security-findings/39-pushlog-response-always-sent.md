# Finding: PushLog Handler Always Sends Response — No Peer Left Hanging

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: GREEN
**Category**: Protocol Correctness

## Summary

All code paths through `handle_pushlog_request` and `handle_two_stream_request` send a response to the requesting peer. Access denied, invalid CID, processing errors, and successful processing all result in explicit response messages. No error path leaves the peer waiting indefinitely.

## Evidence

### Standard PushLog Handler (4 response paths)

```rust
// pushlog.rs:12-104
pub(super) async fn handle_pushlog_request(...) -> Result<()> {
    // Path 1: Access denied → error response
    if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
        let reply = PushLogReply::error(...);
        self.host.send_pushlog_response(channel, reply).await;
        return Err(e);
    }

    // Path 2: Invalid CID → error response
    let cid = match Cid::try_from(request.cid.as_slice()) {
        Ok(cid) => cid,
        Err(e) => {
            let reply = PushLogReply::error(...);
            self.host.send_pushlog_response(channel, reply).await;
            return Err(...);
        }
    };

    // Path 3: Process error → error response
    // Path 4: Success → success response
    let reply = match &process_result {
        Ok(()) => PushLogReply::success(&request.metadata.message_id),
        Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
    };
    self.host.send_pushlog_response(channel, reply).await;

    process_result
}
```

### Two-Stream PushLog Handler (Same 4 Paths)

```rust
// pushlog.rs:106-214
pub(super) async fn handle_two_stream_request(...) -> Result<()> {
    // Same 4 paths, using send_two_stream_response instead
    // Each path signs the response before sending (Go compatibility)
}
```

### Response Send Failure Handling

If `send_pushlog_response` or `send_two_stream_response` fails, the error is logged but does not prevent the function from returning. The peer may time out in this case, but the handler doesn't deadlock:

```rust
if let Err(e) = self.host.send_pushlog_response(channel, reply).await {
    tracing::warn!(error = %e, "Failed to send PushLog response");
}
```

### Response Signing Failure (Two-Stream)

In the two-stream path, if `sign_message` fails during normal response signing (line 189), the handler returns `Err(e)` without sending a response. This is the one path where the peer could be left hanging. However, signing failure is an internal error (key availability), not something an attacker can trigger.

## Conclusion

PushLog response handling is robust. The peer always receives a response unless:
1. The network connection itself is broken (unavoidable)
2. The node's signing key is unavailable (internal error, not attacker-controlled)
