# Finding: Two-Stream Response Signing Failure Sends Unsigned Reply

**Stream**: 03 - P2P Network Security
**Severity**: LOW
**Category**: Error Handling
**Status**: CONFIRMED

## Summary

In `handle_two_stream_request()`, when signing an access-denied or invalid-CID error response fails, the code logs the error but still proceeds to send the unsigned response. This means the peer receives an error reply that lacks a valid signature, which could be confusing if the peer validates signatures on responses. The main success/error response path (line 189-196) correctly returns the signing error without sending, but the access-denied and invalid-CID paths do not.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 134-136 | Access-denied reply: signing failure logged at `error` level but response still sent |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 162-164 | Invalid-CID reply: signing failure logged at `error` level but response still sent |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 189-196 | Main reply path: signing failure correctly returns error and does NOT send |

## Details

### Access-Denied Path (lines 127-144)

```rust
let mut reply = PushLogReply::error(/* ... */);
if let Err(sign_err) = sign_message(self.host.keypair(), &mut reply) {
    tracing::error!(error = %sign_err, "Failed to sign access denied response");
    // Falls through — unsigned reply is still sent below
}
if let Err(send_err) = self.host.send_two_stream_response(peer_id, reply).await {
    // ...
}
```

### Main Path (lines 188-196) — Correct Pattern

```rust
if let Err(e) = sign_message(self.host.keypair(), &mut reply) {
    tracing::error!(/* ... */);
    return Err(e);  // <-- Correctly aborts, does NOT send unsigned reply
}
```

### Impact

Low in practice because:
1. Finding 12 shows that the receiving peer doesn't verify signatures anyway
2. The access-denied and invalid-CID cases are error paths, not data paths
3. An unsigned error reply doesn't leak data — it just tells the sender their request failed

However, if signature verification is added to the two-stream handler, these unsigned error replies would be rejected by the receiver.

## Remediation

Add `return` after the signing failure log in the access-denied and invalid-CID paths to match the main path's pattern.

## Test Gap

No test covers the case where signing fails on an error response.
