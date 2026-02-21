# Finding 26: PushLog Access Check Ordering Is Correct

**Severity: GREEN**
**Category: Authorization Model**
**Status: Verified**

## Summary

Both PushLog handlers (standard and two-stream) perform access checks BEFORE any CID parsing or data processing. On denial, an error response is sent and the function returns early. This prevents information leakage (e.g., confirming whether a CID is valid) to unauthorized peers.

## Evidence

### Standard PushLog — Access Before CID Parsing

`crates/p2p/src/sync/coordinator/event_handler/pushlog.rs:25-48`:
```rust
pub(super) async fn handle_pushlog_request(...) -> Result<()> {
    // Line 26: Access check FIRST
    if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
        // Line 33-46: Error response sent
        let reply = PushLogReply::error(...);
        self.host.send_pushlog_response(channel, reply).await;
        // Line 47: Early return — no CID parsing, no block processing
        return Err(e);
    }

    // Line 51: CID parsing only happens AFTER access is granted
    let cid = match Cid::try_from(request.cid.as_slice()) { ... };
    ...
}
```

### Two-Stream PushLog — Same Pattern

`crates/p2p/src/sync/coordinator/event_handler/pushlog.rs:119-145`:
```rust
pub(super) async fn handle_two_stream_request(...) -> Result<()> {
    // Line 120: Access check FIRST
    if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
        // Line 127-143: Error response sent (signed for two-stream)
        let mut reply = PushLogReply::error(...);
        sign_message(self.host.keypair(), &mut reply);
        self.host.send_two_stream_response(peer_id, reply).await;
        // Line 144: Early return
        return Err(e);
    }

    // Line 148: CID parsing only after access granted
    let cid = match Cid::try_from(request.cid.as_slice()) { ... };
    ...
}
```

### What This Prevents

An unauthorized peer sending a PushLog request learns:
- "access denied" — nothing about whether the CID exists, is valid, or belongs to a real document
- No timing side-channel (rejection is immediate, before any blockstore operations)

### Both Code Paths Are Structurally Identical

The access check logic is duplicated (not shared) between standard and two-stream handlers, but both follow the same pattern: check → error response → early return. The only difference is that the two-stream path signs the error response (for Go compatibility).

## Conclusion

Access check ordering follows security best practices: authenticate/authorize first, then process data. No information leakage on denial.
