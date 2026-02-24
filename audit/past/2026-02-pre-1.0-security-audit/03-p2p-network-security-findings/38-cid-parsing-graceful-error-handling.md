# Finding: CID Parsing Errors Handled Gracefully — No Panics

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: GREEN
**Category**: Error Handling

## Summary

All CID parsing sites across the replication protocol use `Cid::try_from()` or `Cid::read_bytes()` with proper error handling. Invalid CIDs result in error responses (PushLog), warn-and-skip (DocSync/BranchableSync), or `Error::InvalidCid` returns (CAR). No path panics on malformed CID bytes.

## Evidence

### PushLog Handler — Error Response Sent

```rust
// pushlog.rs:51-74
let cid = match Cid::try_from(request.cid.as_slice()) {
    Ok(cid) => { self.peer_state.peer_has_cid(&peer_id, cid); cid }
    Err(e) => {
        let reply = PushLogReply::error(&request.metadata.message_id, &error_msg);
        self.host.send_pushlog_response(channel, reply).await;
        return Err(crate::error::Error::InvalidCid(error_msg));
    }
};
```

Invalid CID → error reply sent → early return with error. Peer is never left hanging.

### Two-Stream PushLog Handler — Same Pattern

```rust
// pushlog.rs:148-174
let cid = match Cid::try_from(request.cid.as_slice()) {
    Ok(cid) => { ... }
    Err(e) => {
        let mut reply = PushLogReply::error(...);
        sign_message(self.host.keypair(), &mut reply);
        self.host.send_two_stream_response(peer_id, reply).await;
        return Err(crate::error::Error::InvalidCid(error_msg));
    }
};
```

Same pattern, with signing for Go compatibility.

### DocSync Reply Handler — Warn and Skip

```rust
// doc_sync.rs:97-129
match Cid::try_from(head_bytes.as_slice()) {
    Ok(cid) => { /* check blockstore, add to cids_to_fetch */ }
    Err(e) => {
        tracing::warn!(doc_id = %item.doc_id, error = %e, "Failed to parse CID");
    }
}
```

Invalid CID in a DocSync reply → warning logged, that specific CID skipped, processing continues for remaining CIDs.

### BranchableSync Reply Handler — Same Warn-and-Skip

```rust
// branchable_sync.rs:97-117
match Cid::try_from(head_bytes.as_slice()) {
    Ok(cid) => { /* check blockstore */ }
    Err(e) => { tracing::warn!(...); }
}
```

### CAR Protocol — Error Returned

```rust
// car.rs handler:31-32 (request)
let root_cid = Cid::read_bytes(std::io::Cursor::new(&buf))
    .map_err(|e| Error::InvalidCid(...))?;

// car.rs decode:61-62 (response)
let cid = Cid::read_bytes(section)
    .map_err(|e| Error::InvalidCid(...))?;
```

Both return `Error::InvalidCid`, propagated up to the event handler.

## Conclusion

CID parsing is consistently safe across all protocol handlers. No `unwrap()`, no `expect()`, no panic paths on malformed CID input.
