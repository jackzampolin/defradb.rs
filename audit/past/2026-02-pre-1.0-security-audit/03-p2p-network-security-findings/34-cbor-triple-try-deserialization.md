# Finding: CBOR Triple-Try Deserialization Amplifies Large Message Cost

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: LOW
**Category**: Performance / DoS Amplification

## Summary

The two-stream request handler attempts to deserialize each incoming buffer as three different message types sequentially (PushLogRequest, DocSyncRequest, BranchableSyncRequest). The response handler tries three types as well (BranchableSyncReply, DocSyncReply, PushLogReply). Combined with Finding 00 (no message size limit), this means a multi-GB malformed buffer is parsed up to 3 times before being rejected.

## Affected Files

| File | Lines | Types Tried |
|------|-------|-------------|
| `crates/p2p/src/two_stream/handler/inbound.rs` | 40-75 | PushLogRequest → DocSyncRequest → BranchableSyncRequest |
| `crates/p2p/src/two_stream/handler/inbound.rs` | 112-207 | BranchableSyncReply → DocSyncReply → PushLogReply |

## Details

### Request Path

```rust
// inbound.rs:40-75
// Try to deserialize as PushLogRequest first
if let Ok(request) = serde_cbor::from_slice::<PushLogRequest>(&buf) {
    return Ok(TwoStreamEvent::InboundRequest { peer_id, request });
}

// Try to deserialize as DocSyncRequest
if let Ok(request) = serde_cbor::from_slice::<DocSyncRequest>(&buf) {
    return Ok(TwoStreamEvent::DocSyncRequest { peer_id, request });
}

// Try to deserialize as BranchableSyncRequest
if let Ok(request) = serde_cbor::from_slice::<BranchableSyncRequest>(&buf) {
    return Ok(TwoStreamEvent::BranchableSyncRequest { peer_id, request });
}

Err(Error::CborDeserialization("failed to deserialize..."))
```

Each `serde_cbor::from_slice` must parse the entire buffer. For a legitimate DocSyncRequest, the PushLogRequest parse fails first (full parse attempt), then DocSyncRequest succeeds. For a completely invalid message, all three parse attempts execute.

### Why This Is Low Severity

This is a constant-factor amplifier (3x), not an algorithmic complexity issue. It only matters in combination with Finding 00 — if messages are capped at 16MB, three parses of 16MB is manageable (48MB parse work). The real issue is that Finding 00 allows multi-GB buffers, making the 3x multiplier significant.

### Why Not Use a Type Discriminator

The two-stream protocol does not include a type tag in the wire format. The handler must try each type because:
- `#[serde(flatten)]` on MetaData means all types share the same base CBOR structure
- serde_cbor ignores unknown fields by default, so DocSyncReply can parse as PushLogReply
- The response handler works around this by checking `collection_id.is_empty()` on BranchableSyncReply

### serde_cbor Unknown Field Handling

Because `serde_cbor::from_slice` ignores unknown fields, a PushLogRequest buffer will successfully deserialize as a DocSyncRequest (with an empty `doc_ids`). The ordering matters: PushLogRequest is tried first specifically because it has more fields that would be silently dropped by the others.

## Remediation

Low priority — fixing Finding 00 (message size limit) reduces this to a non-issue. If further optimized:

1. Add a message type discriminator byte prefix to the wire format
2. Or use a single envelope type with a tagged union

## Test Gap

No test verifies the ordering behavior — e.g., that a DocSyncRequest doesn't accidentally match PushLogRequest first.
