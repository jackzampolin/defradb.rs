# Finding: GossipSub Messages Skip Application-Level Signature Verification

**Stream**: 03 - P2P Network Security
**Severity**: LOW
**Category**: Defense in Depth
**Status**: CONFIRMED (transport-level signing mitigates)

## Summary

GossipSub messages are decoded from CBOR and processed without calling `verify_message()`. The application-level signature in the message payload is never checked. This is mitigated by GossipSub's transport-level `MessageAuthenticity::Signed` configuration, which provides libp2p-level message authentication. However, Go sends `PushLogRequest` (with MetaData/signature) over GossipSub, and Rust strips the MetaData via `PushLogBroadcast::from_request()` without verifying the signature first.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/protocols.rs` | 146-150 | Decodes GossipSub payload as PushLogBroadcast or PushLogRequest — no signature check |
| `crates/p2p/src/sync/coordinator/event_handler/gossip.rs` | 11-57 | `handle_gossip_message()` processes PushLogBroadcast — no verify_message call |
| `crates/p2p/src/message/pushlog.rs` | 274-282 | `PushLogBroadcast::from_request()` strips MetaData (including signature) |

## Details

### Message Decoding Path

```rust
// protocols.rs:146-150
let broadcast = serde_cbor::from_slice::<PushLogBroadcast>(&message.data)
    .or_else(|_| {
        serde_cbor::from_slice::<PushLogRequest>(&message.data)
            .map(|req| PushLogBroadcast::from_request(&req))
            // ^^ Strips signature without verifying it first
    });
```

When a Go node sends a PushLogRequest over GossipSub, Rust deserializes it and immediately converts it to PushLogBroadcast, discarding the MetaData (including sender_id, pubkey, and signature) without checking them.

### Transport-Level Mitigation

GossipSub is configured with `MessageAuthenticity::Signed(keypair)` (behaviour.rs:213), which means:
- All outgoing GossipSub messages are signed at the libp2p layer
- All incoming GossipSub messages have their libp2p-level signature verified by the gossipsub protocol implementation
- The `propagation_source` peer ID is authenticated by the gossipsub protocol

This provides equivalent authentication to what the application-level signature would provide.

### Go Parity

Go's GossipSub handler also relies on libp2p-level message authentication rather than re-verifying the application-level signature in the payload. The Rust behavior matches Go.

## Impact

Low. Transport-level signing provides the same authentication guarantees. The application-level signature in Go's PushLogRequest payload is redundant when sent over GossipSub.

## Remediation

No action needed — transport-level signing is sufficient for GossipSub. Consider adding a comment documenting that application-level signatures are intentionally not checked for GossipSub messages because `MessageAuthenticity::Signed` provides equivalent protection.

## Test Gap

No test verifies that a GossipSub message with an invalid application-level signature is still accepted (because transport-level signing is sufficient). No test verifies that a GossipSub message with an invalid transport-level signature is rejected by the gossipsub protocol.
