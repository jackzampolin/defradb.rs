# Finding: Two-Stream Handler Accepts Messages Without Signature Verification

**Stream**: 03 - P2P Network Security
**Severity**: CRITICAL
**Category**: Authentication Bypass
**Status**: CONFIRMED

## Summary

The two-stream protocol handler — the PRIMARY message exchange path for Go interop and all PushLog replication — deserializes and processes inbound messages without calling `verify_message()`. Signature, pubkey, and sender_id fields are completely ignored. Any peer that has completed a Noise handshake can send forged PushLogRequests with arbitrary sender_id, and they will be accepted and processed as legitimate replication data.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/two_stream/handler/inbound.rs` | 24-76 | `handle_request_stream()` — deserializes PushLogRequest, DocSyncRequest, BranchableSyncRequest without any signature check |
| `crates/p2p/src/two_stream/handler/inbound.rs` | 90-208 | `handle_response_stream()` — deserializes PushLogReply, DocSyncReply, BranchableSyncReply without any signature check |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 106-214 | `handle_two_stream_request()` — processes the deserialized request, checks access control, stores blocks. No signature check before or after. |

## Details

### The Problem

Go's `Receive()` function in `message/message.go:84-109` always calls `verifyMessage(m)` after deserialization:

```go
func Receive(stream io.Reader, peerID string, proto proto, m Message) error {
    b, err := io.ReadAll(stream)
    err = cbor.Unmarshal(b, m)
    m.SetSenderID(peerID)  // Override sender_id with transport peer ID
    err = verifyMessage(m)  // <-- ALWAYS verifies
    // ...
}
```

Rust's two-stream handler does NOT:

```rust
pub async fn handle_request_stream(
    peer_id: PeerId,
    mut stream: Stream,
) -> Result<TwoStreamEvent> {
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;  // Read raw bytes
    if let Ok(request) = serde_cbor::from_slice::<PushLogRequest>(&buf) {
        return Ok(TwoStreamEvent::InboundRequest { peer_id, request });
        // ^^ No verify_message() call — signature is ignored
    }
    // ...
}
```

### Why PushLogCodec Doesn't Help

The `PushLogCodec` in `codec.rs` DOES have signing/verification, but it is configured with **no protocols**:

```rust
let codec = PushLogCodec::with_keypair(keypair.clone());
let pushlog = request_response::Behaviour::with_codec(
    codec,
    std::iter::empty::<(StreamProtocol, ProtocolSupport)>(),  // <-- NO protocols registered
    // ...
);
```

This means the PushLogCodec is never used for actual message exchange. All production traffic flows through the two-stream handler, which has no verification.

### Go Also Overwrites sender_id

Go's `Receive()` does `m.SetSenderID(peerID)` — it replaces the in-message sender_id with the transport-level peer ID. This means Go doesn't trust the sender_id in the message at all; it uses the Noise-authenticated connection identity. Rust trusts whatever sender_id the message contains.

### Attack Scenario

1. Attacker node A connects to victim node V via Noise (valid Ed25519 keypair — trivially generated)
2. A sends a PushLogRequest with `sender_id` set to legitimate node L's peer ID, and a forged `collection_id`
3. V's `handle_request_stream()` deserializes and passes to `handle_two_stream_request()`
4. V's access control check uses the transport `peer_id` (A), but the message's `sender_id` says L — creating an identity confusion
5. The block data from A is stored as if it came from a legitimate replication source

### Impact

- **Peer impersonation**: A message can claim to be from any sender_id
- **Signature bypass**: All 4 verification checks (signature exists, pubkey decodes, peer ID matches pubkey, signature valid) are never executed
- **Data integrity**: Unsigned/forged blocks are accepted into the blockstore

### Mitigating Factor

The Noise transport layer authenticates the connection-level peer ID, so the `peer_id` passed to `handle_two_stream_request()` is the transport-authenticated identity. Access control checks use this transport peer ID, not the in-message sender_id. This means an attacker can't bypass per-collection access control via sender_id forgery. However, the application-layer signature is still completely unchecked.

## Remediation

Add `verify_message()` calls in `handle_request_stream()` and `handle_response_stream()` after deserialization. Additionally, override the message's `sender_id` with the transport peer ID (matching Go's behavior) before verification.

## Test Gap

No test sends a message via the two-stream protocol with an invalid or missing signature and asserts rejection. All 13 signing tests in `signing_tests.rs` test the `sign_message`/`verify_message` functions in isolation — none test the two-stream integration path.
