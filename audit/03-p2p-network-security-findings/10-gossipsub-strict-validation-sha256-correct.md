# Finding: GossipSub ValidationMode::Strict and SHA256 Message IDs Are Correct (GREEN)

**Stream**: 03 - P2P Network Security
**Severity**: INFORMATIONAL
**Category**: Pubsub Security
**Status**: CONFIRMED — NO ISSUE

## Summary

GossipSub is correctly configured with `ValidationMode::Strict` (requires signatures), `MessageAuthenticity::Signed(keypair)` (signs all outgoing messages), and SHA256-based message IDs (collision-resistant deduplication). Peer exchange (`do_px()`) is enabled to match Go behavior. These settings are secure and appropriate.

## Verified Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/behaviour.rs` | 191-223 | Production GossipSub config |

## Details

### Configuration Analysis

```rust
gossipsub::ConfigBuilder::default()
    .heartbeat_interval(Duration::from_secs(1))     // Standard
    .validation_mode(ValidationMode::Strict)          // Requires valid signatures
    .do_px()                                          // Peer exchange in PRUNE
    .flood_publish(true)                              // See Finding 04
    .message_id_fn(|message: &gossipsub::Message| {
        let hash = crypto::sha256(&message.data);     // Collision-resistant
        MessageId::from(hash.to_vec())
    })
    .build()?;

gossipsub::Behaviour::new(MessageAuthenticity::Signed(keypair), gossipsub_config)
```

### What ValidationMode::Strict Enforces

Every incoming GossipSub message MUST have:
1. A valid PeerId as author
2. A sequence number
3. A valid Ed25519 signature over the message

Messages missing any of these are rejected immediately, before processing or forwarding. This prevents:
- **Message spoofing**: Cannot forge messages from another peer
- **Message replay without signature**: Unsigned replays are dropped
- **Anonymous flooding**: Cannot send messages without a valid identity

### SHA256 Message IDs

Using `crypto::sha256(&message.data)` for message IDs ensures:
- **Content-based deduplication**: Same message content → same ID → deduplicated
- **Collision resistance**: SHA256 has 128-bit collision resistance, effectively unforgeable
- **No sequence-number spoofing**: Message ID doesn't depend on mutable metadata

### Peer Exchange (do_px)

When a peer is pruned from the mesh, the PRUNE message includes a list of other known peers for that topic. This helps mesh recovery but could theoretically be used to inject Sybil peers. In practice, pruned peers are re-evaluated before grafting, and `ValidationMode::Strict` ensures only authenticated peers can participate.

### Message Size Limit

GossipSub's default `max_transmit_size` is 64 KB per RPC message. This is enforced at the protocol level and cannot be bypassed by a malicious peer. This provides a natural bound on per-message resource consumption.

### Test Configuration Difference (Noted, Not a Finding)

The `new_without_signing()` test constructor uses `ValidationMode::Permissive` and `MessageAuthenticity::RandomAuthor`. This is appropriate for unit tests but MUST NOT be used in production. It is correctly gated behind `#[cfg(test)]`.

## Conclusion

GossipSub security configuration is correct and matches Go behavior. No finding.
