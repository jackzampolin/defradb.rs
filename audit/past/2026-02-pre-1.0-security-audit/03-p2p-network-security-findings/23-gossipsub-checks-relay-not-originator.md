# Finding 23: GossipSub Access Check Uses Relay Peer, Not Message Originator

**Severity: MEDIUM**
**Category: Authorization Bypass**
**Status: Confirmed**

## Summary

The GossipSub access check in `gossip.rs` uses `propagation_source` (the immediate neighbor that relayed the message) instead of the message's original publisher. If an authorized peer relays a message from an unauthorized peer, the access check passes because it validates the relay peer's authorization, not the originator's.

## Evidence

### Access Check Uses propagation_source

`crates/p2p/src/sync/coordinator/event_handler/gossip.rs:26`:
```rust
pub(super) async fn handle_gossip_message(
    &self,
    propagation_source: libp2p::PeerId,  // <-- relay peer, not originator
    message: PushLogBroadcast,
    topic: String,
) -> Result<()> {
    // Checks if the RELAY peer is authorized, not the message creator
    if let Err(e) = self.check_access(&propagation_source, &message.collection_id) {
        ...
    }
    ...
}
```

### GossipSub Propagation Model

In libp2p GossipSub:
- `propagation_source`: The directly-connected peer that forwarded the message (Noise-authenticated, cannot be spoofed)
- `message.source`: The original publisher's PeerId (optional, can be set by the publisher; may be `None` if anonymized)

The access check validates the wrong identity:
- **Scenario**: Peer X (unauthorized for collection "users") publishes to the `users` topic
- Peer Y (authorized for "users") is in the same GossipSub mesh and relays the message
- Node receives message with `propagation_source = Y`
- `check_access(Y, "users")` → `Ok(())` — message is accepted

### The PushLogBroadcast Contains creator_id

`message.creator_id` could be used for validation, but it's a self-asserted field set by the message publisher, not cryptographically verified at this layer (see Finding 17 — no application-level signature check on GossipSub messages).

## Mitigating Factors

1. **Currently moot**: AccessMode is always Open (Finding 20), so this doesn't matter in practice today
2. **GossipSub mesh formation**: A peer must subscribe to the same topic to relay messages, which provides some natural filtering
3. **Document-level ACP**: The merge layer performs document-level permission checks, providing a second line of defense
4. **Transport authentication**: `propagation_source` IS cryptographically verified (Noise), so at least we know who relayed the message

## Impact

When/if AccessMode::Controlled is activated, an unauthorized peer can get their messages accepted by having them relayed through an authorized peer's GossipSub mesh.

## Recommendation

1. For collection-level checks: verify `message.creator_id` matches the message signature (requires solving Finding 17 first)
2. Alternatively: accept this as a design limitation of GossipSub relay and rely on document-level ACP at the merge layer
3. Document this relay-bypass in the security model comments
