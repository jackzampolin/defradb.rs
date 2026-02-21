# Finding 24: GossipSub Topic Names Leak Collection IDs to Mesh Peers

**Severity: LOW**
**Category: Information Disclosure**
**Status: Confirmed (inherent to GossipSub design)**

## Summary

Collection-specific GossipSub topics use the raw collection ID as the topic name. Any peer in the GossipSub mesh can observe topic subscription events, learning which collections exist and which peers replicate them — even if the coordinator later drops their messages.

## Evidence

### Topic Naming

`crates/p2p/src/topics.rs:51`:
```rust
DefraTopic::Collection(id) => id.clone(),  // raw collection_id IS the topic name
```

### Subscription Events Visible to All Mesh Peers

`crates/p2p/src/host/p2p_host/protocols.rs:185-211`:
```rust
gossipsub::Event::Subscribed { peer_id, topic } => {
    debug!("Peer {} subscribed to {}", peer_id, topic);
    // This event fires for ANY peer in the mesh, regardless of authorization
    ...
}
```

### What an Unauthorized Peer Learns

An unauthorized peer connected to the swarm can observe:
1. **Topic subscriptions**: Which collection IDs exist (by observing `Subscribed` events)
2. **Peer mappings**: Which peers replicate which collections
3. **Message hashes**: GossipSub exposes message IDs (SHA256 of content) even to non-subscribers via IHAVE/IWANT protocol messages

### Coordinator Drops Messages, Not Metadata

`crates/p2p/src/sync/coordinator/event_handler/gossip.rs:26-33`:
```rust
// The message content is dropped, but the peer already received the
// Subscribed event with the topic name at the libp2p layer
if let Err(e) = self.check_access(&propagation_source, &message.collection_id) {
    return Err(e);  // message dropped, but topic name already exposed
}
```

## Mitigating Factors

1. Collection IDs are content-addressed hashes (CIDs like `bafkreih3x2q...`) — opaque without schema context
2. This is inherent to GossipSub's design; all pubsub systems have similar metadata leakage
3. Go DefraDB has the same behavior

## Impact

Low — collection IDs are opaque hashes. An attacker learns that collections exist and which peers replicate them, but not what the collections contain or their schemas. However, combined with Finding 21 (BranchableSync bypass), the collection IDs could be used to query collection heads.

## Recommendation

Accept as a design limitation. If topic privacy is needed in the future, consider:
1. Hashing topic names with a shared secret (requires key distribution)
2. Using a single aggregated topic with encrypted routing (complex, likely unnecessary)
