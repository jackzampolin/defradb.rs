# Finding: GossipSub flood_publish Amplifies to All Subscribed Peers

**Stream**: 03 - P2P Network Security
**Severity**: LOW
**Category**: Amplification / Bandwidth
**Status**: CONFIRMED

## Summary

`flood_publish(true)` causes every locally-published GossipSub message to be sent directly to ALL connected peers subscribed to the topic, bypassing the mesh limit (D=6). This is intentional for reliability but means publish bandwidth scales linearly with peer count rather than being bounded by the mesh size.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/behaviour.rs` | 199 | `.flood_publish(true)` |

## Details

### What flood_publish Does

Normal GossipSub publishes only to mesh peers (D=6 by default). With `flood_publish(true)`, locally-originated messages are sent directly to **every** connected peer subscribed to that topic, regardless of mesh membership.

This is the same configuration as Go:
```go
pubsub.WithFloodPublish(true)
```

### Mesh Size Defaults (All libp2p Defaults)

| Parameter | Value | Purpose |
|-----------|-------|---------|
| D (mesh_n) | 6 | Target mesh peers per topic |
| D_lo | 5 | Minimum before grafting |
| D_hi | 12 | Maximum before pruning |
| D_out | 2 | Minimum outbound mesh peers |
| D_lazy | 6 | Gossip (IHAVE) fan-out |

These defaults are reasonable for small-to-medium networks. However, `flood_publish` bypasses D_hi entirely for locally-published messages.

### Amplification Analysis

For a node with N peers subscribed to topic T:
- **Normal publish**: Sends to D=6 mesh peers (constant)
- **Flood publish**: Sends to N peers (linear)

In DefraDB's architecture, each collection and document has its own topic. A node replicating 100 collections with 50 peers would send each publish to all 50 peers on the relevant topic, rather than 6.

### Why This Is Low Severity

1. **Go parity**: Same configuration, intentional design choice
2. **Small networks**: DefraDB networks are typically small (<50 peers)
3. **Message size bounded**: GossipSub enforces `max_transmit_size` of 64KB per RPC message
4. **Not externally triggerable**: Only locally-published messages are flood-published; relayed messages use normal mesh forwarding
5. **Per-topic scoping**: Each topic has independent mesh membership

### When It Could Become Higher Severity

- Large networks (>100 peers) with many shared topics
- Combined with Finding 01 (no connection limits), an attacker could connect many Sybil peers and force the node to flood-publish to all of them
- Topic fan-out: If many topics are subscribed by many peers, aggregate bandwidth could be significant

## Remediation

No immediate action needed — matches Go behavior and is appropriate for DefraDB's network size. If scaling to larger networks, consider:
1. Disabling `flood_publish` and relying on mesh propagation
2. Configuring explicit mesh size limits (lower D_hi)
3. Adding per-topic peer limits

## Test Gap

No test measures bandwidth amplification from flood_publish with varying peer counts.
