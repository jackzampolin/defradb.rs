# Finding 45: GossipSub Uses Default Mesh Parameters

**Severity**: LOW (informational)
**Category**: Configuration Audit
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

The GossipSub configuration uses libp2p's default mesh parameters. These are well-tested defaults designed for general-purpose use, but should be documented and understood for a database replication system.

## Evidence

**GossipSub config** (`behaviour.rs:192-210`):
```rust
let gossipsub_config = gossipsub::ConfigBuilder::default()
    .heartbeat_interval(Duration::from_secs(1))
    .validation_mode(ValidationMode::Strict)
    .do_px()
    .flood_publish(true)
    .message_id_fn(...)
    .build()?;
```

**Explicitly set**:
- `heartbeat_interval`: 1 second
- `validation_mode`: Strict (all fields validated)
- `do_px()`: Peer exchange enabled in PRUNE
- `flood_publish(true)`: Publish to all peers, not just mesh
- `message_id_fn`: SHA256 of message data (content-addressed, no duplicates)

**Using libp2p defaults** (from `gossipsub::Config::default()`):
| Parameter | Default Value | Meaning |
|-----------|--------------|---------|
| `mesh_n` (D) | 6 | Desired mesh size per topic |
| `mesh_n_low` (D_lo) | 5 | Minimum mesh size before GRAFT |
| `mesh_n_high` (D_hi) | 12 | Maximum mesh size before PRUNE |
| `mesh_n_lazy` (D_lazy) | 6 | Lazy push peers for gossip |
| `gossip_factor` | 0.25 | Fraction of non-mesh peers to gossip to |
| `max_transmit_size` | 65536 (64KB) | Maximum message size |
| `history_length` | 5 | Heartbeat windows of message cache |
| `history_gossip` | 3 | Windows to include in IHAVE gossip |
| `max_ihave_length` | 5000 | Max IHAVE IDs per heartbeat |
| `max_ihave_messages` | 10 | Max IHAVE messages per heartbeat |
| `mcache_len` | 5 | Message cache slots |

## Analysis

**Message cache**: Bounded to `history_length * max_transmit_size * mesh_n` ≈ 5 * 64KB * 12 ≈ 3.8MB per topic. With many topics (one per collection + one per document), this can add up but is bounded.

**`max_transmit_size` (64KB)**: This is the GossipSub-level size limit. Each PushLogBroadcast message must fit within 64KB. This is a natural size limit for the gossip path that is NOT present in the two-stream path.

**`flood_publish(true)`**: Finding 04 already noted this as LOW risk. It means published messages go to ALL connected peers, not just mesh members. For the first hop this creates amplification proportional to the number of connected peers, but GossipSub's built-in deduplication prevents message storms.

## Recommendation

The defaults are reasonable. Document the effective limits (64KB per GossipSub message, 6-12 mesh peers, ~4MB cache per topic) in operational guidance. Consider tuning `max_transmit_size` down if PushLogBroadcast messages are always small.
