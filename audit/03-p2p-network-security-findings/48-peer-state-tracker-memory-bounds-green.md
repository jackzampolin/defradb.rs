# Finding 48: PeerStateTracker Has Proper Memory Bounds

**Severity**: GREEN
**Category**: Resource Management
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

The `PeerStateTracker` has well-designed memory limits with LRU eviction. This is one of the strongest defensive implementations in the P2P stack.

## Evidence

**Three-level memory bounding** (`sync/peer_state/tracker/mod.rs:22-30`):
```rust
const DEFAULT_MAX_CIDS_PER_PEER: usize = 10_000;
const DEFAULT_MAX_TOTAL_CIDS: usize = 1_000_000;
const DEFAULT_MAX_PEERS: usize = 1_000;
```

**Per-peer CID eviction** (`tracker/mod.rs:59-77`):
- Each peer tracks known CIDs in a `HashSet` + `VecDeque` (LRU order)
- When `known_cids.len() >= max_cids`, oldest CIDs are evicted from the front of the deque
- Duplicate CIDs are detected and skipped (no double-insert)

**Global limit enforcement** (`tracker/memory.rs:13-84`):
- Peer count limit: evicts oldest disconnected peers first
- Total CID limit: evicts CIDs from peers with the most CIDs (disconnected peers prioritized)
- Connected peers are never evicted (prevents breaking active connections)

**Memory budget**: 1000 peers × 10,000 CIDs × ~38 bytes/CID ≈ 380MB worst case. The global cap of 1,000,000 total CIDs provides a secondary bound at ~38MB.

## What's Good

1. Eviction is LRU-based (oldest first), not random
2. Disconnected peers are evicted before connected ones
3. Global limits prevent N peers × M CIDs from growing unboundedly
4. Configurable via `with_full_config()`
5. `enforce_global_limits()` is called internally when adding peers/CIDs

## Assessment

This is a model for how other components should handle memory — bounded data structures with eviction policies and configurable limits.
