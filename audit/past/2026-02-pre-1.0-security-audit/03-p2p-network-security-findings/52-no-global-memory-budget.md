# Finding 52: No Global Memory Budget or Per-Peer Memory Tracking

**Severity**: MEDIUM
**Category**: Resource Exhaustion
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

There is no global memory budget for the P2P subsystem and no per-peer memory tracking. While individual components have bounds (PeerStateTracker has CID limits, channels are bounded), there is no overarching mechanism to prevent the combined memory usage of all P2P operations from exceeding a configured limit.

## Evidence

**Components with individual bounds**:
- PeerStateTracker: 1M total CIDs (Finding 48) — ~38MB
- Bounded channels: 256 entries each × 4 channels × ~1KB/entry — ~1MB
- GossipSub cache: 5 slots × 64KB × mesh_size — ~4MB/topic

**Components without bounds**:
- `pending_dags`: `HashMap<Cid, PendingDag>` — no eviction (Finding 32)
- `query_to_root`: `HashMap<QueryId, Cid>` — no eviction
- `bitswap_queries`: `HashMap<QueryId, AbortHandle>` — no eviction
- `peer_addrs`: `HashMap<PeerId, Multiaddr>` — no eviction
- `spawned_tasks`: `JoinSet<()>` — grows with concurrent operations
- `failure_tx`: Unbounded channel (Finding 46)
- In-flight two-stream reads: `Vec::new()` per read_to_end (Finding 44)

**No monitoring**:
- No `jemalloc` or `tikv-jemalloc-ctl` integration for memory stats
- No metrics for P2P memory usage
- No circuit breaker that stops accepting work when memory is high

## Attack Scenario

Even with individual component bounds, an attacker can exploit the combination:
1. Establish connections (no limit) → connection state memory
2. Open streams on each (256 per connection) → yamux buffer memory
3. Send DocSync requests with many doc_ids (Finding 31) → spawned DAG fetchers
4. DAG fetchers create pending_dags entries (no eviction) → growing HashMap
5. Each DAG fetcher spawns Bitswap tasks → more in-flight state

The aggregate exceeds available memory even though no single component is unbounded.

## Recommendation

1. Add a global memory watermark check (e.g., via `jemalloc` stats or `/proc/self/statm`)
2. When memory exceeds threshold, stop accepting new inbound streams
3. Track per-peer memory contribution for eviction decisions
4. Add Prometheus metrics for P2P memory components
