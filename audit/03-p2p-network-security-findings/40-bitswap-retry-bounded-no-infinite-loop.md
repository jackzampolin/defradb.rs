# Finding: Bitswap Retry Logic Is Bounded — No Infinite Loop

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: GREEN
**Category**: Defense in Depth

## Summary

The Bitswap fetch and retry mechanism has multiple timeout and bound mechanisms that prevent infinite loops: per-block timeouts (10s in Bitswap client, 30s in poll fetcher), iteration caps (20 in DAG fetcher), and completion tracking. The `handle_bitswap_complete` retry path iterates over existing pending DAGs without creating new ones unboundedly.

## Evidence

### Bitswap Client: Per-Block Timeout

```rust
// host/command_handler/bitswap.rs:74-117
let per_block_timeout = std::time::Duration::from_secs(10);
loop {
    match tokio::time::timeout(per_block_timeout, chan.recv()).await {
        Ok(Ok(block)) => { fetched += 1; /* store block */ }
        Ok(Err(_)) => { break; }  // Channel closed
        Err(_) => { break; }      // Timeout — give up
    }
}
```

If no block arrives within 10 seconds, the fetch loop breaks and reports partial success.

### DAG Fetcher: 20-Iteration Cap

```rust
// dag_fetcher.rs:74
for iteration in 0..20 {
    // ... try to complete DAG
}
```

Hard cap of 20 iterations per DAG fetch (see Finding 37).

### Poll Fetch Block: 30-Second Timeout

```rust
// dag_fetcher.rs:192-199
let timeout = Duration::from_secs(30);
while start.elapsed() < timeout { ... }
```

Each individual block poll gives up after 30 seconds.

### BitswapComplete Retry: Bounded by Pending DAGs

```rust
// bitswap.rs event handler:79-120
let pending_dags: Vec<Cid> = self.manager.pending_dag_cids();
for root_cid in pending_dags {
    match self.manager.retry_pending_dag(&root_cid).await {
        Ok(true) => { /* completed */ }
        Ok(false) => {
            // Still missing blocks — issue ONE new bitswap_sync
            self.host.bitswap_sync(root_cid, providers, missing).await;
        }
        Err(e) => { /* log and continue */ }
    }
}
```

The retry iterates over the snapshot of pending DAGs at the time of the event. It does not create new pending DAGs. The `bitswap_sync` call may trigger another `BitswapComplete` event later, but each retry only reprocesses existing entries.

### No Infinite Amplification

The retry cycle is:
1. `BitswapComplete` → retry all pending DAGs
2. For incomplete DAGs → issue new `bitswap_sync`
3. New `bitswap_sync` → new `BitswapComplete` (eventually, with timeout)
4. Goto 1

This loop terminates because:
- Each `bitswap_sync` has a 10-second per-block timeout
- Pending DAGs that repeatedly fail to complete stay pending but don't multiply
- Pending DAGs can only be added by DocSync/BranchableSync handlers (not by the retry loop itself)

### Caveat: Pending DAGs Never Expire (See Finding 32)

While the retry loop itself is bounded, pending DAGs that never complete remain in the map indefinitely. This is a separate issue (Finding 32 — unbounded pending DAG growth), not an infinite retry issue.

## Conclusion

Bitswap retry logic is correctly bounded by timeouts and iteration caps. No infinite loop or unbounded retry amplification is possible.
