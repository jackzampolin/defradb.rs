# Finding: DAG Fetcher Spawns Unbounded Concurrent Tasks Per Reply

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: MEDIUM
**Category**: Resource Exhaustion

## Summary

When a DocSyncReply or BranchableSyncReply arrives with N head CIDs, the handler spawns N independent `tokio::spawn` tasks (one `poll_fetch_dag` per CID) with no concurrency limit. A reply with thousands of CIDs spawns thousands of long-lived DAG fetcher tasks, each doing up to 20 iterations of Bitswap polling with 30-second timeouts.

## Affected Files

| File | Lines | Issue |
|------|-------|-------|
| `crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs` | 144-165 | Spawns one `poll_fetch_dag` per CID in `cids_to_fetch` |
| `crates/p2p/src/sync/coordinator/event_handler/branchable_sync.rs` | 131-147 | Same pattern for branchable sync |
| `crates/p2p/src/sync/coordinator/dag_fetcher.rs` | 23-141 | Each task: CAR fetch (10s timeout) + up to 20 Bitswap iterations (30s each) |

## Details

### The Fan-Out Pattern

```rust
// doc_sync.rs:144-165
for (root_cid, doc_id) in cids_to_fetch {
    let host = host.clone();
    let blockstore = blockstore.clone();
    let event_tx = event_tx.clone();

    tokio::spawn(super::super::dag_fetcher::poll_fetch_dag(
        host, blockstore, event_tx,
        root_cid, doc_id, String::new(), String::new(), peer_id,
    ));
}
```

No `Semaphore`, `JoinSet`, or concurrency limit.

### Task Lifetime

Each `poll_fetch_dag` task:
1. Tries CAR fetch: 10s timeout (`try_car_fetch` polls blockstore every 100ms for 10s)
2. Falls back to Bitswap: fetches root block (30s timeout)
3. Iterates up to 20 times (`for iteration in 0..20`), fetching missing blocks
4. Each missing block fetch: 30s timeout (`poll_fetch_block`)

Worst case per task: 10s + 30s + (20 × 30s) = **640 seconds** (10+ minutes)

With 1000 CIDs in a reply, that's 1000 concurrent tasks, each potentially alive for 10 minutes, each doing Bitswap network I/O and blockstore queries.

### No Access Control on Replies

DocSync and BranchableSync replies are processed without access checks (Finding 21). An attacker responding to our sync request can include arbitrary CIDs in the reply, triggering the full fan-out even for non-existent blocks.

### Channel Pressure

All DAG fetcher tasks share the same `event_tx: mpsc::Sender<SyncEvent>` channel (buffer size: 256). With 1000 concurrent tasks, channel contention can cause tasks to block on `event_tx.send(DagReady {...}).await`, holding their allocated resources.

## Remediation

1. Use a `tokio::sync::Semaphore` to limit concurrent DAG fetchers (e.g., 16)
2. Validate reply size: reject DocSyncReply with more than `MAX_SYNC_ITEMS` results
3. Use `JoinSet` instead of fire-and-forget `tokio::spawn` to track and limit tasks

## Test Gap

No test sends a DocSyncReply with many head CIDs and verifies the handler limits concurrent fetchers.
