# Finding: Unbounded Tokio Task Spawning Per Peer

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: HIGH
**Category**: Denial of Service / Resource Exhaustion

## Summary

The two-stream runner spawns a new `tokio::spawn` for every incoming stream with no per-peer limit, no global concurrency limit, and no backpressure. A single malicious peer can open thousands of concurrent streams, each spawning an independent task that allocates memory via `read_to_end()`. Combined with Finding 00 (no message size limit), this enables a multiplicative memory exhaustion attack.

## Affected Files

| File | Lines | Spawn Point | Protocol |
|------|-------|-------------|----------|
| `crates/p2p/src/two_stream/runner.rs` | 82 | Request stream | PushLog/DocSync/BranchableSync requests |
| `crates/p2p/src/two_stream/runner.rs` | 117 | Response stream | PushLog/DocSync/BranchableSync replies |
| `crates/p2p/src/two_stream/runner.rs` | 146 | SE request stream | Searchable encryption |
| `crates/p2p/src/two_stream/runner.rs` | 162 | SE response stream | SE acknowledgements |
| `crates/p2p/src/two_stream/runner.rs` | 179 | CAR request stream | CAR fetch requests |
| `crates/p2p/src/two_stream/runner.rs` | 195 | CAR response stream | CAR fetch responses |

Secondary fan-out (spawned from event handlers after processing):

| File | Lines | Spawn Point | Trigger |
|------|-------|-------------|---------|
| `crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs` | 155 | DAG fetcher per CID | DocSyncReply with N head CIDs |
| `crates/p2p/src/sync/coordinator/event_handler/branchable_sync.rs` | 137 | DAG fetcher per CID | BranchableSyncReply with N head CIDs |
| `crates/p2p/src/host/command_handler/bitswap.rs` | 48 | Bitswap fetch task | Each bitswap_sync call |

## Details

### No Concurrency Limits

The runner's `run()` method uses `tokio::select!` in a loop. Each incoming stream immediately spawns a task:

```rust
// runner.rs:76-107
Some((peer_id, stream)) = self.request_streams.next() => {
    let event_tx = self.event_tx.clone();
    tokio::spawn(async move {
        match TwoStreamHandler::handle_request_stream(peer_id, stream).await {
            // ...
        }
    });
}
```

There is no:
- `Semaphore` to limit concurrent tasks
- Per-peer task counter
- Global task budget
- Queue or admission control

The replication loop (`loop_runner.rs:152`) does use a `Semaphore` with `max_workers` — but this is for outbound replication scheduling, not inbound stream handling.

### Attack Scenario

1. Attacker connects as a P2P peer
2. Opens 10,000 streams on the request protocol simultaneously
3. Each stream spawns an independent tokio task
4. Each task calls `read_to_end()` on `Vec::new()` (no size limit per Finding 00)
5. Attacker sends 100MB on each stream → 1TB total memory pressure
6. Node OOMs and crashes

### Secondary Fan-Out

Even with legitimate-sized messages, a single DocSyncReply can trigger unbounded task fan-out:

```rust
// doc_sync.rs:144-165
for (root_cid, doc_id) in cids_to_fetch {
    tokio::spawn(super::super::dag_fetcher::poll_fetch_dag(
        host, blockstore, event_tx, root_cid, doc_id, ...
    ));
}
```

A DocSyncReply with 10,000 items spawns 10,000 DAG fetcher tasks. Each fetcher does up to 20 iterations of Bitswap polling (30s timeout each), so tasks can live up to 10 minutes.

### No Per-Peer Memory Accounting

Each spawned task operates independently. There is no shared budget tracking how much memory a given peer_id has caused to be allocated. Concurrent streams from the same peer accumulate without coordination.

## Remediation

1. Add a `tokio::sync::Semaphore` to limit total concurrent inbound stream tasks (e.g., 64 global)
2. Add per-peer task counters — reject new streams from a peer that already has N in-flight tasks
3. For DAG fetcher fan-out, use a bounded `JoinSet` or `Semaphore` to limit concurrent fetchers (e.g., 16)

## Test Gap

No test opens multiple concurrent streams from a single peer and verifies resource limits are enforced.
