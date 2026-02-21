# Finding: DAG Fetch Depth Correctly Capped at 20 Iterations

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: GREEN
**Category**: Defense in Depth

## Summary

The DAG fetcher (`poll_fetch_dag`) limits DAG traversal to 20 iterations, preventing infinite-depth DAG bomb attacks. The `find_all_missing_links` function uses an iterative work queue (not recursion), so stack depth is constant regardless of DAG depth. This is a well-implemented defense.

## Evidence

### Iteration Cap

```rust
// dag_fetcher.rs:74
for iteration in 0..20 {
    // ... fetch missing blocks at each level
    let missing = find_all_missing_links(blockstore.as_ref(), &root_data).await?;
    if missing.is_empty() {
        break;
    }
    // ... fetch missing blocks via Bitswap
}
```

After 20 iterations, the loop exits. If the DAG is still incomplete, the function logs a warning and the DAG is NOT emitted as ready:

```rust
// dag_fetcher.rs:133-140
if remaining.is_empty() {
    // Emit DagReady
} else {
    warn!(remaining_count = remaining.len(), "DAG fetch incomplete");
}
```

### Iterative (Not Recursive) Link Traversal

```rust
// links.rs:73-127
pub async fn find_all_missing_links<B: Blockstore>(
    blockstore: &B, block_data: &[u8],
) -> Result<Vec<Cid>> {
    let mut queue: VecDeque<Vec<u8>> = VecDeque::new();
    queue.push_back(block_data.to_vec());

    while let Some(data) = queue.pop_front() {
        // ... extract links, check blockstore, enqueue children
    }
    Ok(missing)
}
```

Uses `VecDeque` work queue, not recursion. Stack depth is O(1). Memory is proportional to the number of blocks enqueued, but the `visited` HashSet prevents reprocessing.

### Merged Subtree Short-Circuit

```rust
// links.rs:91-101
match blockstore.is_merged(&link_cid).await {
    Ok(true) => continue, // Skip already-merged subtrees
    // ...
}
```

This optimization skips traversal of subtrees that have already been successfully merged, reducing work for overlapping DAG fetches.

### Per-Block Timeout

Each block fetch via Bitswap has a 30-second timeout:

```rust
// dag_fetcher.rs:192-199
let timeout = Duration::from_secs(30);
let start = std::time::Instant::now();
while start.elapsed() < timeout {
    if let Ok(true) = blockstore.has(cid).await {
        return true;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
}
false
```

### One Exception: collect_recursive (CAR Outbound)

The `collect_recursive` function in `car.rs:82-114` uses actual recursion (`Box::pin`). However, it's only used on the outbound path (serving CAR requests from our blockstore), so the attacker doesn't control the DAG depth. The `visited` HashSet prevents infinite loops from cycles.

## Conclusion

DAG depth bombs are effectively mitigated. An attacker cannot cause unbounded recursion or iteration through crafted DAG structures.
