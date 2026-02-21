# Finding 50: CAR Response Collects Unbounded DAG from Blockstore

**Severity**: MEDIUM
**Category**: Resource Exhaustion
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

When handling a CAR fetch request, the node recursively collects ALL blocks in the requested DAG from its local blockstore and sends them as a single CARv1 response. There is no limit on the number of blocks or total bytes collected. A peer can request any root CID and receive the entire reachable DAG.

## Evidence

**CAR request handler** (`coordinator/event_handler/car.rs:13-42`):
```rust
let blocks = collect_dag_blocks(self.manager.blockstore().as_ref(), &root_cid).await?;
// ... encode all blocks into CARv1 ...
let car_data = encode_car(&[root_cid], &block_refs)?;
self.host.send_car_response(peer_id, car_data).await?;
```

**DAG collection** (`sync/car.rs:72-114`):
```rust
pub async fn collect_dag_blocks<B: Blockstore>(
    blockstore: &B,
    root_cid: &Cid,
) -> Result<Vec<(Cid, Vec<u8>)>> {
    let mut blocks = Vec::new();
    let mut visited = HashSet::new();
    collect_recursive(blockstore, root_cid, &mut blocks, &mut visited).await?;
    Ok(blocks)
}
```

The `collect_recursive` function uses `Box::pin` recursion (actual recursion, not iterative) with a `visited` set to prevent cycles, but no depth or breadth limit.

## Attack Scenarios

**Memory exhaustion on responder**:
1. Attacker stores a document with a large DAG (many CRDT operations = deep DAG)
2. Attacker sends CAR request for the root CID
3. Responder collects ALL blocks into memory, potentially hundreds of MB
4. Responder OOM or heavy swap pressure

**Amplification**:
1. Attacker sends a CID (38 bytes) as a CAR request
2. Responder looks up the DAG and responds with potentially megabytes of data
3. Amplification factor: size(response) / size(request) can be 10,000x+

**Additional concern**: `collect_recursive` uses actual recursive `Box::pin` futures. Very deep DAGs could cause stack-like issues in the async runtime, though tokio's heap-allocated futures mitigate this somewhat.

## Recommendation

1. Limit the number of blocks per CAR response (e.g., 1000 blocks)
2. Limit total CAR response size (e.g., 16MB, matching `MAX_MESSAGE_SIZE`)
3. Convert `collect_recursive` to iterative traversal (consistent with `find_all_missing_links` which already uses iterative BFS)
