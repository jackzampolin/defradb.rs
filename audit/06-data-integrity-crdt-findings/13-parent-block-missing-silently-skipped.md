# Missing Parent Blocks Silently Skipped During Merge

**Severity:** Medium
**Category:** Data Integrity / Convergence
**Status:** Confirmed — Design Trade-off

## Summary

When the merge handler traverses parent composites via `block.heads`, missing parent blocks are silently skipped with `continue`. If the parent's CRDT deltas haven't been applied to the local node, the current block's merge proceeds without the full DAG history. This can cause the receiving node to compute a different document state than nodes that received the parents, violating CRDT convergence.

## Affected Files

- `crates/db/src/merge_handler/composite.rs:117-135` (parent composite skip)
- `crates/db/src/merge_handler/collection.rs:33-51` (parent collection skip)

## Details

### Silent Skip Pattern

```rust
// composite.rs:117-135
let head_data = match self.blockstore.get(head_cid).await {
    Ok(Some(data)) => data,
    Ok(None) => {
        tracing::debug!(
            parent_cid = %head_cid,
            child_cid = %cid,
            "Parent composite not in blockstore, skipping"
        );
        continue;  // <-- SILENT SKIP
    }
    Err(e) => {
        tracing::debug!(
            parent_cid = %head_cid,
            error = %e,
            "Failed to load parent composite, skipping"
        );
        continue;  // <-- SILENT SKIP
    }
};
```

### Impact Scenario

1. Node A creates document Doc1 with field `name = "Alice"` (block B1)
2. Node A updates Doc1 with `name = "Bob"` (block B2, heads = [B1])
3. Node B receives B2 via P2P but B1 is NOT in the blockstore (Bitswap fetch failed partially)
4. Merge handler for B2 tries to load B1, gets None, skips it
5. B2's composite merge proceeds: the LWW field merge for `name` runs with priority 2, writes "Bob"
6. Later, B1 arrives and its composite merge runs: the LWW field merge for `name` runs with priority 1
7. Since priority 2 > 1, the LWW merge rejects B1's value — final state is "Bob", which is correct

So for LWW, the parent-skip is actually safe because LWW's priority-based conflict resolution makes merge order irrelevant. The parent only needs to be applied if it has HIGHER priority than the current block.

### When It's NOT Safe

For **counters**, skipping a parent could cause double-counting or missed increments if the nonce tracking depends on DAG ordering. However, counter nonce idempotency (verified in Session 1, finding 08) protects against double-counting. Missing a decrement parent could leave the counter too high until the parent eventually arrives.

For **document state reconstruction**, the composite merge overlays field values onto the existing document. If the parent hasn't been merged yet, the existing document may be missing fields that the parent would have set. The current block only carries its own field values, not a full snapshot. Missing parent fields means the saved document is incomplete.

### Design Rationale

This is a deliberate availability-over-correctness trade-off matching Go's behavior. In a P2P network, blocks may arrive out of order or with gaps. Blocking on missing parents would stall the entire merge pipeline. Instead, the system relies on eventual consistency: the missing parent will eventually arrive via Bitswap retry or replication, triggering its own merge, which brings the state into convergence.

### Attacker Exploitation

A malicious peer could:
1. Send a composite block B2 with heads = [B1] via GossipSub
2. Withhold B1 from Bitswap responses
3. The target node merges B2 without B1's context
4. The target node's document state diverges from honest nodes until B1 is obtained from another peer

This is mitigated by having multiple peers serve blocks via Bitswap, but in a network partition scenario with limited peers, the divergence could persist.

## Remediation

This is a known trade-off. Improvements:

1. **Upgrade log level from debug to warn** for missing parent blocks — this should be visible in operational monitoring
2. **Track missing parents** and request them proactively via Bitswap after the current merge completes
3. **Add a metric counter** for skipped parents to enable alerting on sustained divergence

## Test Gap

No test verifies correct behavior when parent blocks are missing:
- Integration test: merge composite when its parent head is not in blockstore
- Integration test: verify convergence after late parent arrival
- Property test: random DAG delivery order always converges
