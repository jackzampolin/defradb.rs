# Recursive DAG Traversal Has No Depth Limit

**Severity:** High
**Category:** Denial of Service / Stack Safety
**Status:** Confirmed

## Summary

The composite and collection merge handlers process parent blocks recursively via `Box::pin`. There is no depth limit. An attacker can craft a chain of blocks where each points to the previous as its parent, creating an arbitrarily deep DAG. While `Box::pin` heap-allocates each future, the async runtime's call stack still grows with each recursive `.await`, eventually causing a stack overflow that crashes the node.

## Affected Files

- `crates/db/src/merge_handler/composite.rs:101-161` (composite parent recursion)
- `crates/db/src/merge_handler/composite.rs:751-791` (batch composite parent recursion)
- `crates/db/src/merge_handler/collection.rs:31-73` (collection parent recursion)
- `crates/db/src/merge_handler/collection.rs:269-295` (batch collection parent recursion)

## Details

### Composite Merge Recursion

```rust
// composite.rs:101-161
if let Some(heads) = &block.heads {
    for head_cid in heads {
        // ... dedup check ...

        // Load parent from blockstore
        let head_data = match self.blockstore.get(head_cid).await { ... };
        let head_block = match Block::from_dag_cbor(&head_data) { ... };

        if let CrdtDelta::Composite(head_payload) = &head_block.delta {
            // Recursive call — no depth counter, no limit
            let _ = Box::pin(self.process_composite_delta(
                head_cid, &head_block, head_payload, metadata, from_collection,
            )).await;
        }
    }
}
```

### Collection Merge Recursion

```rust
// collection.rs:31-73
if let Some(heads) = &block.heads {
    for head_cid in heads {
        // ... load parent ...
        if let CrdtDelta::Collection(head_payload) = &head_block.delta {
            // Recursive call — no depth counter, no limit
            let _ = Box::pin(self.process_collection_delta(
                head_cid, &head_block, head_payload, metadata,
            )).await;
        }
    }
}
```

### Stack Overflow Analysis

Tokio worker threads have a default stack size of **2 MB** (configurable via `tokio::runtime::Builder::thread_stack_size`). Each recursive async frame has overhead from:
- The `Box::pin` future allocation (heap, not stack — this is fine)
- The `.await` suspension point machinery (stack)
- Local variables in the function body (stack)
- The blockstore `get()` call frame chain (stack)

A conservative estimate puts each recursion level at 1-4 KB of stack usage. With a 2 MB stack, overflow occurs around **500-2000 levels** of recursion.

### Attack Vector

An attacker can:

1. Create a chain of 10,000 composite blocks, each with its `heads` pointing to the previous block
2. Send all blocks to the target node via Bitswap (they're valid CBOR, valid CIDs)
3. Send the final block via GossipSub/PushLog, triggering the merge handler
4. The merge handler recursively walks the entire chain, overflowing the stack
5. The tokio worker thread panics, potentially crashing the node

This is amplified because the collection delta has the **same recursive pattern** and also lacks a dedup guard, so the attack surface is larger.

### Go Comparison

Go's `processLog` in `internal/db/merge.go` also walks the DAG recursively, but Go's goroutine stacks start small (8 KB) and grow dynamically up to 1 GB. Rust's tokio threads have a fixed stack size, making this a Rust-specific concern.

## Remediation

Add a depth counter to all recursive merge functions:

```rust
const MAX_MERGE_DEPTH: usize = 1000;

pub(crate) async fn process_composite_delta(
    &self,
    cid: &Cid,
    block: &Block,
    payload: &CompositeDeltaPayload,
    metadata: &BlockMetadata<'_>,
    from_collection: bool,
    depth: usize,  // NEW
) -> Result<MergeOutcome, MergeError> {
    if depth > MAX_MERGE_DEPTH {
        return Err(MergeError::MergeFailed(
            format!("DAG depth {} exceeds maximum {}", depth, MAX_MERGE_DEPTH)
        ));
    }
    // ... recursive call passes depth + 1 ...
}
```

Alternatively, convert to an iterative approach using an explicit stack (Vec of CIDs to process), which eliminates the stack overflow risk entirely.

## Test Gap

No test sends a DAG chain deeper than ~5 levels. Need:
- Unit test: 100-deep composite chain merges correctly
- Unit test: 1000+ deep chain returns depth error, does not crash
- Integration test: P2P peer sends deep chain, receiver rejects gracefully
