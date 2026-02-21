# Priority Values Derived from DAG Height — Not User-Controlled

**Severity:** Informational (Verified Clean)
**Category:** Adversarial Resilience / CRDT Design
**Status:** Verified
**Session:** 6 of 6

## Summary

Priority values used in LWW conflict resolution are computed from the Merkle DAG height, not user-specified. This means the u64::MAX priority ceiling attack (Finding 05) requires block forgery, not just API misuse. However, a node with a far-future clock does NOT affect priority (clocks are not used).

## Affected Files

- `crates/db/src/block_builder/write.rs` line 398 (`max_priority() + 1`)
- `crates/db/src/block_builder/collection.rs` lines 39, 53 (collection priority)
- `crates/db/src/block_builder/mod.rs` line 279 (max priority scan)

## Details

### Priority Generation

```rust
// write.rs:398
let priority: u64 = snapshot.max_priority() + 1;
```

Priority is the DAG height: each new block's priority is one more than the maximum priority of its parent blocks. This is a monotonically increasing counter that reflects the document's update history depth.

### Clock Skew — Not a Factor

Unlike timestamp-based LWW CRDTs, this system uses DAG height. A node with a far-future system clock does NOT produce higher priorities. Priority escalation requires:

1. Repeatedly updating a document to increment the DAG height
2. Forging blocks with arbitrary priority values (requires bypassing block construction)

### u64::MAX Reachability

At 1 update/second, reaching `u64::MAX` takes ~584 billion years. At 1 million updates/second, it takes ~584 thousand years. The priority ceiling is not practically reachable through normal operation.

### Attack Surface

The only way to inject a u64::MAX priority is via a forged P2P block. Since CID verification is disabled by default (Finding 18), a compromised peer could inject such a block. Defense requires enabling CID verification (hash_on_read) and/or block signature verification.

## Remediation

No change needed. The priority generation mechanism is sound. The residual risk (forged blocks) is addressed by Findings 18/23/29 (CID verification gaps).

## Test Gap

None — priority generation is tested indirectly through integration tests that verify correct merge ordering.
