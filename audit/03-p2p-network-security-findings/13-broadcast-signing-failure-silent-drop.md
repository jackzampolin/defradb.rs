# Finding: Broadcast Signing Failure Silently Drops Field Blocks

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Data Integrity / Silent Failure
**Status**: CONFIRMED

## Summary

When broadcasting a composite block and its linked field blocks to replicator peers, signing failures on field blocks are silently swallowed. The `is_ok()` pattern discards the field block with no logging, no error propagation, and no indication to the receiver that the DAG is incomplete. The receiver gets the composite block but may be missing one or more field blocks, leading to an incomplete DAG that requires Bitswap fetches to complete.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/sync/coordinator/broadcast.rs` | 103-105 | `if sign_message(...).is_ok()` silently drops field blocks on signing failure |
| `crates/p2p/src/sync/coordinator/broadcast.rs` | 116-118 | Composite block signing failure logs at `debug` level only |

## Details

### The Silent Drop

```rust
// broadcast.rs:95-106 — Field block signing
for (field_cid, field_data) in &field_blocks {
    let mut req = PushLogRequest::new(/* ... */);
    if sign_message(self.host.keypair(), &mut req).is_ok() {
        requests.push((*field_cid, req));
    }
    // ^^ If signing fails, the block is silently omitted. No log. No error.
}
```

Compare with the composite block handling immediately below (line 116):

```rust
// broadcast.rs:116-119 — Composite block signing (better — at least logs)
if let Err(e) = sign_message(self.host.keypair(), &mut composite_req) {
    tracing::debug!(error = %e, "Failed to sign composite PushLog request");
    continue;  // Skips entire peer — appropriate
}
```

And `push_to_replicators()` at line 180:

```rust
// broadcast.rs:180-183 — Single block signing (also logs)
if let Err(e) = sign_message(self.host.keypair(), &mut request) {
    tracing::debug!(error = %e, "Failed to sign PushLog request");
    continue;
}
```

### Impact

1. **Partial DAG delivery**: Receiver gets composite block but is missing field blocks, requiring Bitswap round-trips that may fail (especially after node restart when peer state is lost)
2. **No observability**: No log message at any level — the signing failure is completely invisible to operators
3. **Inconsistent error handling**: Two other signing sites in the same file log and handle the error explicitly; only the field block path uses the silent `is_ok()` pattern

### Likelihood

Signing with Ed25519 is unlikely to fail in practice (it would require a corrupted keypair or memory error). However, the pattern violates defense-in-depth: if it ever does fail, the failure mode is silent data loss.

## Remediation

Replace the `is_ok()` pattern with explicit error handling matching the composite block path:

```rust
if let Err(e) = sign_message(self.host.keypair(), &mut req) {
    tracing::warn!(error = %e, cid = %field_cid, "Failed to sign field block PushLog request");
    continue;
}
requests.push((*field_cid, req));
```

## Test Gap

No test verifies that all linked field blocks are included in the replication push. No test simulates signing failure.
