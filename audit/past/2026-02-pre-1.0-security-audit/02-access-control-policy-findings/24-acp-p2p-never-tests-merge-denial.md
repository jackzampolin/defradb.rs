# Finding: P2P ACP Tests Never Verify Merge Denial

**Stream**: 02 - Access Control Policy
**Severity**: HIGH (test gap for HIGH/CRITICAL vulnerability chain)
**Category**: Test Gap
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation
**Related Findings**: 00 (recovery mode bypass — HIGH), 18 (no signature verification — HIGH), 19 (creator identity spoofing — HIGH), 20 (block verify disconnected — MEDIUM)

## Summary

`acp_p2p.rs` tests that ACP-protected documents **replicate successfully** across P2P nodes, but never verifies that unauthorized access is **blocked** after replication. The test actually documents a design limitation: on the receiving node, all replicated documents (including ACP-protected ones) are visible to anonymous queries because ACP relationships are node-local and don't replicate.

No integration test exists for:
- P2P merge rejection of blocks from unauthorized peers
- Block signature verification during merge
- Recovery mode ACP bypass during version sync
- Creator identity verification during merge

## Evidence

### acp_p2p.rs — Tests Success, Not Denial

```rust
// acp_p2p.rs:124-136
// Verify replicated documents on node 1
let node1_result = node1
    .query("query { User { name age } }")
    .expect("query on node1 failed");
let node1_users = node1_result["User"]
    .as_array()
    .expect("node1 result not array");
assert_eq!(
    node1_users.len(),
    2,           // ← Both public AND protected docs visible
    "node1 should have both replicated docs"
);
```

This assertion confirms that on node1, an **anonymous query** sees the "Protected" document. The test treats this as expected behavior (ACP relationships don't replicate), but it also means:
- No test verifies that a **Bob identity** on node1 is denied the protected document
- No test verifies that node1 **could** enforce ACP if policies were also deployed there

### p2p_sync.rs — No ACP Awareness

```rust
// p2p_sync.rs:193
for_each_p2p_topology!(p2p_sync_versions, collection_sync_versions_test, .with_p2p());
```

Version sync tests run without `.with_acp_local()`. No test for:
- Version sync with recovery metadata bypassing ACP (Finding 00)
- Schema injection via version sync from malicious peer

### Missing: P2P Merge Authentication Tests

No test anywhere in the suite verifies:

```
1. Peer A sends a block claiming creator = Alice's DID
2. Block has no valid signature from Alice
3. Local node should REJECT the merge (currently: accepts it — Finding 18)
```

### Missing: Recovery Mode Tests

No test anywhere in the suite verifies:

```
1. Node crashes with unmerged ACP-protected blocks
2. On restart, recovery re-merges blocks
3. ACP checks should still be enforced (currently: recovery skips ACP — Finding 00)
```

### p2p_sync.rs collection_sync_versions_test — Structural Issue

The version sync test at line 114-169 deploys schema without ACP and syncs a version CID. It does not test:
- What happens when version sync fetches blocks from an untrusted peer
- Whether ACP is applied to version-synced schema definitions
- Whether `BlockMetadata::recovery()` in version sync bypasses ACP

## Missing Tests

### P2P Merge Denial

```
1. Deploy ACP policy + schema on both nodes
2. Create ACP-protected document as Alice on node0
3. Set up replication from node0 to node1
4. On node1: deploy same ACP policy but do NOT grant any relationships
5. Query as Bob on node1 → should see 0 (ACP enforced)
6. Verify block signature is checked during merge (assert merge handler validates signature)
```

### P2P Merge with Invalid Signature

```
1. Connect two P2P nodes
2. Send a crafted block with invalid/missing signature
3. Verify merge is rejected
```

### Version Sync ACP

```
1. Deploy ACP policy on node
2. Trigger version sync with blocks from untrusted peer
3. Verify ACP is NOT bypassed via recovery metadata
```

## Severity Rationale

HIGH because:
- Findings 00, 18, 19, 20 form a combined attack chain rated collectively as a systemic P2P authentication gap
- Zero test coverage for any of these four findings
- acp_p2p.rs gives false confidence by testing replication success without testing security enforcement
- The existing test actually demonstrates the ACP gap (protected docs visible without relations on receiving node) but treats it as expected behavior
