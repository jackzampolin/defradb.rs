# Finding: Recovery Mode Bypasses ACP Checks on P2P Merge

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM (not HIGH - see analysis below)
**Category**: Access Control Bypass
**Status**: CONFIRMED - BY DESIGN, NEEDS HARDENING

## Summary

When `BlockMetadata.is_recovery` is true, the `AcpMergeHandler` completely skips ACP permission checks and delegates directly to the inner merge handler. This is triggered during crash recovery and version/schema sync.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/db/src/acp_merge_handler.rs:193-205` | `handle_block()` | Skips all ACP checks when `is_recovery == true` |
| `crates/p2p/src/sync/replication/recovery.rs:77` | `process_unmerged_block()` | Calls with `BlockMetadata::recovery()` |
| `crates/cli/src/version_syncer.rs:308` | version sync | Calls with `BlockMetadata::recovery()` |
| `crates/ffi/src/p2p/version_sync.rs:264` | FFI version sync | Calls with `BlockMetadata::recovery()` |

## Details

### The Bypass

```rust
// crates/db/src/acp_merge_handler.rs:193-205
async fn handle_block(&self, cid: &Cid, block_data: &[u8], metadata: BlockMetadata<'_>)
    -> Result<MergeOutcome, Self::Error>
{
    if metadata.is_recovery {
        tracing::debug!(cid = %cid, "Recovery mode: delegating to inner handler without ACP check");
        return self.inner.handle_block(cid, block_data, metadata).await.map_err(Into::into);
    }
    // ... normal ACP checks follow ...
}
```

### When Recovery Mode Activates

1. **Crash recovery** (`recovery.rs`): After a crash, blocks that were stored but not yet merged are re-processed. Metadata (creator, collection_id, doc_id) is unavailable because it wasn't persisted alongside the block.

2. **Version/schema sync** (`version_syncer.rs`, `version_sync.rs`): Collection schema definitions don't have traditional doc_id/collection_id, so they use recovery metadata.

### Why This is MEDIUM, Not HIGH

The recovery path processes blocks that were **already stored in the local blockstore** - meaning they already passed ACP checks during their initial reception. Recovery just re-applies the merge logic to blocks that crashed before merge completed.

However, there are concerns:

### Remaining Risks

1. **Version sync abuse**: The version_syncer uses recovery mode for schema sync, which means schema updates from P2P skip ACP. If an attacker can inject a malicious schema definition block into the blockstore, it would be merged without ACP checks on restart.

2. **No verification that blocks were previously ACP-checked**: The recovery path trusts that any block in the blockstore was legitimately received. If the blockstore is corrupted or manipulated, recovery would merge unauthorized blocks.

3. **`BlockMetadata::recovery()` is public**: Any code with access to the merge handler can call it with recovery metadata to bypass ACP. The constructor is `pub fn recovery() -> Self` with no access control.

## Remediation

### Option A: Re-check ACP during recovery

Extract metadata from block_data (the inner handler already does this) and perform ACP checks even in recovery mode. This is the safest option but may slow recovery.

### Option B: Mark recovery-eligible blocks

During normal operation, record which blocks passed ACP. During recovery, only process blocks that have this mark. Unauthorized blocks fail recovery with an error.

### Option C: Restrict recovery mode access (minimal fix)

Make `BlockMetadata::recovery()` `pub(crate)` to limit who can trigger it. Add an assertion that recovery mode is only used during node startup, not during normal P2P operation.

## Test Coverage

The existing tests (`acp_merge_handler.rs:288-330`) only test the peer-to-identity mapping, not the recovery bypass path. No test verifies that recovery mode correctly handles ACP-protected documents.
