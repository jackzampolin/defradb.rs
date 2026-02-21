# Finding: Recovery Mode Bypasses ACP Checks on P2P Merge

**Stream**: 02 - Access Control Policy
**Severity**: HIGH (upgraded from MEDIUM — version sync exploitable mid-operation)
**Category**: Access Control Bypass
**Status**: CONFIRMED - EXPLOITABLE VIA VERSION SYNC

## Summary

When `BlockMetadata.is_recovery` is true, the `AcpMergeHandler` completely skips ACP permission checks and delegates directly to the inner merge handler. The initial assessment rated this MEDIUM under the assumption that recovery mode only activates at startup for crash recovery. **Deep-dive reveals that version sync uses recovery metadata during normal operation**, triggered by authenticated API calls that fetch blocks from untrusted P2P peers.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/db/src/acp_merge_handler.rs:193-205` | `handle_block()` | Skips all ACP checks when `is_recovery == true` |
| `crates/p2p/src/sync/replication/recovery.rs:77` | `recover_unmerged()` | Startup-only: calls with `BlockMetadata::recovery()` |
| `crates/cli/src/version_syncer.rs:308` | version sync | **Mid-operation**: calls with `BlockMetadata::recovery()` |
| `crates/ffi/src/p2p/version_sync.rs:264` | FFI version sync | **Mid-operation**: calls with `BlockMetadata::recovery()` |
| `crates/http/src/handlers/p2p/collections.rs:134-148` | `sync_versions()` | HTTP endpoint triggers version sync |
| `crates/http/src/router/routes.rs:130-131` | route | `POST /api/v0/p2p/collections/sync-versions` |

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

### Three Call Sites for `BlockMetadata::recovery()`

1. **Crash recovery** (`recovery.rs:77`): After a crash, blocks stored but not merged are re-processed. `recover_unmerged()` is only callable from within the P2P crate. Metadata is unavailable because it wasn't persisted alongside the block. **Startup-only, low risk.**

2. **Version sync — CLI** (`version_syncer.rs:308`): Collection schema definitions are synced via Bitswap from remote peers. The code fetches blocks, then processes them with `BlockMetadata::recovery()`. **Triggered mid-operation via HTTP API.**

3. **Version sync — FFI** (`ffi/version_sync.rs:264`): Same pattern for FFI builds.

### Critical Discovery: Version Sync is HTTP-Triggered

The version sync is not just an internal startup operation — it's exposed as an HTTP endpoint:

```
POST /api/v0/p2p/collections/sync-versions
```

The handler at `collections.rs:134-148`:
```rust
pub async fn sync_versions(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<SyncVersionsRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pSyncCollectionVersions).await?;
    let p2p = state.require_p2p()?;
    p2p.sync_collection_versions(body.version_ids).await.map_err(HttpError::Internal)?;
    Ok(Json(()))
}
```

This requires `P2pSyncCollectionVersions` NAC permission, so an authenticated user can trigger it. The flow:

1. User calls `POST /api/v0/p2p/collections/sync-versions` with CID strings
2. System fetches blocks from **connected P2P peers** via Bitswap
3. Blocks are decoded and linked blocks are BFS-fetched (version_syncer.rs:236-305)
4. Blocks are processed through merge handler with `BlockMetadata::recovery()` (line 308)
5. `AcpMergeHandler` sees `is_recovery = true`, **skips all ACP checks**
6. `DbMergeHandler` processes the delta — including `CollectionDefinition` which registers new schemas

### Attack Scenario

A malicious P2P peer that is connected to the node can:

1. Craft a `CollectionDefinition` block with a known CID
2. Wait for the local node's version sync to request that CID via Bitswap
3. Serve the crafted block, which gets processed without ACP
4. The schema is registered in the local database, potentially:
   - Overriding existing schema definitions
   - Registering schemas that grant broader access than intended
   - Injecting schema versions that bypass policy constraints

### Answers to Critical Questions

| Question | Answer |
|----------|--------|
| Recovery only at startup? | **NO** — version sync uses recovery metadata mid-operation |
| Can a P2P peer cause recovery path? | **YES** — via version sync, blocks from peers enter recovery path |
| Is `BlockMetadata::recovery()` only called from recover_unmerged()? | **NO** — also from version_syncer.rs:308 and ffi/version_sync.rs:264 |
| Could version_syncer exploit inject schemas? | **YES** — blocks fetched from peers are processed without ACP |
| What if inner handler metadata extraction fails? | Inner handler (`DbMergeHandler`) decodes block data directly — if decode fails, it returns `MergeError::BlockDecode` which propagates as error, no silent bypass |

### Why This is Now HIGH

- **Not startup-only**: Version sync happens during normal operation
- **Externally triggerable**: Any user with P2pSyncCollectionVersions can trigger it
- **Untrusted input**: Blocks are fetched from P2P peers who can serve arbitrary data
- **Schema injection**: CollectionDefinition blocks modify the database schema
- **No signature verification**: Blocks in the merge path are not signature-verified (see finding 18)

### Mitigating Factor

The version sync endpoint requires NAC authentication (`P2pSyncCollectionVersions`). However:
- When NAC is not configured (default), all requests pass
- Even with NAC, the permission grants schema injection capability that bypasses ACP
- The P2P peer providing blocks is unauthenticated

## Remediation

### Option A: Use normal metadata for version sync (recommended)

Instead of `BlockMetadata::recovery()`, version sync should construct proper metadata from the decoded block's `CollectionDefinition` payload. The collection name, ID, and creator are all embedded in the block data. This ensures ACP checks are applied.

### Option B: Separate recovery flag from version sync

Create `BlockMetadata::version_sync()` with a distinct flag that still applies ACP checks but relaxes the doc_id/collection_id requirement. The inner handler can extract these from the block data while the outer AcpMergeHandler still enforces access control.

### Option C: Restrict `BlockMetadata::recovery()` visibility

Make `BlockMetadata::recovery()` `pub(crate)` in the p2p crate and remove its use from version sync. Version sync should use normal metadata with fields populated from block decoding.

## Test Coverage

No test verifies that version sync enforces ACP. Needed tests:
1. Version sync from an unauthorized peer → should be rejected
2. Recovery at startup with ACP-protected documents → should re-check ACP
3. Version sync injecting a schema that conflicts with existing ACP policy → should be rejected
