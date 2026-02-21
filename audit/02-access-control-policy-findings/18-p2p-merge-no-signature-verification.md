# Finding: P2P Merge Path Does Not Verify Block Signatures

**Stream**: 02 - Access Control Policy
**Severity**: HIGH
**Category**: Authentication Bypass / Integrity
**Status**: CONFIRMED

## Summary

Blocks received from P2P peers are merged into the local database without verifying their cryptographic signatures. The `verify_block_signature()` function exists but is only used by the on-demand `/api/v0/block/verify` endpoint — it is never called during the P2P merge flow. This means any connected peer can inject arbitrary blocks that will be merged as if they were legitimately created.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/db/src/merge_handler/mod.rs:330-435` | `handle_block()` | No signature verification in merge path |
| `crates/db/src/acp_merge_handler.rs:187-236` | `handle_block()` | Checks ACP permission but not signature |
| `crates/db/src/block_verify.rs:15-112` | `verify_block_signature()` | Exists but NOT called during merge |
| `crates/cli/src/block_adapter.rs:47` | caller | Only used by `/api/v0/block/verify` endpoint |
| `crates/ffi/src/block.rs:77` | caller | Only used by FFI block verify |
| `crates/p2p/src/sync/replication/handlers.rs:56-176` | `handle_block_received()` | Passes blocks to merge handler without verification |

## Details

### The Missing Verification

The P2P merge flow is:

```
PubSub message received
    ↓
SyncManager emits SyncEvent::BlockReceived
    ↓
ReplicationLoop calls handle_block_received()
    ↓
handle_block_received() loads block from blockstore
    ↓
AcpMergeHandler::handle_block()  ← checks ACP, NOT signature
    ↓
DbMergeHandler::handle_block()   ← decodes + merges, NOT signature
    ↓
CRDT delta applied to database
```

At no point in this chain is `verify_block_signature()` called.

### What `verify_block_signature()` Does (When Called)

```rust
// crates/db/src/block_verify.rs:15-112
pub async fn verify_block_signature<S: Store>(
    database: &Arc<DB<S>>,
    document_acp: &dyn acp::DocumentACP,
    cid_str: &str,
    public_key_hex: &str,
    key_type: crypto::KeyType,
    caller_identity: &acp::Identity,
) -> Result<(), String> {
    // 1. Load block from blockstore
    // 2. Extract signature CID from block
    // 3. Load signature block
    // 4. Check ACP Read permission (using caller_identity)
    // 5. Verify signature identity matches provided public key
    // 6. Verify signature over block data
}
```

This is a thorough verification — it checks that the block's signature was created by the claimed key, and that the serialized block data matches the signature. But it's only called on-demand, never during merge.

### What the Merge Handler Does with Signatures

The `DbMergeHandler::handle_block()` in `merge_handler/mod.rs:330-435`:

1. Decodes the block from DAG-CBOR ✓
2. Handles encryption/decryption (copies `block.signature` field through) ✓
3. Processes based on delta type (LWW, Counter, Composite, etc.) ✓
4. **Never reads `block.signature`** ✗
5. **Never verifies any cryptographic property of the block** ✗

The `block.signature` field is only referenced to copy it during decryption (lines 372, 392, 174, 194 in the merge handler files) — it's structurally preserved but never validated.

### Attack Scenario

A connected P2P peer can:

1. Craft a block with a valid CRDT delta (e.g., LWW with a high priority)
2. Set any `creator` identity in the PushLog metadata
3. Either omit the signature or forge one
4. Publish the block on the appropriate PubSub topic
5. The local node's merge handler will:
   - Check ACP permission using the peer-reported `creator` (which can be spoofed — see finding 19)
   - Process the CRDT delta and merge it into the database
   - Never verify the block's authenticity

### Impact

Without signature verification during merge:
- **Data integrity**: Any peer can inject arbitrary document mutations
- **Identity spoofing**: Combined with finding 19, a peer can claim any creator identity
- **ACP circumvention**: If the spoofed creator has UPDATE permission, the merge succeeds
- **CRDT corruption**: Crafted deltas with manipulated priorities can override legitimate values

### Mitigating Factors

1. The AcpMergeHandler does check ACP permissions using the `creator` from metadata — but this creator is self-reported by the peer
2. Blocks must be valid DAG-CBOR and valid CRDT deltas to be processed
3. The PubSub layer provides some network-level authentication (peers must have valid libp2p identities)

## Remediation

### Option A: Verify signature in AcpMergeHandler before ACP check (recommended)

Add signature verification as the first step in the merge handler, before checking ACP permissions:

```rust
async fn handle_block(&self, cid: &Cid, block_data: &[u8], metadata: BlockMetadata<'_>)
    -> Result<MergeOutcome, Self::Error>
{
    // Step 0: Verify block signature (if present)
    let block = Block::from_dag_cbor(block_data)?;
    if let Some(sig_cid) = &block.signature {
        verify_block_signature_from_data(block_data, sig_cid)?;
    }

    // Step 1: Check ACP permission
    // ... existing logic ...
}
```

### Option B: Verify signature in SyncCoordinator before merge

Add signature verification at the point where blocks are received from P2P, before they enter the merge handler.

### Option C: Require signatures for all ACP-protected documents

At minimum, verify signatures for blocks targeting ACP-protected documents. Unsigned blocks would only be accepted for policy-less collections.

## Test Coverage

No test verifies that P2P merge validates block signatures. Needed tests:
1. P2P merge with valid signature → succeeds
2. P2P merge with invalid/missing signature → rejected
3. P2P merge with signature from different identity than claimed creator → rejected
