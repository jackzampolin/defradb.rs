# Finding: P2P Block Creator Identity Taken from Peer-Reported Metadata

**Stream**: 02 - Access Control Policy
**Severity**: HIGH
**Category**: Identity Spoofing / Authentication
**Status**: CONFIRMED

## Summary

During P2P merge, the creator identity used for ACP permission checks comes from `BlockMetadata.creator`, which is populated from the PushLog message sent by the remote peer. This identity is **self-reported** — not cryptographically verified from the block's signature. A malicious peer can claim any creator identity, potentially gaining UPDATE permission to ACP-protected documents.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/db/src/acp_merge_handler.rs:140-175` | `check_merge_permission()` | Uses `metadata.creator` for ACP check |
| `crates/db/src/acp_merge_handler.rs:208-217` | `handle_block()` | Extracts creator from `metadata.creator` |
| `crates/db/src/acp_merge_handler.rs:115-137` | `peer_to_identity()` | Converts peer ID to DID, but peer ID is self-reported |
| `crates/p2p/src/sync/replication/handlers.rs:327-342` | `process_event()` | Passes `creator` from `SyncEvent::BlockReceived` |
| `crates/p2p/src/sync/merge.rs:60-70` | `BlockMetadata` | `creator` is `Option<&str>` — no cryptographic binding |

## Details

### The Trust Chain Gap

The ACP merge handler checks permissions like this:

```rust
// crates/db/src/acp_merge_handler.rs:208-222
let (creator, collection_id, doc_id) =
    match (metadata.creator, metadata.collection_id, metadata.doc_id) {
        (Some(c), Some(col), Some(d)) => (c, col, d),
        _ => { return Err(...) }
    };

let permitted = self.check_merge_permission(creator, collection_id, doc_id).await?;
```

And `check_merge_permission()` does:

```rust
// crates/db/src/acp_merge_handler.rs:153
let identity = self.peer_to_identity(creator);

// crates/db/src/acp_merge_handler.rs:156-163
let permitted = check_doc_permission(
    self.acp.as_ref(),
    &identity,
    DocumentPermission::Update,
    collection.schema(),
    doc_id,
).await?;
```

The `creator` string comes from `BlockMetadata`, which is populated from the P2P message:

```rust
// crates/p2p/src/sync/replication/handlers.rs:328-340
SyncEvent::BlockReceived {
    cid,
    doc_id,
    collection_id,
    creator,         // ← self-reported by remote peer
} => {
    handle_block_received(
        coordinator,
        handler,
        config,
        cid,
        BlockMetadata::normal(&doc_id, &collection_id, &creator),
    ).await
}
```

### The Correct Approach

The block's signature contains the creator's identity:

```rust
// crates/db/src/block_verify.rs:89
let sig_identity = String::from_utf8_lossy(&signature.header.identity);
```

In `verify_block_signature()`, the code verifies that `signature.header.identity` matches the provided public key. This is the cryptographically-verified identity. But during P2P merge, this field is never consulted — the peer-reported `creator` is used instead.

### Attack Scenario

1. Alice creates a document with ACP policy granting UPDATE only to herself
2. Malicious peer Bob connects to Alice's node
3. Bob crafts a PushLog message with `creator = "Alice's_peer_id"` and a malicious block delta
4. Alice's node receives the message:
   - `BlockMetadata::normal(doc_id, collection_id, "Alice's_peer_id")`
   - `peer_to_identity("Alice's_peer_id")` → maps to Alice's DID
   - `check_doc_permission(Alice, Update, ...)` → **PERMITTED** (Alice is the owner)
5. The malicious delta is merged as if Alice created it
6. Alice's document is corrupted with Bob's data

### Interaction with Finding 18

This finding is tightly coupled with finding 18 (no signature verification during merge). Together they form a complete authentication bypass:

- **Finding 18**: Blocks are not signature-verified during merge (integrity gap)
- **Finding 19**: Creator identity is peer-reported, not signature-derived (authentication gap)

Fixing either one partially mitigates the other:
- If signatures were verified, the block would need a valid signature from the claimed creator
- If creator identity came from the signature, a spoofed creator wouldn't match

### peer_to_identity() Analysis

The `peer_to_identity()` function (lines 115-137) has additional issues:

1. If no `peer_to_did` mapping is configured, it returns `Identity::Anonymous`
2. Anonymous identity is rejected for ACP-protected docs (good)
3. But if peer_to_did IS configured, it maps the **self-reported** peer ID to a DID
4. The mapping trusts that the peer ID in the metadata matches the actual sending peer

The P2P layer uses libp2p PeerIds which are cryptographically derived from the peer's public key — but the `creator` field in the PushLog message is a string that any peer can set to any value. The PeerId of the sending peer is NOT enforced to match the `creator` field.

## Remediation

### Option A: Derive creator identity from block signature (recommended)

During merge, extract the creator from the block's cryptographic signature rather than from metadata:

```rust
async fn handle_block(&self, cid: &Cid, block_data: &[u8], metadata: BlockMetadata<'_>)
    -> Result<MergeOutcome, Self::Error>
{
    let block = Block::from_dag_cbor(block_data)?;

    // Extract creator from signature, not from metadata
    let creator = if let Some(sig_cid) = &block.signature {
        let sig_data = self.blockstore.get(sig_cid).await?;
        let signature = Signature::from_dag_cbor(&sig_data)?;
        String::from_utf8_lossy(&signature.header.identity).to_string()
    } else {
        // No signature — use metadata.creator but only for policy-less collections
        metadata.creator.unwrap_or("anonymous").to_string()
    };

    let permitted = self.check_merge_permission(&creator, collection_id, doc_id).await?;
    // ...
}
```

### Option B: Validate metadata.creator matches sending peer

Ensure the `creator` field in the PushLog message matches the actual libp2p PeerId of the sending peer. Reject messages where the creator doesn't match.

### Option C: Validate metadata.creator against block signature

Before ACP check, verify that `metadata.creator` matches `signature.header.identity`. Reject if mismatch.

## Test Coverage

No test verifies that spoofed creator identities are rejected. Needed tests:
1. P2P merge with creator matching block signature → succeeds
2. P2P merge with creator NOT matching block signature → rejected
3. P2P merge with no block signature and ACP-protected document → rejected
